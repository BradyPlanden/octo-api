use base64::{engine::general_purpose, Engine as _};
use std::fs::File;
use std::num::NonZero;
use serde_json;
use tokio;
use polars::prelude::*;

struct ApiConfig {
    base_url: String,
    api_key: String,
    mpan: String,
    serial: String,
    url: Option<String>,
}

impl ApiConfig {
    fn new(base_url: &str, api_key: &str, mpan: &str, serial: &str) -> Self{
        ApiConfig {
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
            mpan: mpan.to_string(),
            serial: serial.to_string(),
            url: None,
        }
    }
    fn url(&self) -> String {
        // Return cached value if it exists, otherwise calculate
        self.url.clone().unwrap_or_else(|| {
            format!("{}/{}/meters/{}/consumption/?page_size={}&period_from={}&period_to={}",
                    self.base_url, self.mpan, self.serial, 500*48, "2024-01-10T00:00Z", "2025-03-09T00:00Z")
        })
    }
}

/// For a given api configuration, request the data and convert to json object
async fn get_api_data(api_config: ApiConfig)-> Result<serde_json::Value, reqwest::Error>{
    let response = reqwest::Client::new()
        .get(api_config.url())
        .header("Authorization", format!("Basic {}", general_purpose::STANDARD.encode(api_config.api_key)))
        .send()
        .await?;

    if response.status().is_success() {
        let json: serde_json::Value = response.json().await?;
        Ok(json)
    } else {
        Err(response.error_for_status().unwrap_err())
    }
}

/// Construct a polars dataframe from a serde json object
fn construct_dataframe(json: &serde_json::value::Value, field: &str)-> DataFrame {
    let json_str = serde_json::to_string(&json[field])
        .expect("Failed to serialize JSON value");

    let df = polars::prelude::JsonReader::new(
        std::io::Cursor::new(json_str.as_bytes())
    )
    .infer_schema_len(Some(NonZero::new(100).unwrap()))  // Optional: limit rows for schema inference
    .finish()
    .expect("Failed to parse JSON");
    df
}

/// Writes a dataframe to a provided parquet file
fn write_parquet(df: &mut DataFrame, file: File) {
    let parquet_writer = ParquetWriter::new(file)
        .with_compression(ParquetCompression::Snappy);

    // write
    parquet_writer.finish(df)
        .expect("Failed to write parquet file");
}

#[tokio::main]
async fn main() -> Result<(), PolarsError>  {
    // API configuration
    let file = File::open("src/api_config.json")
        .expect("file should open read only");
    let json: serde_json::Value = serde_json::from_reader(file)
        .expect("file should be proper JSON");

    // Construct api
    let api_config = ApiConfig::new(
        json["base_url"].as_str().expect("base_url should be a string"),
        json["api_key"].as_str().expect("api_key should be a string"),
        json["mpan"].as_str().expect("mpan should be a string"),
        json["serial"].as_str().expect("serial should be a string"),
    );

    // Get API data
    let agile_data = get_api_data(api_config).await.unwrap();
    let mut df = construct_dataframe(&agile_data, "results");

    // Write parquet file
    let file = File::create("data.parquet").expect("Could not create file");
    write_parquet(&mut df, file);

    Ok(())

    // Test write
    // let mut file = File::open("data.parquet").unwrap();
    // let df_test = ParquetReader::new(&mut file).finish().unwrap();
    // println!("{:?}", df_test);

    // let stored_data = import_stored_parquet(path);
    // let indices = compare_stored_with_api(api_data, stored_data);

}

// To Do:

// fn import_stored_parquet(path: File){
// //  Import the previously ingested data
// //  Assumes parquest format
// //  inputs: file path
// //     returns: polars dataframe? Table?
//
// }

// fn compare_stored_with_api(stored: i32, api: i32){
// //     Option 1:
// //     grab the tail of both object
// //     if they aren't the same, write the full api data over the stored
//
// //     Option 2:
// //     search api data for tail of stored data
// //     add all following datapoints to the stored data
// //     assuming the structure of the file has not been modified
//
// //     Returns:
// //     Indices to take from the api data and append to the stored
// }