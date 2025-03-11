use base64::{engine::general_purpose, Engine as _};
use polars::prelude::*;
use std::fs::File;
use std::num::NonZeroUsize;
use std::path::Path;

struct ApiConfig {
    base_url: String,
    api_key: String,
    mpan: String,
    serial: String,
    page_size: usize,
    period_from: String,
    period_to: String,
    url: Option<String>,
}

impl ApiConfig {
    fn new(
        base_url: &str,
        api_key: &str,
        mpan: &str,
        serial: &str,
        page_size: usize,
        period_from: &str,
        period_to: &str,
    ) -> Self {
        Self {
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
            mpan: mpan.to_string(),
            serial: serial.to_string(),
            page_size,
            period_from: period_from.to_string(),
            period_to: period_to.to_string(),
            url: None,
        }
    }
    fn url(&self) -> String {
        // Return cached value if it exists, otherwise calculate
        self.url.clone().unwrap_or_else(|| {
            format!(
                "{}/{}/meters/{}/consumption/?page_size={}&period_from={}&period_to={}",
                self.base_url,
                self.mpan,
                self.serial,
                self.page_size,
                self.period_from,
                self.period_to
            )
        })
    }
}

/// Fetches API data and stores it as a JSON object
async fn get_api_data(config: &ApiConfig) -> Result<serde_json::Value, reqwest::Error> {
    let client = reqwest::Client::new();
    let auth_header = format!(
        "Basic {}",
        general_purpose::STANDARD.encode(&config.api_key)
    );

    let response = client
        .get(config.url())
        .header("Authorization", auth_header)
        .send()
        .await?;

    response.error_for_status()?.json().await
}

/// Construct a Polars dataframe from a serde JSON object
fn construct_dataframe(
    json: &serde_json::value::Value,
    field: &str,
) -> Result<DataFrame, PolarsError> {
    let json_str = serde_json::to_string(&json[field]).expect("Failed to serialize JSON value");

    let df = JsonReader::new(std::io::Cursor::new(json_str.as_bytes()))
        .infer_schema_len(Some(NonZeroUsize::new(100).unwrap())) // Optional: limit rows for schema inference
        .finish()
        .expect("Failed to parse JSON");

    Ok(df)
}

/// Writes a dataframe to a provided parquet file
fn write_parquet(df: &mut DataFrame, path: impl AsRef<Path>) -> Result<(), PolarsError> {
    let file = File::create(path)?;

    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Snappy)
        .finish(df)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), PolarsError> {
    // API configuration
    let file = File::open("src/api_config.json").expect("failed to open config file");
    let json: serde_json::Value =
        serde_json::from_reader(file).expect("failed to parse config as JSON");

    // Construct api config
    let api_config = ApiConfig::new(
        json["base_url"]
            .as_str()
            .expect("base_url not a string or missing"),
        json["api_key"]
            .as_str()
            .expect("api_key not a string or missing"),
        json["mpan"].as_str().expect("mpan not a string or missing"),
        json["serial"]
            .as_str()
            .expect("serial not a string or missing"),
        json["page_size"]
            .as_i64()
            .expect("page number not a number or missing") as usize,
        json["period_from"]
            .as_str()
            .expect("period_from not a string or missing"),
        json["period_to"]
            .as_str()
            .expect("period_to not a string or missing"),
    );

    // Get API data and write parquet
    let path = "data.parquet";
    let agile_data = get_api_data(&api_config).await.unwrap();
    let mut df = construct_dataframe(&agile_data, "results")?;
    write_parquet(&mut df, path)?;

    println!("Data successfully written to {}", path);
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
