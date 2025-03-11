# Octo-Api

Octo-Api is a small module for acquiring historical Octopus energy household data. Data is acquired through Octopus' REST API and stored in parquet format for further analysis.

## Installation

Within a bash shell:

```bash
cargo install --git https://github.com/BradyPlanden/octo-data.git
```

## Using Octo-Api

To use the module, create a`api_config.json` file from the template and store it in the same directory as the executable or the `cargo run` command.

Data will be retrieved via the REST API and stored in parquet format named `data.parquet`. The format of this file will be:

| consumption   | interval end | interval begin |
|---------------|--------------|----------------|
| Values in kWh | Time String  | Time String    |


## License

Octo-Api is released under the [BSD 3-Clause License](https://github.com/BradyPlanden/octo-data/blob/main/LICENSE).