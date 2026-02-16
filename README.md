# csv-to-parquet

A fast CLI tool written in Go that converts CSV files to Apache Parquet format with automatic schema detection and type inference.

## Features

- **Automatic type detection** - Samples rows to infer column types (Int64, Float64, Boolean, String, Date/Timestamp)
- **Single file or batch processing** - Convert one CSV or an entire directory of CSVs
- **Concurrent processing** - Up to 4 parallel file conversions
- **Flexible configuration** - YAML config file with CLI flag overrides
- **Custom delimiters** - Support for comma, tab, pipe, or any single-character delimiter
- **Header normalization** - Automatic BOM removal, space/dot replacement, trimming
- **Optional cleanup** - Automatically delete source CSV files after conversion
- **Detailed reporting** - Summary of converted files, sizes, and space savings

## Requirements

- Go 1.24 or later

## Installation

```bash
git clone <repo-url>
cd csv-to-parquet
go build -o csv-to-parquet .
```

## Usage

```bash
# Convert a single file
./csv-to-parquet --input data.csv

# Convert all CSVs in a directory
./csv-to-parquet --input ./data_folder

# Specify output directory
./csv-to-parquet --input data.csv --output ./parquet_output

# Keep original CSV files (default: delete after conversion)
./csv-to-parquet --input data.csv --keep

# Custom delimiter (e.g. tab-separated)
./csv-to-parquet --input data.tsv --delimiter "\t"

# Verbose logging
./csv-to-parquet --input data.csv --log-level debug

# Use a custom config file
./csv-to-parquet --config myconfig.yaml --input data.csv
```

## CLI Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--config` | Path to YAML config file | `config.yaml` |
| `--input` | Input CSV file or directory (**required**) | |
| `--output` | Output directory | same as input |
| `--keep` | Keep original CSV files after conversion | `false` (deletes originals) |
| `--log-level` | Log level: `debug`, `info`, `warn`, `error` | `info` |
| `--batch-size` | Number of rows per Parquet row group | `10000` |
| `--delimiter` | CSV delimiter character | `,` |
| `--sample-rows` | Number of rows sampled for type detection | `100` |

CLI flags override values set in the config file.

## Configuration File

The tool reads a `config.yaml` file by default:

```yaml
input: ""              # Input file or directory path
output: ""             # Output directory (empty = same as input)
delete_original: true  # Delete CSV files after conversion
log_level: "info"      # Logging level
batch_size: 10000      # Rows per Parquet row group
delimiter: ","         # CSV field delimiter
sample_rows: 100       # Rows sampled for type detection
```

## Type Detection

The tool samples rows from each CSV and infers column types automatically:

| Detected Type | Parquet Type |
|---------------|-------------|
| Integer | INT64 |
| Float | DOUBLE |
| Boolean | BOOLEAN |
| Date/Timestamp | UTF8 (string) |
| Text | UTF8 (string) |

Supported date formats: `YYYY-MM-DD`, `DD/MM/YYYY`, `MM/DD/YYYY`, ISO 8601 datetime, and RFC 3339.

If a column contains mixed types, the tool widens to the most compatible type (e.g. int + float becomes float, any mismatch falls back to string).

## Project Structure

```
csv-to-parquet/
├── main.go              # Entry point and summary reporting
├── config/
│   └── config.go        # Configuration loading and CLI flag parsing
├── converter/
│   └── converter.go     # Core conversion logic and type detection
├── config.yaml          # Default configuration file
├── go.mod               # Go module definition
└── go.sum               # Dependency checksums
```

## License

This project is licensed under the GNU General Public License v3.0. See [LICENSE](LICENSE) for details.
