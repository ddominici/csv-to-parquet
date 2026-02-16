# Quick Start

Get up and running with csv-to-parquet in under 5 minutes.

## 1. Build

```bash
go build -o csv-to-parquet .
```

## 2. Convert a Single File

```bash
./csv-to-parquet --input myfile.csv
```

This will:
- Detect column types automatically
- Create `myfile.parquet` in the same directory
- Delete the original CSV file

To keep the original file, add `--keep`:

```bash
./csv-to-parquet --input myfile.csv --keep
```

## 3. Convert a Directory of CSVs

```bash
./csv-to-parquet --input ./csv_folder --output ./parquet_folder --keep
```

All `.csv` files in `csv_folder` will be converted concurrently (up to 4 at a time), and Parquet files will be written to `parquet_folder`.

## 4. Customize Type Detection

If your CSV has many distinct value patterns, increase the sample size for better type accuracy:

```bash
./csv-to-parquet --input data.csv --sample-rows 500
```

## 5. Handle Non-Standard CSVs

For tab-separated or pipe-separated files:

```bash
# Tab-separated
./csv-to-parquet --input data.tsv --delimiter "\t"

# Pipe-separated
./csv-to-parquet --input data.psv --delimiter "|"
```

## 6. Use a Config File

Instead of passing flags every time, create a `config.yaml`:

```yaml
input: "./incoming"
output: "./converted"
delete_original: false
log_level: info
batch_size: 10000
delimiter: ","
sample_rows: 100
```

Then run:

```bash
./csv-to-parquet --config config.yaml
```

CLI flags still override config file values:

```bash
./csv-to-parquet --config config.yaml --log-level debug
```

## 7. Tune for Large Files

For very large CSVs, increase the batch size to reduce memory overhead per row group:

```bash
./csv-to-parquet --input large.csv --batch-size 50000
```

## 8. Debug Issues

Enable debug logging to see schema detection details and per-row processing:

```bash
./csv-to-parquet --input data.csv --log-level debug
```

## What's Next?

See the full [README](README.md) for all options, type detection details, and project structure.
