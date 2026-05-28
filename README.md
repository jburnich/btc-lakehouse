# btc-lakehouse
End-to-end Bitcoin analytics platform

## Installation

**Requirements:** Python 3.13 and [uv](https://docs.astral.sh/uv/getting-started/installation/)

```bash
git clone https://github.com/jburnich/btc-lakehouse.git
cd btc-lakehouse

# Create virtual environment and install dependencies
uv sync

# Configure environment variables
cp .env.example .env
```

## Data source

Bitcoin on-chain transaction data from the [AWS Public Blockchain dataset](https://registry.opendata.aws/aws-public-blockchain/), available as Parquet files on S3 (`s3://aws-public-blockchain/v1.0/btc/`).

## Usage

Download a daily BTC transaction partition:

```bash
uv run ingest 2026-01-01
```

## Tests

```bash
uv run pytest
```
