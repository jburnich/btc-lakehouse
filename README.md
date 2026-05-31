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

```bash
# Fetch a daily BTC partition and upload it to S3
uv run ingest 2026-01-01

# Compute gold metrics from the raw partition
uv run transform 2026-01-01
```

## Infrastructure

The infrastructure is managed with [Terraform](https://developer.hashicorp.com/terraform) >= 1.14. It provisions:
- S3 bucket (raw + gold layers)
- EMR Serverless application (Spark jobs)
- Glue catalog database
- IAM roles

The Terraform state is stored in a private S3 backend. The backend bucket must be created manually beforehand and accessible (read + write) by your AWS user. Set `TF_BACKEND_BUCKET` in your `.env`.

```bash
source .env
cd terraform
terraform init -backend-config="bucket=$TF_BACKEND_BUCKET"
terraform apply
```

After applying, retrieve the outputs and add them to your `.env`:

```bash
terraform output emr_application_id
terraform output emr_execution_role_arn
```

```
export EMR_APPLICATION_ID=<emr_application_id>
export EMR_EXECUTION_ROLE_ARN=<emr_execution_role_arn>
```

## Tests

```bash
uv run pytest
```
