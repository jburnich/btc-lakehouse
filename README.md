# btc-lakehouse

End-to-end Bitcoin on-chain analytics pipeline — from raw S3 ingestion to a Streamlit dashboard.

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

## Setup

After provisioning the infrastructure, synchronize the Iceberg tables with the Glue catalog. This command is idempotent. Re-run after any schema change in [src/tables.json](src/tables.json):

```bash
uv run sync-tables
```

## Usage

```bash
# Fetch a daily BTC partition and upload it to S3
uv run ingest 2026-01-01

# Compute all gold metrics from the raw partition (daily_metrics + address_stats)
uv run transform 2026-01-01

# Compute a specific job only
uv run transform 2026-01-01 --job daily_metrics
uv run transform 2026-01-01 --job address_stats
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

## Orchestration

Flows are orchestrated with [Prefect Cloud](https://app.prefect.cloud) (free tier — 1 workspace, 5 deployments).

### Prefect Cloud setup

1. Create an account on [app.prefect.cloud](https://app.prefect.cloud)
2. Create a **Managed** work pool named `managed-execution`
3. Add the following variables in Prefect Cloud (**Settings → Variables**):
   - `aws-bucket-name`, `aws-region`, `emr-application-id`, `emr-execution-role-arn`
4. Add the following secrets (**Settings → Blocks → Secret**):
   - `aws-access-key-id`, `aws-secret-access-key`

### CI/CD

Flows are automatically deployed to Prefect Cloud on every push to `main` when `prefect.yaml` or `src/flows.py` changes.

Add the following secrets to your GitHub repository (**Settings → Secrets → Actions**):
- `PREFECT_API_KEY`
- `PREFECT_API_URL`

## Dashboard

```bash
streamlit run app.py
```

Displays daily Bitcoin network metrics (transactions, volume, fees) queried from Athena.

## Tests

```bash
uv run pytest
```
