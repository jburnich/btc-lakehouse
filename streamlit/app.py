import os
import time

import boto3
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

REGION = os.environ["AWS_REGION"]
DATABASE = "btc_lakehouse"
WORKGROUP = os.environ["ATHENA_WORKGROUP"]

st.set_page_config(page_title="BTC On-Chain Dashboard", page_icon="₿", layout="wide")
st.title("₿ Bitcoin On-Chain — Daily Metrics")
st.caption("AWS Public Blockchain · Gold layer S3 (Parquet) · Athena")


@st.cache_resource
def athena_client():
    return boto3.client("athena", region_name=REGION)


def query_athena(sql: str) -> pd.DataFrame:
    client = athena_client()
    exec_id = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"]["Status"]
        if status["State"] == "SUCCEEDED":
            break
        if status["State"] in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena: {status.get('StateChangeReason', status['State'])}")
        time.sleep(0.5)

    pages = client.get_paginator("get_query_results").paginate(QueryExecutionId=exec_id)
    columns, rows = None, []
    for page in pages:
        rs = page["ResultSet"]
        if columns is None:
            columns = [c["Name"] for c in rs["ResultSetMetadata"]["ColumnInfo"]]
        for row in rs["Rows"][1:]:
            rows.append([f.get("VarCharValue", "") for f in row["Data"]])
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=300)
def load_daily() -> pd.DataFrame:
    df = query_athena("SELECT * FROM daily_metrics ORDER BY date")
    numeric = ["total_transactions", "total_volume_btc", "total_fees_btc", "avg_fee_sat", "median_fee_sat"]
    df[numeric] = df[numeric].apply(pd.to_numeric, errors="coerce")
    return df


with st.spinner("Chargement des données..."):
    df = load_daily()

st.subheader("Métriques journalières")

display = df[["date", "total_transactions", "total_volume_btc", "total_fees_btc", "avg_fee_sat", "median_fee_sat"]].rename(columns={
    "date": "Date",
    "total_transactions": "Transactions",
    "total_volume_btc": "Volume (BTC)",
    "total_fees_btc": "Frais totaux (BTC)",
    "avg_fee_sat": "Frais moyen (sats)",
    "median_fee_sat": "Frais médian (sats)",
})
st.dataframe(display, use_container_width=True, hide_index=True)
