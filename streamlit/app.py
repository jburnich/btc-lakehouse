import os
import time

import boto3
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from PIL import Image

load_dotenv()

REGION = os.environ["AWS_REGION"]
DATABASE = "btc_lakehouse"
WORKGROUP = os.environ["ATHENA_WORKGROUP"]
CACHE_TTL = 300

st.set_page_config(
    page_title="Dashboard Bitcoin",
    page_icon=Image.open("streamlit/bitcoin_logo.png"),
    layout="wide",
)
st.title("₿ Dashboard Bitcoin")


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
        status = client.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"][
            "Status"
        ]
        if status["State"] == "SUCCEEDED":
            break
        if status["State"] in ("FAILED", "CANCELLED"):
            raise RuntimeError(
                f"Athena: {status.get('StateChangeReason', status['State'])}"
            )
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


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def load_daily() -> pd.DataFrame:
    df = query_athena("SELECT * FROM daily_metrics ORDER BY date")
    numeric = [
        "total_transactions",
        "total_volume_btc",
        "total_fees_btc",
        "avg_fee_sat",
        "median_fee_sat",
    ]
    df[numeric] = df[numeric].apply(pd.to_numeric, errors="coerce")
    return df


def line_chart(data: pd.DataFrame, column: str, y_title: str) -> None:
    st.vega_lite_chart(
        data.reset_index(),
        {
            "mark": {"type": "line", "color": "#1f77b4"},
            "encoding": {
                "x": {
                    "field": "date",
                    "type": "temporal",
                    "title": None,
                    "axis": {
                        "format": "%d %b",
                        "tickCount": "week",
                        "labelAngle": -45,
                    },
                },
                "y": {"field": column, "type": "quantitative", "title": y_title},
            },
        },
        use_container_width=True,
    )


with st.spinner("Chargement des données..."):
    df = load_daily()

df["date"] = pd.to_datetime(df["date"])
date_min, date_max = df["date"].min().date(), df["date"].max().date()

date_range = st.date_input(
    "Plage de dates",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

if len(date_range) == 2:
    start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
    filtered = df[(df["date"] >= start) & (df["date"] <= end)]
else:
    filtered = df

display = filtered[
    [
        "date",
        "total_transactions",
        "total_volume_btc",
        "total_fees_btc",
        "avg_fee_sat",
        "median_fee_sat",
    ]
].copy()
display["date"] = display["date"].dt.strftime("%Y-%m-%d")
display = display.rename(
    columns={
        "date": "Date",
        "total_transactions": "Transactions",
        "total_volume_btc": "Volume (BTC)",
        "total_fees_btc": "Frais totaux (BTC)",
        "avg_fee_sat": "Frais moyen (satoshis)",
        "median_fee_sat": "Frais médian (satoshis)",
    }
)
st.dataframe(display, use_container_width=True, hide_index=True)

chart_df = filtered.set_index("date")

st.subheader("Transactions par jour")
line_chart(chart_df, "total_transactions", "Transactions")

st.subheader("Volume (BTC) par jour")
line_chart(chart_df, "total_volume_btc", "Volume BTC")

st.subheader("Frais moyen par jour")
st.caption(
    "Le satoshi (sat) est la plus petite unité de Bitcoin : 1 BTC = 100 000 000 sats. Les frais de transaction sont exprimés en sats."
)
line_chart(chart_df, "avg_fee_sat", "Frais moyen (sats)")

st.subheader("Corrélations")
corr_cols = {
    "total_transactions": "Transactions",
    "total_volume_btc": "Volume BTC",
    "avg_fee_sat": "Frais moyen (sats)",
    "median_fee_sat": "Frais médian (sats)",
    "total_fees_btc": "Frais totaux BTC",
}
corr = filtered[list(corr_cols.keys())].rename(columns=corr_cols).corr()


def _style_corr(df: pd.DataFrame) -> pd.DataFrame:
    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    for row in df.index:
        for col in df.columns:
            if row == col:
                styles.loc[row, col] = "background-color: #2d2d2d; color: transparent"
            else:
                v = df.loc[row, col]
                if v >= 0:
                    r = int(255 - 75 * v)
                    g = int(255 - 251 * v)
                    b = int(255 - 217 * v)
                else:
                    r = int(59 - 59 * v)
                    g = int(76 - 76 * v)
                    b = int(192 + 63 * v)
                text = "black" if abs(v) < 0.6 else "white"
                styles.loc[row, col] = (
                    f"background-color: rgb({r},{g},{b}); color: {text}"
                )
    return styles


st.dataframe(
    corr.style.apply(_style_corr, axis=None).format("{:.2f}"),
    use_container_width=True,
)
