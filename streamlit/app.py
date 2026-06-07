import os
import re
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


_VALID_ADDR = re.compile(r"^[a-zA-Z0-9]{20,90}$")


def load_address(addr: str) -> pd.DataFrame:
    if not _VALID_ADDR.match(addr):
        raise ValueError("Adresse invalide")
    safe = addr.replace("'", "")
    return query_athena(f"SELECT * FROM address_stats WHERE address = '{safe}' LIMIT 1")


def fmt_btc(v: str) -> str:
    return f"{float(v):.8f}".rstrip("0").rstrip(".")


def fmt_date(v: str) -> str:
    return str(v)[:16] + " UTC"


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


_TOP_METRICS = {
    "Balance (BTC)": "balance_btc",
    "Reçu (BTC)": "total_received_btc",
    "Envoyé (BTC)": "total_sent_btc",
    "Transactions": "tx_count",
}


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def load_top50(order_by: str) -> pd.DataFrame:
    df = query_athena(f"SELECT * FROM address_stats ORDER BY {order_by} DESC LIMIT 50")
    for col in ["total_received_btc", "total_sent_btc", "balance_btc", "tx_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


tab_metrics, tab_address = st.tabs(["Activité quotidienne", "Recherche d'adresse"])

# Tab 1: Daily metrics
with tab_metrics:
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
                    styles.loc[row, col] = (
                        "background-color: #2d2d2d; color: transparent"
                    )
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

# Tab 2: Address lookup
with tab_address:
    st.markdown(
        "[En savoir plus sur les adresses Bitcoin](https://en.bitcoin.it/wiki/Address)"
    )
    address_input = st.text_input(
        "Adresse BTC",
        placeholder="Entrez une adresse Bitcoin",
        max_chars=100,
    )

    if address_input:
        try:
            with st.spinner("Recherche..."):
                result = load_address(address_input.strip())
            if result.empty:
                st.info("Adresse introuvable dans le dataset.")
            else:
                row = result.iloc[0]
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Reçu (BTC)", fmt_btc(row["total_received_btc"]))
                col2.metric("Envoyé (BTC)", fmt_btc(row["total_sent_btc"]))
                col3.metric("Balance (BTC)", fmt_btc(row["balance_btc"]))
                col4.metric("Transactions", row["tx_count"])
                st.markdown(f"Première activité - **{fmt_date(row['first_seen'])}**")
                st.markdown(f"Dernière activité - **{fmt_date(row['last_seen'])}**")
        except ValueError as e:
            st.warning(str(e))

    st.divider()
    st.subheader("Top 50 adresses")

    sort_label = st.selectbox("Classé par", options=list(_TOP_METRICS.keys()))
    sort_col = _TOP_METRICS[sort_label]

    with st.spinner("Chargement..."):
        top50 = load_top50(sort_col)

    display_top = top50[
        [
            "address",
            "balance_btc",
            "total_received_btc",
            "total_sent_btc",
            "tx_count",
            "first_seen",
            "last_seen",
        ]
    ].copy()
    for col in ["balance_btc", "total_received_btc", "total_sent_btc"]:
        display_top[col] = display_top[col].apply(lambda v: fmt_btc(str(v)))
    display_top["first_seen"] = display_top["first_seen"].apply(
        lambda v: fmt_date(str(v))
    )
    display_top["last_seen"] = display_top["last_seen"].apply(
        lambda v: fmt_date(str(v))
    )
    display_top = display_top.rename(
        columns={
            "address": "Adresse",
            "balance_btc": "Balance (BTC)",
            "total_received_btc": "Reçu (BTC)",
            "total_sent_btc": "Envoyé (BTC)",
            "tx_count": "Transactions",
            "first_seen": "Première activité",
            "last_seen": "Dernière activité",
        }
    )
    st.dataframe(display_top, use_container_width=True, hide_index=True)
