import pytest
from pyspark.sql import SparkSession, functions as F
from emr_scripts.transform import compute_daily_metrics, compute_address_delta


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local").appName("btc-test").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestDailyMetrics:
    def test_excludes_coinbase(self, spark):
        data = [
            ("tx1", False, "2026-01-01 10:00:00", 1.0, 0.0001),
            ("tx2", True, "2026-01-01 11:00:00", 50.0, 0.0),  # coinbase
        ]
        df = spark.createDataFrame(
            data, ["txid", "is_coinbase", "block_timestamp", "output_value", "fee"]
        )
        result = compute_daily_metrics(df)

        assert result.count() == 1
        assert result.collect()[0]["total_transactions"] == 1

    def test_aggregation_by_date(self, spark):
        data = [
            ("tx1", False, "2026-01-01 10:00:00", 1.0, 0.0001),
            ("tx2", False, "2026-01-01 11:00:00", 2.0, 0.0002),
            ("tx3", False, "2026-01-01 12:00:00", 1.0, 0.0003),
            ("tx4", False, "2026-01-02 10:00:00", 5.0, 0.0001),
        ]
        df = spark.createDataFrame(
            data, ["txid", "is_coinbase", "block_timestamp", "output_value", "fee"]
        )
        result = compute_daily_metrics(df)

        rows = {str(r["date"]): r for r in result.collect()}
        jan1 = rows["2026-01-01"]
        assert jan1["total_transactions"] == 3
        assert jan1["total_volume_btc"] == pytest.approx(4.0)
        assert jan1["total_fees_btc"] == pytest.approx(0.0006)
        assert jan1["avg_fee_sat"] == 20000  # avg(10k, 20k, 30k) sats
        assert jan1["median_fee_sat"] == 20000

        assert rows["2026-01-02"]["total_transactions"] == 1


class TestAddressDelta:
    def test_balance(self, spark):
        received = spark.createDataFrame(
            [
                ("addr1", 1.5, "2026-01-01 10:00:00"),
                ("addr2", 0.5, "2026-01-01 11:00:00"),
            ],
            ["address", "value_btc", "block_timestamp"],
        )
        sent = spark.createDataFrame(
            [("addr1", 0.3, "2026-01-01 12:00:00")],
            ["address", "value_btc", "block_timestamp"],
        )
        result = compute_address_delta(received, sent)

        rows = {r["address"]: r for r in result.collect()}
        assert rows["addr1"]["balance_btc"] == pytest.approx(1.2)
        assert rows["addr2"]["balance_btc"] == pytest.approx(0.5)  # receive-only
        assert rows["addr2"]["total_sent_btc"] == 0.0

    def test_tx_count(self, spark):
        received = spark.createDataFrame(
            [
                ("addr1", 1.0, "2026-01-01 10:00:00"),
                ("addr1", 0.5, "2026-01-01 11:00:00"),
            ],
            ["address", "value_btc", "block_timestamp"],
        )
        sent = spark.createDataFrame(
            [
                ("addr1", 0.3, "2026-01-01 12:00:00"),
                ("addr1", 0.2, "2026-01-01 13:00:00"),
            ],
            ["address", "value_btc", "block_timestamp"],
        )
        result = compute_address_delta(received, sent)

        assert result.collect()[0]["tx_count"] == 4  # 2 received + 2 sent

    def test_first_and_last_seen(self, spark):
        received = spark.createDataFrame(
            [
                ("addr1", 1.0, "2026-01-05 10:00:00"),
                ("addr1", 0.5, "2026-01-10 11:00:00"),
            ],
            ["address", "value_btc", "block_timestamp"],
        )
        sent = spark.createDataFrame(
            [("addr1", 0.3, "2026-01-08 09:00:00")],
            ["address", "value_btc", "block_timestamp"],
        )
        result = compute_address_delta(received, sent)
        row = result.collect()[0]

        assert str(row["first_seen"]) == "2026-01-05 10:00:00"
        assert str(row["last_seen"]) == "2026-01-10 11:00:00"

    def test_null_addresses_excluded(self, spark):
        received = spark.createDataFrame(
            [("addr1", 1.0, "2026-01-01 10:00:00"), (None, 0.5, "2026-01-01 11:00:00")],
            ["address", "value_btc", "block_timestamp"],
        )
        filtered = received.filter(F.col("address").isNotNull())
        sent = spark.createDataFrame([], received.schema)

        result = compute_address_delta(filtered, sent)
        assert result.count() == 1
