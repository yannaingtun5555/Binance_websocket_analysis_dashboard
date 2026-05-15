import json
import os
import time
from datetime import datetime

from clickhouse_driver import Client
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
TOPICS = ("crypto.trades", "crypto.klines", "crypto.ticker")


def parse_datetime(value):
    return datetime.fromisoformat(value)


def ensure_schema(client):
    client.execute("CREATE DATABASE IF NOT EXISTS crypto")
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto.trades
        (
            symbol String,
            price Float64,
            qty Float64,
            trade_time DateTime
        )
        ENGINE = MergeTree
        ORDER BY (symbol, trade_time)
        """
    )
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto.klines
        (
            symbol String,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            interval String,
            start_time DateTime
        )
        ENGINE = MergeTree
        ORDER BY (symbol, start_time)
        """
    )
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto.ticker
        (
            symbol String,
            price Float64,
            change_percent Float64,
            volume Float64,
            event_time DateTime
        )
        ENGINE = MergeTree
        ORDER BY (symbol, event_time)
        """
    )


def wait_for_clickhouse(max_retries=30, delay=2):
    for attempt in range(max_retries):
        try:
            client = Client(host=CLICKHOUSE_HOST)
            client.execute("SELECT 1")
            return client
        except Exception as exc:
            print(f"ClickHouse not ready ({attempt + 1}/{max_retries}): {exc}", flush=True)
            time.sleep(delay)
    raise RuntimeError("ClickHouse did not become ready")


def insert_record(client, topic, data):
    if topic == "crypto.trades":
        client.execute(
            "INSERT INTO crypto.trades (symbol, price, qty, trade_time) VALUES",
            [(
                data["symbol"],
                float(data["price"]),
                float(data["qty"]),
                parse_datetime(data["trade_time"]),
            )],
        )
    elif topic == "crypto.klines":
        client.execute(
            """
            INSERT INTO crypto.klines
            (symbol, open, high, low, close, volume, interval, start_time)
            VALUES
            """,
            [(
                data["symbol"],
                float(data["open"]),
                float(data["high"]),
                float(data["low"]),
                float(data["close"]),
                float(data["volume"]),
                data["interval"],
                parse_datetime(data["start_time"]),
            )],
        )
    elif topic == "crypto.ticker":
        client.execute(
            """
            INSERT INTO crypto.ticker
            (symbol, price, change_percent, volume, event_time)
            VALUES
            """,
            [(
                data["symbol"],
                float(data["price"]),
                float(data["change_percent"]),
                float(data["volume"]),
                parse_datetime(data["event_time"]),
            )],
        )


def main():
    clickhouse = wait_for_clickhouse()
    ensure_schema(clickhouse)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="crypto-clickhouse-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    )

    print(f"Consuming {', '.join(TOPICS)} from {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
    for message in consumer:
        insert_record(clickhouse, message.topic, message.value)


if __name__ == "__main__":
    main()
