# Binance WebSocket Analysis Dashboard

Real-time crypto data pipeline using Binance WebSocket, Kafka, ClickHouse, and Streamlit.

## What it does
- Streams `BTCUSDT` trade, kline, and ticker events from Binance.
- Publishes events to Kafka topics.
- Consumes Kafka messages and writes them to ClickHouse.
- Visualizes live and historical data in a Streamlit dashboard.

## Architecture
`Binance WebSocket -> Producer -> Kafka -> Consumer -> ClickHouse -> Streamlit`

Kafka topics:
- `crypto.trades`
- `crypto.klines`
- `crypto.ticker`

ClickHouse tables:
- `crypto.trades`
- `crypto.klines`
- `crypto.ticker`

## Project structure
- `pipeline/producer/` Binance WebSocket producer
- `pipeline/consumer/` Kafka consumer -> ClickHouse writer
- `init/` Kafka topic creation and ClickHouse init SQL
- `Dashboard/` Streamlit dashboard
- `docker-compose.yml` full local stack

## Prerequisites
- Docker
- Docker Compose

## Quick start
1. Start all services:

```bash
docker compose up --build
```

2. Open dashboard:
- http://localhost:8501

3. Verify data in ClickHouse (optional):

```bash
docker exec -it clickhouse-server clickhouse-client
```

Then run:

```sql
SELECT count() FROM crypto.trades;
SELECT count() FROM crypto.klines;
SELECT count() FROM crypto.ticker;
```

## Services and ports
- Streamlit: `8501`
- Kafka (host access): `9092`
- ClickHouse HTTP: `8123`
- ClickHouse native: `9000`
- ZooKeeper: `2181`

## Stop
```bash
docker compose down
```

To also remove persisted ClickHouse data:

```bash
docker compose down -v
```

## Notes
- Default ClickHouse user is configured with empty password in `init/clickhouse_default_user.xml`.
- Producer currently streams only `BTCUSDT`.
