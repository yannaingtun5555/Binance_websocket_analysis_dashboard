CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.trades
(
    symbol String,
    price Float64,
    qty Float64,
    trade_time DateTime
)
ENGINE = MergeTree
ORDER BY (symbol, trade_time);

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
ORDER BY (symbol, start_time);

CREATE TABLE IF NOT EXISTS crypto.ticker
(
    symbol String,
    price Float64,
    change_percent Float64,
    volume Float64,
    event_time DateTime
)
ENGINE = MergeTree
ORDER BY (symbol, event_time);