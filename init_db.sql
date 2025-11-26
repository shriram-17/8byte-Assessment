CREATE TABLE IF NOT EXISTS stock_data (
    symbol TEXT,
    day DATE,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    PRIMARY KEY(symbol, day)
);
