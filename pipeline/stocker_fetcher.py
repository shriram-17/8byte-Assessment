import os
import yfinance as yf
from datetime import datetime, timedelta


def fetch_and_upsert(conn, symbol, days_back=30):
    cursor = conn.cursor()

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days_back)

    df = yf.download(
        symbol,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        interval="1d",
        progress=False
    )

    if df.empty:
        raise ValueError(f"No data received for {symbol} from Yahoo Finance")

    for date, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stock_data(symbol, day, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, day) DO UPDATE
            SET open=EXCLUDED.open,
                high=EXCLUDED.high,
                low=EXCLUDED.low,
                close=EXCLUDED.close,
                volume=EXCLUDED.volume;
        """, (symbol, date.date(), row["Open"], row["High"], row["Low"], row["Close"], int(row["Volume"])))

    conn.commit()

    return f"Inserted/Updated {len(df)} rows"
