from dagster import job, op, resource
import os
import psycopg2
from stocker_fetcher import fetch_and_upsert

@resource
def postgres_resource(_):
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB")
    )

@op(required_resource_keys={"postgres"})
def fetch_stock_data(context):
    symbol = os.getenv("DEFAULT_SYMBOL", "AAPL")
    days = int(os.getenv("DAYS_BACK", "30"))

    context.log.info(f"Fetching stock data for {symbol} (last {days} days)")
    result = fetch_and_upsert(context.resources.postgres, symbol, days)

    context.log.info(result)

@job(resource_defs={"postgres": postgres_resource})
def stock_job():
    fetch_stock_data()
