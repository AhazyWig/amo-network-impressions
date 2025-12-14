from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import os
import polars as pl
import pandas as pd
from datetime import datetime, timedelta

GZ_FILE = "NetworkBackfillImpressions.gz"
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")
RAW_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.network_impressions_raw"
FACT_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.network_impressions_fact"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# credentials = service_account.Credentials.from_service_account_file(BQ_CREDENTIALS_FILE)
client = bigquery.Client()

def read_file(filename: str):
    logging.info(f"Reading file {filename} ")
    try:
        df = pl.read_csv(filename)
        logging.info(f"Read {df.height} rows")
        return df
    except Exception as e:
        logging.error(f"Failed to read file: {e}")

def extract_session_id(df: pl.Dataframe):
    df = df.with_columns(
        pl.coalesce(
            pl.col("CustomTargeting").str.extract(r"(?:^|;)sessionid=([^;]+)"),
            pl.col("CustomTargeting").str.extract(r"(?:^|;)dfpsessionid=([^;]+)"),
            pl.lit("unknown")
        ).alias("session_id")
    )
    return df

def transform_raw(df):
    logging.info("Transforming data")

    df = df.with_columns(
            pl.coalesce(
                pl.col("CustomTargeting").str.extract(r"(?:^|;)sessionid=([^;]+)"),
                pl.col("CustomTargeting").str.extract(r"(?:^|;)dfpsessionid=([^;]+)"),
                pl.lit("unknown")
            ).alias("session_id"),
            pl.col("Time").str.strptime(pl.Datetime, "%Y-%m-%d-%H:%M:%S", strict=False).alias("event_time"),
            pl.col("EstimatedBackfillRevenue").cast(pl.Float64),
            pl.col("ImpressionId").cast(pl.Utf8)
        )
    # Створення dt для партицій
    df = df.with_columns(pl.col("event_time").dt.date().alias("dt"))
    return df

def upload_raw(df):
    if df.is_empty():
        logging.info("No data to upload")
        return
    client.load_table_from_dataframe(df.to_pandas(), RAW_TABLE, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()
    logging.info(f"Inserted {df.height} rows into RAW table")


def merge_fact(days_back):
    # Для спрощення вважаємо, що запізнілі події могли надійти кілька днів тому
    # Наприклад, беремо останні 3 дні
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days_back)

    query = f"""
    MERGE `{FACT_TABLE}` T
    USING (
        SELECT
            session_id,
            Product,
            DeviceCategory,
            dt,
            SUM(EstimatedBackfillRevenue) AS total_cost,
            COUNT(ImpressionId) AS impression_count
        FROM `{RAW_TABLE}`
        WHERE dt BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY session_id, Product, DeviceCategory, dt
    ) S
    ON T.session_id = S.session_id
       AND T.Product = S.Product
       AND T.DeviceCategory = S.DeviceCategory
       AND T.dt = S.dt
    WHEN MATCHED THEN
      UPDATE SET total_cost = S.total_cost, impression_count = S.impression_count
    WHEN NOT MATCHED THEN
      INSERT (session_id, Product, DeviceCategory, dt, total_cost, impression_count)
      VALUES (S.session_id, S.Product, S.DeviceCategory, S.dt, S.total_cost, S.impression_count)
    """
    client.query(query).result()
    logging.info(f"Fact table updated successfully for dates {start_date} and {end_date}")


def create_table_if_not_exists(table_name, raw):
    if raw:
        schema = [
            bigquery.SchemaField("session_id", "STRING"),
            bigquery.SchemaField("Product", "STRING"),
            bigquery.SchemaField("DeviceCategory", "STRING"),
            bigquery.SchemaField("event_time", "TIMESTAMP"),
            bigquery.SchemaField("EstimatedBackfillRevenue", "FLOAT"),
            bigquery.SchemaField("ImpressionId", "STRING"),
            bigquery.SchemaField("dt", "DATE"),
        ]
    else:
        schema = [
            bigquery.SchemaField("session_id", "STRING"),
            bigquery.SchemaField("Product", "STRING"),
            bigquery.SchemaField("DeviceCategory", "STRING"),
            bigquery.SchemaField("dt", "DATE"),
            bigquery.SchemaField("total_cost", "FLOAT"),
            bigquery.SchemaField("impression_count", "INT64"),
        ]

    table = bigquery.Table(table_name, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="dt")
    table.clustering_fields = ["Product", "DeviceCategory"]
    try:
        client.create_table(table, exists_ok=True)
        logging.info(f"Table {table_name} created or already exists.")
    except Exception as e:
        logging.error(f"Failed to create table {table_name}: {e}")
        raise

if __name__ == "__main__":
    create_table_if_not_exists(RAW_TABLE, raw=True)
    create_table_if_not_exists(FACT_TABLE, raw=False)
    df = read_file(GZ_FILE)
    df = extract_session_id(df)
    
    df = transform_raw(df)
    upload_raw(df)

    merge_fact(days_back=3)
    logging.info("Success")

