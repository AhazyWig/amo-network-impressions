from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import os
import polars as pl
import pandas as pd
from datetime import datetime

GZ_FILE = "NetworkBackfillImpressions.gz"
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.network_impressions_fact"

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

def transform_agg(df):
    logging.info("Transforming data")

    df = df.with_columns(
        pl.col("Time").str.strptime(pl.Datetime, "%Y-%m-%d-%H:%M:%S", strict=False).alias("event_time"),
        pl.col("EstimatedBackfillRevenue").cast(pl.Float64)
    )
    # Create dt column for partition
    df = df.with_columns(pl.col("event_time").dt.date().alias("dt"))

    agg_df = (
        df.groupby(["session_id", "Product", "DeviceCategory", "dt"])
          .agg([
              pl.sum("EstimatedBackfillRevenue").alias("total_cost"),
              pl.count("ImpressionId").alias("impression_count")
          ])
    )
    
    return agg_df



def create_table_if_not_exists():
    logging.debug(f"Creating table {BQ_TABLE} if not exists")
    schema = [
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("Product", "STRING"),
        bigquery.SchemaField("DeviceCategory", "STRING"),
        bigquery.SchemaField("total_cost", "FLOAT"),
        bigquery.SchemaField("impression_count", "INT64"),
        bigquery.SchemaField("dt", "DATE"),
    ]
    table = bigquery.Table(BQ_TABLE, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="dt")
    table.clustering_fields = ["Product", "DeviceCategory"]
    try:
        client.create_table(table, exists_ok=True)
        logging.info(f"Table {BQ_TABLE} created or already exists.")
    except Exception as e:
        logging.error(f"Failed to create table {BQ_TABLE}: {e}")


def upload_to_bigquery(df):
    if df.is_empty():
        logging.info("No data to upload")
        return
    
    pandas_df = df.to_pandas()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    try:
        job = client.load_table_from_dataframe(pandas_df, BQ_TABLE, job_config=job_config)
        job.result()
        logging.info(f"Inserted {len(pandas_df)} records into {BQ_TABLE}")
    except Exception as e:
        logging.error(f"Error inserting records: {e}")


if __name__ == "__main__":
    create_table_if_not_exists()
    df = read_file(GZ_FILE)
    df = extract_session_id(df)
    agg_df = transform_agg(df)
    upload_to_bigquery(agg_df)
    logging.info("Success")
    
