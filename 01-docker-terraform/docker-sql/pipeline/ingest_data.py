#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Yellow taxi dtype
yellow_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

yellow_parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

# Green taxi dtype
green_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    "trip_type": "Int64",
}

green_parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]


def detect_format(url):
    """Detect file format from URL."""
    url_lower = url.lower()
    if url_lower.endswith(".parquet"):
        return "parquet"
    elif url_lower.endswith(".csv.gz") or url_lower.endswith(".csv"):
        return "csv"
    return None


def read_data_chunked(
    url, file_format, chunksize, use_dtype=None, use_parse_dates=None
):
    """Generator that yields DataFrames in chunks."""
    if file_format == "csv":
        yield from pd.read_csv(
            url,
            dtype=use_dtype,
            parse_dates=use_parse_dates,
            iterator=True,
            chunksize=chunksize,
        )
    elif file_format == "parquet":
        df = pd.read_parquet(url)
        for start in range(0, len(df), chunksize):
            yield df.iloc[start : start + chunksize]
    else:
        raise ValueError(f"Unsupported format: {file_format}")


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--url", required=True, help="URL to data file")
@click.option("--target-table", required=True, help="Target table name")
@click.option(
    "--chunksize", default=100000, type=int, help="Chunk size for reading data"
)
@click.option(
    "--format",
    "file_format",
    default=None,
    type=click.Choice(["csv", "parquet"]),
    help="File format (auto-detected if not specified)",
)
@click.option(
    "--taxi-type",
    default=None,
    type=click.Choice(["yellow", "green"]),
    help="Taxi type for dtype hints (optional)",
)
def run(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    url,
    target_table,
    chunksize,
    file_format,
    taxi_type,
):
    """Ingest NYC taxi data into PostgreSQL database."""

    data_url = url

    # Set dtype based on taxi type (only applies to CSV)
    use_dtype = None
    use_parse_dates = None
    if taxi_type == "yellow":
        use_dtype = yellow_dtype
        use_parse_dates = yellow_parse_dates
    elif taxi_type == "green":
        use_dtype = green_dtype
        use_parse_dates = green_parse_dates

    # Detect or validate format
    if file_format is None:
        file_format = detect_format(data_url)
        if file_format is None:
            raise click.UsageError(
                "Cannot detect format from URL. Please specify --format (csv or parquet)"
            )
        print(f"Detected format: {file_format}")

    # Parquet handles its own types, so ignore dtype hints
    if file_format == "parquet":
        use_dtype = None
        use_parse_dates = None

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    df_iter = read_data_chunked(
        data_url, file_format, chunksize, use_dtype, use_parse_dates
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            print(f"Table {target_table} created")
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")
        print(f"Inserted: {len(df_chunk)}")

    print(f"Finished loading {target_table}!")


if __name__ == "__main__":
    run()