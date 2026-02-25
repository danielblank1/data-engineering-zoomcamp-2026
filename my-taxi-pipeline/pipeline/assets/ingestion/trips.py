"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "Pickup timestamp"
  - name: dropoff_datetime
    type: timestamp
    description: "Dropoff timestamp"
  - name: payment_type
    type: integer
    description: "Payment type ID"
  - name: total_amount
    type: double
    description: "Total fare amount"
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp of data extraction"

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2022-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2022-02-01")
    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get(
        "taxi_types", ["yellow", "green"]
    )

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    frames = []
    current = start
    while current < end:
        year = current.year
        month = current.month
        for taxi_type in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            try:
                df = pd.read_parquet(url)
                df["taxi_type"] = taxi_type
                frames.append(df)
            except Exception as e:
                print(f"Failed to fetch {url}: {e}")

        # Move to next month
        if month == 12:
            current = current.replace(year=year + 1, month=1)
        else:
            current = current.replace(month=month + 1)

    if frames:
        final_df = pd.concat(frames, ignore_index=True)
        final_df["extracted_at"] = datetime.utcnow()
        return final_df

    return pd.DataFrame()
