"""
Question 2: Send green taxi trip data to the 'green-trips' Kafka topic.
Measures total time to send all rows and flush.
"""
import json
from time import time

import pandas as pd
from kafka import KafkaProducer

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
COLUMNS = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount',
]
TOPIC = 'green-trips'
SERVER = 'localhost:9092'

df = pd.read_parquet(URL, columns=COLUMNS)

# Convert Timestamp columns to strings so they serialize cleanly to JSON
for col in ('lpep_pickup_datetime', 'lpep_dropoff_datetime'):
    df[col] = df[col].astype(str)

producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

t0 = time()

for _, row in df.iterrows():
    producer.send(TOPIC, value=row.to_dict())

producer.flush()

t1 = time()
print(f'Sent {len(df):,} rows to "{TOPIC}"')
print(f'took {(t1 - t0):.2f} seconds')
