"""
Question 3: Count how many trips have trip_distance > 5.0 km.
Reads all messages from 'green-trips' from the earliest offset.
"""
import json

from kafka import KafkaConsumer

SERVER = 'localhost:9092'
TOPIC = 'green-trips'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[SERVER],
    auto_offset_reset='earliest',
    group_id='green-trips-q3',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=10_000,  # stop after 10s of no new messages
)

print(f"Reading all messages from '{TOPIC}'...")

total = 0
over_5 = 0

for message in consumer:
    trip = message.value
    total += 1
    if trip.get('trip_distance', 0) > 5.0:
        over_5 += 1

consumer.close()

print(f"Total trips: {total:,}")
print(f"Trips with trip_distance > 5.0: {over_5:,}")
