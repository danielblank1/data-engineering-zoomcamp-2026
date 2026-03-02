import os
import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, max as spark_max
from pyspark.sql.functions import unix_timestamp


import urllib.request

for url, filename in [
    (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet",
        "yellow_tripdata_2025-11.parquet",
    ),
    (
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
        "taxi_zone_lookup.csv",
    ),
]:
    if not os.path.exists(filename):
        print(f"Downloading {filename}...")
        urllib.request.urlretrieve(url, filename)

# Create Spark session
spark = SparkSession.builder.master("local[*]").appName("Module6Homework").getOrCreate()

# ── Q1: Spark Version ──
print(f"\n{'='*50}")
print(f"Q1 - Spark Version: {spark.version}")
print(f"{'='*50}")

# ── Q2: Repartition & Parquet Size ──
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
df.repartition(4).write.parquet("yellow_2025_11_partitioned", mode="overwrite")

files = glob.glob("yellow_2025_11_partitioned/*.parquet")
sizes = [os.path.getsize(f) / (1024 * 1024) for f in files]
avg_size = sum(sizes) / len(sizes)

print(f"\n{'='*50}")
print(f"Q2 - Parquet file sizes:")
for f, s in zip(files, sizes):
    print(f"  {os.path.basename(f)}: {s:.1f} MB")
print(f"  Average: {avg_size:.1f} MB")
print(f"{'='*50}")

# ── Q3: Trips on Nov 15 ──
df = spark.read.parquet("yellow_2025_11_partitioned")

nov15_count = df.filter(to_date(col("tpep_pickup_datetime")) == "2025-11-15").count()

print(f"\n{'='*50}")
print(f"Q3 - Trips on Nov 15: {nov15_count:,}")
print(f"{'='*50}")

# ── Q4: Longest Trip in Hours ──

df = df.withColumn(
    "trip_duration_hours",
    (
        unix_timestamp(col("tpep_dropoff_datetime"))
        - unix_timestamp(col("tpep_pickup_datetime"))
    )
    / 3600,
)

longest = df.agg(spark_max("trip_duration_hours")).collect()[0][0]

print(f"\n{'='*50}")
print(f"Q4 - Longest trip: {longest:.1f} hours")
print(f"{'='*50}")

# ── Q5: Spark UI Port ──
print(f"\n{'='*50}")
print(f"Q5 - Spark UI Port: 4040")
print(f"{'='*50}")

# ── Q6: Least Frequent Pickup Zone ──
zones = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)

df.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

least_frequent = spark.sql(
    """
    SELECT z.Zone, COUNT(*) as cnt
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY cnt ASC
    LIMIT 5
"""
).collect()

print(f"\n{'='*50}")
print(f"Q6 - Least frequent pickup zones:")
for row in least_frequent:
    print(f"  {row['Zone']}: {row['cnt']:,}")
print(f"{'='*50}")

# Cleanup
spark.stop()
print("\nDone!")