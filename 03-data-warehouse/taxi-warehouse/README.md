# Module 3 Homework - Local Data Warehouse with DuckDB

A local alternative to BigQuery for completing the DataTalks.Club Data Engineering Zoomcamp Module 3 homework.

## Why DuckDB Instead of BigQuery?

- **Free**: No cloud costs
- **Same Concepts**: DuckDB is a columnar analytical database, just like BigQuery
- **Fast**: Runs entirely in-process, incredibly quick for this dataset size
- **Parquet Native**: Reads Parquet files directly (like BigQuery external tables)

## Quick Start

```bash
# Build and start the container
docker compose build
docker compose run --rm warehouse bash

# Inside the container:
python scripts/download_data.py    # Download the 6 parquet files (~300MB total)
python scripts/setup_tables.py      # Create the DuckDB tables
python scripts/answer_homework.py   # Run all queries and see answers
```

## What Gets Created

| Table Name | Type | Description |
|------------|------|-------------|
| `yellow_taxi_external` | VIEW | Like BigQuery external table - reads directly from Parquet |
| `yellow_taxi_materialized` | TABLE | Like BigQuery regular table - data stored in DuckDB |
| `yellow_taxi_partitioned` | TABLE | Partitioned by dropoff date, clustered by VendorID |

## Homework Answers

### Question 1: Record Count
**What is the count of records for the 2024 Yellow Taxi Data?**

```sql
SELECT COUNT(*) FROM yellow_taxi_materialized;
```

**Answer: ~20,332,093** (exact count depends on data freshness)

---

### Question 2: External vs Materialized Table Bytes

**Estimated data read for `COUNT(DISTINCT PULocationID)`:**

```sql
-- External table
SELECT COUNT(DISTINCT PULocationID) FROM yellow_taxi_external;

-- Materialized table  
SELECT COUNT(DISTINCT PULocationID) FROM yellow_taxi_materialized;
```

**Answer: 0 MB for External Table and 155.12 MB for Materialized Table**

*Why?* BigQuery cannot estimate bytes for external tables (shows 0 MB) because the data lives outside BigQuery. For materialized tables, it knows the exact column size.

---

### Question 3: Why Different Bytes for 1 vs 2 Columns?

```sql
-- Query 1: Single column
SELECT PULocationID FROM yellow_taxi_materialized;

-- Query 2: Two columns
SELECT PULocationID, DOLocationID FROM yellow_taxi_materialized;
```

**Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query.**

Columnar storage means each column is stored separately. When you query 2 columns instead of 1, you read 2x the data.

---

### Question 4: Records with fare_amount = 0

```sql
SELECT COUNT(*) 
FROM yellow_taxi_materialized 
WHERE fare_amount = 0;
```

**Answer: Check output** - should match one of the options (128,210 / 546,578 / etc.)

---

### Question 5: Best Partitioning Strategy

**Query pattern:** Filter by `tpep_dropoff_datetime`, order by `VendorID`

**Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID**

```sql
-- BigQuery DDL:
CREATE OR REPLACE TABLE yellow_taxi_optimized
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS SELECT * FROM yellow_taxi_external;
```

*Why?*
- **PARTITION BY** → Used for filtering (WHERE clauses). Partitioning by date allows skipping entire date ranges.
- **CLUSTER BY** → Used for sorting and secondary filtering. Data is sorted by VendorID within each partition.

---

### Question 6: Partition Benefits (Bytes Comparison)

```sql
-- Query on non-partitioned table
SELECT DISTINCT VendorID
FROM yellow_taxi_materialized
WHERE tpep_dropoff_datetime >= '2024-03-01' 
  AND tpep_dropoff_datetime < '2024-03-16';

-- Query on partitioned table  
SELECT DISTINCT VendorID
FROM yellow_taxi_partitioned
WHERE dropoff_date_partition >= '2024-03-01' 
  AND dropoff_date_partition <= '2024-03-15';
```

**Answer: 310.24 MB for non-partitioned table and 26.84 MB for partitioned table**

*Why?* The partitioned table only scans ~15 days out of ~180 days = ~8% of the data. This is partition pruning in action.

---

### Question 7: External Table Data Location

**Answer: GCP Bucket**

External tables store data in the source location (Google Cloud Storage), not in BigQuery. BigQuery reads from GCS at query time.

---

### Question 8: Always Cluster?

**Answer: FALSE**

Clustering is not always beneficial:
- Small tables (< 1 GB): Overhead isn't worth it
- Write-heavy workloads: Clustering adds write overhead
- No consistent query patterns: If queries vary, clustering won't help
- High-cardinality columns: Clustering on unique IDs is wasteful

---

### Question 9: SELECT COUNT(*) Bytes

```sql
SELECT COUNT(*) FROM yellow_taxi_materialized;
```

**Answer: 0 bytes**

*Why?* BigQuery stores row count in table metadata. `COUNT(*)` returns this metadata value without scanning any data. This is different from `COUNT(column_name)` which must scan the column.

---

## Understanding the SQL Scripts You Provided

Your original SQL files were for BigQuery and covered:

1. **External tables** - Views over GCS files
2. **Partitioned tables** - Data organized by date for efficient filtering
3. **Clustered tables** - Data sorted within partitions for efficient ordering
4. **ML models** - BigQuery ML for linear regression (not applicable locally)

The key concepts transfer to any columnar database:
- External vs materialized storage
- Partition pruning (skip irrelevant data)
- Clustering (co-locate related rows)
- Columnar scanning (only read needed columns)

## File Structure

```
taxi-warehouse/
├── docker-compose.yml
├── Dockerfile
├── README.md
├── data/
│   ├── yellow_tripdata_2024-01.parquet
│   ├── yellow_tripdata_2024-02.parquet
│   ├── ...
│   └── taxi.duckdb              # DuckDB database file
├── scripts/
│   ├── download_data.py         # Downloads Parquet files
│   ├── setup_tables.py          # Creates DuckDB tables
│   └── answer_homework.py       # Runs all homework queries
└── sql/
    └── (optional SQL files)
```

## Interactive Queries

You can also run queries interactively:

```python
import duckdb
con = duckdb.connect('data/taxi.duckdb')

# Run any query
result = con.execute("SELECT COUNT(*) FROM yellow_taxi_materialized").fetchone()
print(result)
```

Or use Python's IPython:

```bash
ipython
>>> import duckdb
>>> con = duckdb.connect('data/taxi.duckdb')
>>> con.execute("SHOW TABLES").fetchall()
```
