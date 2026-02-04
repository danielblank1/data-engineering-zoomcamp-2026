#!/usr/bin/env python3
"""
Setup DuckDB tables for NYC Yellow Taxi data.

This script creates:
1. External table (view) - reads directly from Parquet files (like BigQuery external table)
2. Materialized table - data loaded into DuckDB (like BigQuery regular table)
3. Partitioned + Clustered table - optimized for the homework queries

DuckDB is a columnar database, so it demonstrates the same concepts as BigQuery:
- Columnar storage
- Partition pruning
- Clustering for sort order
"""

import duckdb
from pathlib import Path

DATA_DIR = Path("/app/data")
DB_PATH = Path("/app/data/taxi.duckdb")

def get_parquet_files():
    """Get list of parquet files."""
    files = sorted(DATA_DIR.glob("yellow_tripdata_2024-*.parquet"))
    if not files:
        raise FileNotFoundError(
            "No parquet files found! Run download_data.py first."
        )
    return files

def setup_database():
    """Create all tables in DuckDB."""
    
    files = get_parquet_files()
    print(f"Found {len(files)} parquet files")
    for f in files:
        print(f"  - {f.name}")
    
    # Create glob pattern for all files
    parquet_glob = str(DATA_DIR / "yellow_tripdata_2024-*.parquet")
    
    # Connect to DuckDB (persistent database)
    con = duckdb.connect(str(DB_PATH))
    
    print("\n" + "=" * 60)
    print("Creating tables...")
    print("=" * 60)
    
    # =========================================================================
    # 1. EXTERNAL TABLE (View over Parquet files)
    # This is like BigQuery's external table - reads directly from files
    # =========================================================================
    print("\n1. Creating EXTERNAL TABLE (yellow_taxi_external)...")
    print("   This reads directly from Parquet files without loading data.")
    
    con.execute(f"""
        CREATE OR REPLACE VIEW yellow_taxi_external AS
        SELECT * FROM read_parquet('{parquet_glob}')
    """)
    
    # Get row count
    count = con.execute("SELECT COUNT(*) FROM yellow_taxi_external").fetchone()[0]
    print(f"   ✓ Created view with {count:,} rows")
    
    # =========================================================================
    # 2. MATERIALIZED TABLE (Data loaded into DuckDB)
    # This is like BigQuery's regular table - data is stored in DuckDB
    # =========================================================================
    print("\n2. Creating MATERIALIZED TABLE (yellow_taxi_materialized)...")
    print("   This loads all data into DuckDB's columnar storage.")
    
    con.execute(f"""
        CREATE OR REPLACE TABLE yellow_taxi_materialized AS
        SELECT * FROM read_parquet('{parquet_glob}')
    """)
    
    count = con.execute("SELECT COUNT(*) FROM yellow_taxi_materialized").fetchone()[0]
    print(f"   ✓ Created table with {count:,} rows")
    
    # =========================================================================
    # 3. PARTITIONED + CLUSTERED TABLE
    # Partitioned by tpep_dropoff_datetime (date), clustered by VendorID
    # This is the optimized table for Question 5
    # =========================================================================
    print("\n3. Creating PARTITIONED + CLUSTERED TABLE (yellow_taxi_partitioned)...")
    print("   Partitioned by: DATE(tpep_dropoff_datetime)")
    print("   Clustered by: VendorID")
    
    # DuckDB doesn't have native partitioning like BigQuery, but we can:
    # 1. Add a partition column
    # 2. Sort by partition + cluster columns for similar benefits
    con.execute(f"""
        CREATE OR REPLACE TABLE yellow_taxi_partitioned AS
        SELECT 
            *,
            DATE_TRUNC('day', tpep_dropoff_datetime) AS dropoff_date_partition
        FROM read_parquet('{parquet_glob}')
        ORDER BY dropoff_date_partition, VendorID
    """)
    
    count = con.execute("SELECT COUNT(*) FROM yellow_taxi_partitioned").fetchone()[0]
    print(f"   ✓ Created table with {count:,} rows")
    
    # =========================================================================
    # Show schema
    # =========================================================================
    print("\n" + "=" * 60)
    print("Table Schema (yellow_taxi_materialized):")
    print("=" * 60)
    
    schema = con.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'yellow_taxi_materialized'
        ORDER BY ordinal_position
    """).fetchall()
    
    for col_name, data_type in schema:
        print(f"  {col_name}: {data_type}")
    
    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 60)
    print("Setup Complete!")
    print("=" * 60)
    print("""
Tables created:
  1. yellow_taxi_external     - External table (view over Parquet files)
  2. yellow_taxi_materialized - Regular table (data in DuckDB)
  3. yellow_taxi_partitioned  - Partitioned by dropoff date, clustered by VendorID

To query, run:
  python scripts/query.py

Or use DuckDB CLI:
  python -c "import duckdb; con = duckdb.connect('data/taxi.duckdb'); ..."
""")
    
    con.close()

if __name__ == "__main__":
    setup_database()
