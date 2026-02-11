import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

data_dir = Path("data") / "fhv"
data_dir.mkdir(exist_ok=True, parents=True)

for month in range(1, 13):
    parquet_filename = f"fhv_tripdata_2019-{month:02d}.parquet"
    parquet_filepath = data_dir / parquet_filename

    if parquet_filepath.exists():
        print(f"Skipping {parquet_filename} (already exists)")
        continue

    csv_gz_filename = f"fhv_tripdata_2019-{month:02d}.csv.gz"
    csv_gz_filepath = data_dir / csv_gz_filename

    print(f"Downloading {csv_gz_filename}...")
    response = requests.get(f"{BASE_URL}/fhv/{csv_gz_filename}", stream=True)
    response.raise_for_status()

    with open(csv_gz_filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Converting to Parquet...")
    con = duckdb.connect()
    con.execute(f"""
        COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
        TO '{parquet_filepath}' (FORMAT PARQUET)
    """)
    con.close()
    csv_gz_filepath.unlink()
    print(f"Completed {parquet_filename}")

# Load into DuckDB
con = duckdb.connect("taxi_rides_ny.duckdb")
con.execute("CREATE SCHEMA IF NOT EXISTS prod")
con.execute("""
    CREATE OR REPLACE TABLE prod.fhv_tripdata AS
    SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
""")
count = con.execute("SELECT COUNT(*) FROM prod.fhv_tripdata").fetchone()[0]
print(f"Loaded {count:,} rows into prod.fhv_tripdata")
con.close()
