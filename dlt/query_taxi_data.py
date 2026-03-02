import dlt
import duckdb


def get_connection():
    """Connect to the DuckDB database created by the pipeline."""
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data",
    )
    # Get the DuckDB file path from the pipeline
    db_path = pipeline.pipeline_name + ".duckdb"

    # Use dlt's sql_client for queries
    return pipeline


def run_queries():
    pipeline = get_connection()

    with pipeline.sql_client() as client:
        print("=" * 70)
        print("NYC TAXI DATA - EXPLORATION QUERIES")
        print("=" * 70)

        # 1. Total row count
        result = client.execute_sql("SELECT COUNT(*) FROM taxi_data.rides")
        print(f"\n1. Total records loaded: {result[0][0]}")

        # 2. Show table schema / columns
        print("\n2. Table columns:")
        result = client.execute_sql(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'taxi_data' AND table_name = 'rides' "
            "ORDER BY ordinal_position"
        )
        for row in result:
            print(f"   {row[0]:30s} {row[1]}")

        # 3. Sample rows
        print("\n3. First 3 rows (sample):")
        result = client.execute_sql("SELECT * FROM taxi_data.rides LIMIT 3")
        for row in result:
            print(f"   {row}")

        # 4. Date range (UPDATED COLUMN NAME)
        print("\n4. Date range of trips:")
        result = client.execute_sql(
            "SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) "
            "FROM taxi_data.rides"
        )
        if result:
            print(f"   From: {result[0][0]}")
            print(f"   To:   {result[0][1]}")

        # 5. Average trip distance
        print("\n5. Average trip distance:")
        result = client.execute_sql(
            "SELECT ROUND(AVG(trip_distance), 2) FROM taxi_data.rides"
        )
        print(f"   {result[0][0]} miles")

        # 6. Average fare amount (UPDATED COLUMN NAME)
        print("\n6. Average fare amount:")
        result = client.execute_sql(
            "SELECT ROUND(AVG(fare_amt), 2) FROM taxi_data.rides"
        )
        print(f"   ${result[0][0]}")

        # 7. Trip count by passenger count
        print("\n7. Trips by passenger count:")
        result = client.execute_sql(
            "SELECT passenger_count, COUNT(*) as trips "
            "FROM taxi_data.rides "
            "GROUP BY passenger_count "
            "ORDER BY trips DESC "
            "LIMIT 10"
        )
        for row in result:
            print(f"   {row[0]} passengers: {row[1]} trips")

        # 8. Payment type distribution
        print("\n8. Payment type distribution:")
        result = client.execute_sql(
            "SELECT payment_type, COUNT(*) as trips "
            "FROM taxi_data.rides "
            "GROUP BY payment_type "
            "ORDER BY trips DESC"
        )
        for row in result:
            print(f"   Type {row[0]}: {row[1]} trips")

        # 9. Busiest pickup hours (UPDATED COLUMN NAME)
        print("\n9. Top 5 busiest pickup hours:")
        result = client.execute_sql(
            "SELECT EXTRACT(HOUR FROM trip_pickup_date_time) as hour, "
            "COUNT(*) as trips "
            "FROM taxi_data.rides "
            "GROUP BY hour "
            "ORDER BY trips DESC "
            "LIMIT 5"
        )
        for row in result:
            print(f"   Hour {int(row[0]):02d}:00 - {row[1]} trips")

        # 10. Total revenue (UPDATED COLUMN NAME)
        print("\n10. Total revenue (total_amt):")
        result = client.execute_sql(
            "SELECT ROUND(SUM(total_amt), 2) FROM taxi_data.rides"
        )
        print(f"    ${result[0][0]}")

        print("\n" + "=" * 70)
        print("Done! You can also explore with:")
        print("  dlt pipeline taxi_pipeline show")
        print("=" * 70)


if __name__ == "__main__":
    run_queries()