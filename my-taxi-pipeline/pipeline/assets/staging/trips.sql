/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: trip_id
    type: varchar
    description: "Unique trip identifier"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: total_amount
    type: double
    description: "Total fare amount"
    checks:
      - name: non_negative

custom_checks:
  - name: no_duplicate_trips
    description: "Ensure no duplicate trip IDs in the time window"
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT trip_id)
      FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 0

@bruin */

SELECT
    md5(
        CONCAT(
            COALESCE(CAST(COALESCE(t.tpep_pickup_datetime, t.lpep_pickup_datetime) AS VARCHAR), ''),
            COALESCE(CAST(COALESCE(t.tpep_dropoff_datetime, t.lpep_dropoff_datetime) AS VARCHAR), ''),
            COALESCE(CAST(t.pu_location_id AS VARCHAR), ''),
            COALESCE(CAST(t.do_location_id AS VARCHAR), ''),
            COALESCE(CAST(t.fare_amount AS VARCHAR), ''),
            COALESCE(CAST(t.trip_distance AS VARCHAR), ''),
            COALESCE(t.taxi_type, '')
        )
    ) AS trip_id,
    COALESCE(t.tpep_pickup_datetime, t.lpep_pickup_datetime) AS pickup_datetime,
    COALESCE(t.tpep_dropoff_datetime, t.lpep_dropoff_datetime) AS dropoff_datetime,
    t.pu_location_id AS pickup_location_id,
    t.do_location_id AS dropoff_location_id,
    CAST(t.payment_type AS INTEGER) AS payment_type,
    t.fare_amount,
    t.total_amount,
    t.trip_distance,
    t.taxi_type,
    p.payment_type_name
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
    ON CAST(t.payment_type AS INTEGER) = p.payment_type_id
WHERE COALESCE(t.tpep_pickup_datetime, t.lpep_pickup_datetime) >= '{{ start_datetime }}'
  AND COALESCE(t.tpep_pickup_datetime, t.lpep_pickup_datetime) < '{{ end_datetime }}'
  AND t.total_amount >= 0