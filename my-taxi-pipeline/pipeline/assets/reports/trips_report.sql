/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: payment_type_name
    type: varchar
    description: "Payment method name"
    primary_key: true
  - name: pickup_date
    type: DATE
    description: "Date of pickup"
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: "Number of trips"
    checks:
      - name: non_negative

@bruin */

SELECT
    payment_type_name,
    CAST(pickup_datetime AS DATE) AS pickup_date,
    COUNT(*) AS trip_count
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY payment_type_name, CAST(pickup_datetime AS DATE)