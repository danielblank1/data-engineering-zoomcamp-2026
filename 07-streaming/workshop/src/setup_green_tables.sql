-- Run BEFORE submitting any Flink jobs.
-- Usage: docker exec -i workshop-postgres-1 psql -U postgres < src/setup_green_tables.sql

DROP TABLE IF EXISTS green_trips_5min;
DROP TABLE IF EXISTS green_trips_session;
DROP TABLE IF EXISTS green_trips_hourly_tips;

-- Question 4: 5-minute tumbling window by pickup location
CREATE TABLE green_trips_5min (
    window_start TIMESTAMP,
    pulocationid INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, pulocationid)
);

-- Question 5: Session window (5-minute gap) by pickup location
CREATE TABLE green_trips_session (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    pulocationid INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, pulocationid)
);

-- Question 6: 1-hour tumbling window, total tips
CREATE TABLE green_trips_hourly_tips (
    window_start TIMESTAMP PRIMARY KEY,
    total_tip_amount DOUBLE PRECISION
);