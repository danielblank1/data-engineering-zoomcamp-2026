"""
Question 6: 1-hour tumbling window — total tip_amount per hour (all locations).
Find the hour with the highest total tip amount.

Submit with:
  docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q6_tumbling_tip_amount.py

Then query:
  SELECT window_start, total_tip_amount
  FROM green_trips_hourly_tips
  ORDER BY total_tip_amount DESC
  LIMIT 3;
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_source(t_env):
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            lpep_pickup_datetime  VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID          INT,
            DOLocationID          INT,
            passenger_count       DOUBLE,
            trip_distance         DOUBLE,
            tip_amount            DOUBLE,
            total_amount          DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id'          = 'q6-consumer-group',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)


def create_sink(t_env):
    t_env.execute_sql("""
        CREATE TABLE green_trips_hourly_tips (
            window_start     TIMESTAMP(3),
            total_tip_amount DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_hourly_tips',
            'username'   = 'postgres',
            'password'   = 'postgres',
            'driver'     = 'org.postgresql.Driver'
        )
    """)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(
        env, environment_settings=EnvironmentSettings.new_instance().in_streaming_mode().build()
    )

    create_source(t_env)
    create_sink(t_env)

    t_env.execute_sql("""
        INSERT INTO green_trips_hourly_tips
        SELECT
            window_start,
            SUM(tip_amount) AS total_tip_amount
        FROM TABLE(
            TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
        )
        GROUP BY window_start, window_end
    """).wait()


if __name__ == '__main__':
    run()
