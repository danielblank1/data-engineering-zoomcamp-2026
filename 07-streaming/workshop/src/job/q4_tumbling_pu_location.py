"""
Question 4: 5-minute tumbling window — count trips per PULocationID.
Results written to PostgreSQL table `green_trips_5min`.

Submit with:
  docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_pu_location.py

Then query:
  SELECT PULocationID, num_trips
  FROM green_trips_5min
  ORDER BY num_trips DESC
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
            'properties.group.id'          = 'q4-consumer-group',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)


def create_sink(t_env):
    t_env.execute_sql("""
        CREATE TABLE green_trips_5min (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips    BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_5min',
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
        INSERT INTO green_trips_5min
        SELECT
            window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID
    """).wait()


if __name__ == '__main__':
    run()
