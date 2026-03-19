"""
Question 5: Session window (5-minute gap) per PULocationID.
Find the PULocationID with the longest session (most trips in a single session).

Submit with:
  docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q5_session_pu_location.py

Then query:
  SELECT PULocationID, num_trips
  FROM green_trips_session
  ORDER BY num_trips DESC
  LIMIT 5;
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
            'properties.group.id'          = 'q5-consumer-group',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)


def create_sink(t_env):
    t_env.execute_sql("""
        CREATE TABLE green_trips_session (
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            PULocationID INT,
            num_trips    BIGINT,
            PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_session',
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
        INSERT INTO green_trips_session
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(
                TABLE green_trips PARTITION BY PULocationID,
                DESCRIPTOR(event_timestamp),
                INTERVAL '5' MINUTE
            )
        )
        GROUP BY window_start, window_end, PULocationID
    """).wait()


if __name__ == '__main__':
    run()
