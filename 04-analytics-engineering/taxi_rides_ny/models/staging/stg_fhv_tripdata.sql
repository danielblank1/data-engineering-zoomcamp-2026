select
    -- identifiers
    dispatching_base_num,
    cast("PUlocationID" as integer) as pickup_location_id,
    cast("DOlocationID" as integer) as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast("dropOff_datetime" as timestamp) as dropoff_datetime,

    -- trip info
    "SR_Flag" as sr_flag,
    "Affiliated_base_number" as affiliated_base_number

from {{ source('raw', 'fhv_tripdata') }}

where dispatching_base_num is not null

{% if target.name == 'dev' %}
limit {{ var('record_limit', 10000) }}
{% endif %}