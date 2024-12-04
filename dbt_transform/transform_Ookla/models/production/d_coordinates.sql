with source_stg as (
    SELECT * FROM {{ ref('stg_ookla') }}
),

d_coordinates as (
    SELECT
        source_stg.CoordinateID,
        source_stg.long1,
        source_stg.lat1,
        source_stg.long2,
        source_stg.lat2,
        source_stg.long3,
        source_stg.lat3,
        source_stg.long4,
        source_stg.lat4,
        source_stg.long5,
        source_stg.lat5,
        source_stg.SpeedID,
    FROM source_stg
)

SELECT * FROM d_coordinates