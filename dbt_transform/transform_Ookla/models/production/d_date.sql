with source_stg as (
    SELECT * FROM {{ ref('stg_ookla') }}
),

d_date as (
    SELECT
        source_stg.DateID,
        source_stg.Quarter,
        source_stg.Year,
        source_stg.SpeedID
    FROM source_stg
)

SELECT * FROM d_date