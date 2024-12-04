with source_stg as (
    SELECT * FROM {{ ref('stg_ookla') }}
),

f_speed as (
    SELECT
        source_stg.SpeedID,
        source_stg.quadkey,
        source_stg.avg_d_kbps,
        source_stg.avg_u_kbps,
        source_stg.avg_d_mbps,
        source_stg.avg_u_mbps,
        source_stg.avg_lat_ms,
        source_stg.tests,
        source_stg.devices
    FROM source_stg
)

SELECT * FROM f_speed