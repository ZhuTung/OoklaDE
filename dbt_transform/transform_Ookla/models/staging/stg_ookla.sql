with source_stg as (
    SELECT * FROM Staging.stg
),

stg_ookla as (
    SELECT * FROM source_stg
)

SELECT * FROM stg_ookla