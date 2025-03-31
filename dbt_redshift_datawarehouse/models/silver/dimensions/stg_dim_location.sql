{{
    config(
        materialized='incremental',
        alias='stg_dim_location',
        schema=var('silver_schema'),
        unique_key='location_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    location_id,
    address,
    city,
    state,
    country,
    getdate() as created_at
FROM
    {{var('bronze_schema')}}.external_locations