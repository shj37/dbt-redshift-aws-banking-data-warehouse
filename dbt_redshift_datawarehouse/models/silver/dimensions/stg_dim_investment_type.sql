{{
    config(
        materialized='incremental',
        alias='stg_dim_investment_type',
        schema=var('silver_schema'),
        unique_key='investment_type_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    investment_type_id,
    investment_type_name,
    getdate() as created_at
FROM
    {{var('bronze_schema')}}.external_investment_types