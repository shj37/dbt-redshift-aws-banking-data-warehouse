{{
    config(
        materialized='incremental',
        alias='stg_dim_transaction_type',
        schema=var('silver_schema'),
        unique_key='transaction_type_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    transaction_type_id,
    description,
    getdate() as created_at
FROM
    {{var('bronze_schema')}}.external_transaction_types
