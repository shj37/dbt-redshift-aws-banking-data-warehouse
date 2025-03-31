{{
    config(
        materialized='incremental',
        alias='stg_dim_loan',
        schema=var('silver_schema'),
        unique_key='loan_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    loan_id,
    loan_type,
    loan_amount,
    interest_rate,
    getdate() as created_at
FROM
    {{var('bronze_schema')}}.external_loans