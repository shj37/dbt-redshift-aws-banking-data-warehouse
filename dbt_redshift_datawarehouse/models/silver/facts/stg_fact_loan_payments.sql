{{
    config(
        materialized='incremental',
        alias='stg_fact_loan_payments',
        schema=var('silver_schema'),
        unique_key='payment_id',
        incremental_strategy='delete+insert',
        primary_key='payment_id',
        sort_key='payment_id',
        distribution='even'
    )
}}
 
SELECT
    payment_id,
    date_id,
    loan_id,
    customer_id,
    payment_amount,
    payment_status,
    getdate() as created_at
FROM
    {{var('bronze_schema')}}.external_fact_loan_payments
