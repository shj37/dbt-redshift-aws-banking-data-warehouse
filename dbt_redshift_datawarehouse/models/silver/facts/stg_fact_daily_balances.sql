{{
    config(
        materialized='incremental',
        alias='stg_fact_daily_balances',
        schema=var('silver_schema'),
        unique_key='balance_id',
        incremental_strategy='delete+insert',
        primary_key='balance_id',
        sort_key='balance_id',
        distribution='even'
    )
}}
 
SELECT
    balance_id,
    date_id,
    account_id,
    opening_balance,
    closing_balance,
    average_balance,
    getdate() as created_at
FROM
    {{var('bronze_schema')}}.external_fact_daily_balances
