{{
    config(
        materialized='incremental',
        alias='stg_fact_investments',
        schema=var('silver_schema'),
        unique_key='investment_id',
        incremental_strategy='delete+insert',
        primary_key='investment_id',
        sort_key='investment_id',
        distribution='even'
    )
}}
 
SELECT
    investment_id,
    date_id,
    investment_type_id,
    account_id,
    amount_invested,
    investment_return,
    getdate() as created_at
FROM
    {{var('bronze_schema')}}.external_fact_investments
