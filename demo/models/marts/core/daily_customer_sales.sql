{{
    config(
        materialized='dynamic_table',
        target_lag='1 hour',
        snowflake_warehouse='dbt_wh_xs',
        refresh_mode='INCREMENTAL',
        initialize='ON_CREATE',
        on_configuration_change='apply'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

regions as (
    select * from {{ source('raw_data', 'SRC_REGIONS') }}
),

daily_sales as (
    select
        o.customer_id,
        o.order_date,
        r.region_name,
        r.country,
        o.order_status,  -- Added to select
        count(o.order_id) as order_count,
        sum(o.order_total) as total_sales,
        avg(o.order_total) as avg_order_value
    from orders o
    join regions r on o.region_id = r.region_id
    -- where o.order_status = 'COMPLETED'
    group by 1, 2, 3, 4, 5
)
select
    customer_id,
    order_date,
    region_name,
    country,
    order_status,
    order_count,
    total_sales,
    avg_order_value,
    extract(year from order_date) as year,
    extract(month from order_date) as month,
    extract(day from order_date) as day
from daily_sales