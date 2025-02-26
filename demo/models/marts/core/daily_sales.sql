{{ dynamic_table_config() }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

regions as (
    select * from {{ source('raw_data', 'SRC_REGIONS') }}
),

daily_sales as (
    select
        o.order_date,
        r.region_name,
        r.country,
        count(distinct o.order_id) as order_count,
        count(distinct o.customer_id) as customer_count,
        sum(o.order_total) as total_sales,
        avg(o.order_total) as avg_order_value
    from orders o
    join regions r on o.region_id = r.region_id
    where o.order_status = 'COMPLETED'
    group by 1, 2, 3
)

select
    order_date,
    region_name,
    country,
    order_count,
    customer_count,
    total_sales,
    avg_order_value,
    -- Adding time dimension columns for easier querying
    extract(year from order_date) as year,
    extract(month from order_date) as month,
    extract(day from order_date) as day
from daily_sales