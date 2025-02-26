with source as (
    select * from {{ source('raw_data', 'SRC_ORDERS') }}
),

staged as (
    select
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        upper(order_status) as order_status,
        order_total,
        region_id,
        
        -- Adding metadata fields
        -- current_timestamp() as dbt_updated_at,
        '{{ invocation_id }}' as dbt_job_id
    from source
)

select * from staged