with source as (
    select * from {{ source('marketing_data', 'SRC_MARKETING_EVENTS') }}
),

staged as (
    select
        event_id,
        customer_id,
        cast(event_date as date) as event_date,
        campaign_id,
        channel,
        event_type,
        revenue_impact,
        
        -- Adding metadata fields
        '{{ invocation_id }}' as dbt_job_id
    from source
)

select * from staged