{{ dynamic_table_config(
    target_lag='1 hour',
    snowflake_warehouse='dbt_wh_xs',
    refresh_mode='AUTO'
) }}

with sales as (
    select
        customer_id,
        order_date,
        sum(total_sales) as total_sales,
        sum(order_count) as order_count
    from {{ ref('daily_customer_sales') }}
    where order_status = 'COMPLETED'
    and order_date >= dateadd('day', -365, current_date())
    group by 1, 2
),

marketing as (
    select
        customer_id,
        event_date,
        campaign_id,
        channel,
        sum(case when event_type = 'Conversion' then 1 else 0 end) as conversions,
        sum(revenue_impact) as marketing_revenue
    from {{ ref('stg_marketing_events') }}
    where event_date >= dateadd('day', -365, current_date())
    group by 1, 2, 3, 4
),

campaign_info as (
    select
        campaign_id,
        campaign_name,
        start_date,
        end_date,
        budget,
        target_audience
    from {{ source('marketing_data', 'SRC_CAMPAIGNS') }}
),

-- Create daily customer activity
daily_customer_activity as (
    select
        coalesce(s.customer_id, m.customer_id) as customer_id,
        coalesce(s.order_date, m.event_date) as activity_date,
        s.total_sales,
        s.order_count,
        m.campaign_id,
        c.campaign_name,
        m.channel,
        m.conversions,
        m.marketing_revenue
    from sales s
    full outer join marketing m
        on s.customer_id = m.customer_id
        and s.order_date = m.event_date
    left join campaign_info c
        on m.campaign_id = c.campaign_id
)

select
    customer_id,
    activity_date,
    total_sales,
    order_count,
    campaign_id,
    campaign_name,
    channel,
    conversions,
    marketing_revenue,
    -- Classification fields
    case
        when total_sales > 0 and marketing_revenue > 0 then 'Marketing-Influenced Sale'
        when total_sales > 0 then 'Organic Sale'
        when marketing_revenue > 0 then 'Marketing Activity'
        else 'Other Activity'
    end as activity_type,
    -- Time dimensions
    extract(year from activity_date) as year,
    extract(month from activity_date) as month,
    extract(day from activity_date) as day
from daily_customer_activity