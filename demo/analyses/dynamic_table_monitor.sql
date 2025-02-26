-- This analysis creates a simple monitoring dashboard for dynamic table refreshes

with refresh_history as (
    select *
    from table(information_schema.dynamic_table_refresh_history(
        table_name => '{{ ref("daily_sales").identifier }}'
    ))
    order by refresh_start_time desc
),

refresh_stats as (
    select 
        refresh_type,
        status,
        count(*) as refresh_count,
        avg(total_rows_added) as avg_rows_added,
        avg(total_rows_updated) as avg_rows_updated,
        avg(total_rows_deleted) as avg_rows_deleted,
        avg(total_seconds_taken) as avg_duration_seconds
    from refresh_history
    group by 1, 2
),

latest_refresh as (
    select 
        refresh_start_time,
        refresh_type,
        status,
        total_rows_added,
        total_rows_updated,
        total_rows_deleted,
        total_seconds_taken
    from refresh_history
    order by refresh_start_time desc
    limit 1
),

data_lag as (
    select 
        max(order_date) as latest_data_date,
        datediff('hour', max(order_date), current_timestamp()) as hours_lag,
        max(last_refresh_timestamp) as last_refresh_timestamp
    from {{ ref('daily_sales') }}
)

select 
    'Latest Refresh' as section,
    'Time' as metric,
    to_varchar(lr.refresh_start_time) as value
from latest_refresh lr

union all
select 
    'Latest Refresh',
    'Type',
    lr.refresh_type
from latest_refresh lr

union all
select 
    'Latest Refresh',
    'Status',
    lr.status
from latest_refresh lr

union all
select 
    'Latest Refresh',
    'Duration (seconds)',
    to_varchar(lr.total_seconds_taken)
from latest_refresh lr

union all
select 
    'Latest Refresh',
    'Rows Added',
    to_varchar(lr.total_rows_added)
from latest_refresh lr

union all
select 
    'Data Freshness',
    'Latest Data Date',
    to_varchar(dl.latest_data_date)
from data_lag dl

union all
select 
    'Data Freshness',
    'Hours Behind',
    to_varchar(dl.hours_lag)
from data_lag dl

union all
select 
    'Data Freshness',
    'Last Refresh Timestamp',
    to_varchar(dl.last_refresh_timestamp)
from data_lag dl

union all
select 
    'Refresh Statistics',
    rs.refresh_type || ' - ' || rs.status,
    to_varchar(rs.refresh_count) || ' refreshes, avg ' || 
    to_varchar(round(rs.avg_duration_seconds, 1)) || ' seconds, avg ' ||
    to_varchar(round(rs.avg_rows_added, 0)) || ' rows added'
from refresh_stats rs

order by section, metric