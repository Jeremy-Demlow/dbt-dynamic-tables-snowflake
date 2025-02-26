{% macro generate_marketing_data(days_back=365, events_per_day=100, database_override=none, schema_override=none, drop_existing=false) %}
    
    {% set target_database = database_override if database_override else 'MARKETING_DB' %}
    {% set target_schema = schema_override if schema_override else 'RAW' %}

    -- Create database first
    {% set create_db_query %}
        create database if not exists {{ target_database }};
    {% endset %}
    {% do run_query(create_db_query) %}
    {{ log("Created database " ~ target_database, info=true) }}
    
    -- Create schema next
    {% set create_schema_query %}
        create schema if not exists {{ target_database }}.{{ target_schema }};
    {% endset %}
    {% do run_query(create_schema_query) %}
    {{ log("Created schema " ~ target_database ~ "." ~ target_schema, info=true) }}
    
    {% if drop_existing %}
        {% set drop_query %}
            drop table if exists {{ target_database }}.{{ target_schema }}.SRC_MARKETING_EVENTS;
        {% endset %}
        {% do run_query(drop_query) %}
        {{ log("Dropped existing marketing events table", info=true) }}
    {% endif %}
    
    -- Create marketing events table
    {% set create_events_table_query %}
        create table if not exists {{ target_database }}.{{ target_schema }}.SRC_MARKETING_EVENTS (
            event_id varchar(36),
            customer_id varchar(36),
            event_date timestamp,
            campaign_id varchar(20),
            channel varchar(20),
            event_type varchar(20),
            revenue_impact float
        );
    {% endset %}
    {% do run_query(create_events_table_query) %}
    {{ log("Created marketing events table", info=true) }}
    
    -- Create campaigns reference table
    {% set create_campaigns_table_query %}
        create table if not exists {{ target_database }}.{{ target_schema }}.SRC_CAMPAIGNS (
            campaign_id varchar(20),
            campaign_name varchar(50),
            start_date date,
            end_date date,
            budget float,
            target_audience varchar(30)
        );
    {% endset %}
    {% do run_query(create_campaigns_table_query) %}
    {{ log("Created campaigns table", info=true) }}
    
    -- Insert campaign data
    {% set insert_campaigns_query %}
        truncate table if exists {{ target_database }}.{{ target_schema }}.SRC_CAMPAIGNS;
        
        insert into {{ target_database }}.{{ target_schema }}.SRC_CAMPAIGNS
        values
            ('CAMP001', 'Summer Sale', dateadd('day', -60, current_date()), dateadd('day', -30, current_date()), 50000, 'All Customers'),
            ('CAMP002', 'Fall Collection', dateadd('day', -45, current_date()), dateadd('day', -15, current_date()), 75000, 'Premium Customers'),
            ('CAMP003', 'Holiday Special', dateadd('day', -30, current_date()), dateadd('day', 15, current_date()), 100000, 'All Customers'),
            ('CAMP004', 'New Year Promo', dateadd('day', -15, current_date()), dateadd('day', 15, current_date()), 80000, 'Lapsed Customers'),
            ('CAMP005', 'Spring Preview', dateadd('day', 0, current_date()), dateadd('day', 30, current_date()), 60000, 'Premium Customers');
    {% endset %}
    {% do run_query(insert_campaigns_query) %}
    {{ log("Populated campaigns table", info=true) }}
    
    -- Check if sales customer IDs exist
    {% set customer_count_query %}
        select count(*) as customer_count from SALES_DB.RAW.SRC_ORDERS;
    {% endset %}
    
    {% set customer_count_result = run_query(customer_count_query) %}
    {% set customer_count = customer_count_result.columns['CUSTOMER_COUNT'][0] %}
    
    -- Generate marketing events data
    {% set channels = ["Email", "Social", "Search", "Display", "Direct"] %}
    {% set event_types = ["Impression", "Click", "Conversion", "Engagement", "Unsubscribe"] %}
    {% set campaigns = ["CAMP001", "CAMP002", "CAMP003", "CAMP004", "CAMP005"] %}
    
    {% for days_ago in range(days_back, 0, -1) %}
        {% if customer_count > 0 %}
            -- Use customer IDs from sales data if available
            {% set insert_query %}
                insert into {{ target_database }}.{{ target_schema }}.SRC_MARKETING_EVENTS 
                select 
                    uuid_string() as event_id,
                    s.customer_id,
                    dateadd('day', -{{ days_ago }}, current_date()) as event_date,
                    case 
                        when {{ days_ago }} > 45 then 'CAMP001'
                        when {{ days_ago }} > 30 then 'CAMP002'
                        when {{ days_ago }} > 15 then 'CAMP003'
                        else 'CAMP004'
                    end as campaign_id,
                    array_construct('Email', 'Social', 'Search', 'Display', 'Direct')[uniform(0, 4, random())] as channel,
                    array_construct('Impression', 'Click', 'Conversion', 'Engagement', 'Unsubscribe')[uniform(0, 4, random())] as event_type,
                    case
                        when array_construct('Impression', 'Click', 'Conversion', 'Engagement', 'Unsubscribe')[uniform(0, 4, random())] = 'Conversion' 
                        then round(uniform(10, 200, random()), 2)
                        else 0
                    end as revenue_impact
                from (
                    select customer_id from SALES_DB.RAW.SRC_ORDERS 
                    sample ({{ events_per_day }} rows)
                ) s;
            {% endset %}
        {% else %}
            -- Fallback to generating random IDs if no sales data exists
            {% set insert_query %}
                insert into {{ target_database }}.{{ target_schema }}.SRC_MARKETING_EVENTS 
                select 
                    uuid_string() as event_id,
                    uuid_string() as customer_id,
                    dateadd('day', -{{ days_ago }}, current_date()) as event_date,
                    case 
                        when {{ days_ago }} > 45 then 'CAMP001'
                        when {{ days_ago }} > 30 then 'CAMP002'
                        when {{ days_ago }} > 15 then 'CAMP003'
                        else 'CAMP004'
                    end as campaign_id,
                    array_construct('Email', 'Social', 'Search', 'Display', 'Direct')[uniform(0, 4, random())] as channel,
                    array_construct('Impression', 'Click', 'Conversion', 'Engagement', 'Unsubscribe')[uniform(0, 4, random())] as event_type,
                    case
                        when array_construct('Impression', 'Click', 'Conversion', 'Engagement', 'Unsubscribe')[uniform(0, 4, random())] = 'Conversion' 
                        then round(uniform(10, 200, random()), 2)
                        else 0
                    end as revenue_impact
                from table(generator(rowcount => {{ events_per_day }}));
            {% endset %}
        {% endif %}
        
        {% do run_query(insert_query) %}
        {{ log("Inserted " ~ events_per_day ~ " marketing events for " ~ days_ago ~ " days ago", info=true) }}
    {% endfor %}
    
    -- Create the stg_marketing_events view in SALES_DB.SALES_STAGE if it doesn't exist yet
    {% set create_stg_view_query %}
        create schema if not exists SALES_DB.SALES_STAGE;
        
        create or replace view SALES_DB.SALES_STAGE.stg_marketing_events as
        select
            customer_id,
            event_date,
            campaign_id,
            channel,
            event_type,
            revenue_impact
        from {{ target_database }}.{{ target_schema }}.SRC_MARKETING_EVENTS;
    {% endset %}
    {% do run_query(create_stg_view_query) %}
    {{ log("Created stg_marketing_events view in SALES_DB.SALES_STAGE", info=true) }}
    
    -- Create daily_customer_sales view if it doesn't exist
    {% set create_sales_view_query %}
        create schema if not exists SALES_DB.SALES_ANALYTICS;
        
        create or replace view SALES_DB.SALES_ANALYTICS.daily_customer_sales as
        select
            customer_id,
            order_date,
            order_total as total_sales,
            1 as order_count,
            order_status
        from SALES_DB.RAW.SRC_ORDERS;
    {% endset %}
    {% do run_query(create_sales_view_query) %}
    {{ log("Created daily_customer_sales view in SALES_DB.SALES_ANALYTICS", info=true) }}
    
    {{ log("Successfully generated marketing test data with matching customer IDs from sales data", info=true) }}
{% endmacro %}