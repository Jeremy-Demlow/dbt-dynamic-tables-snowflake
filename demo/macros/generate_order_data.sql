{% macro generate_order_data(days_back=30, orders_per_day=100, database_override=none, schema_override=none, drop_existing=false) %}
    
    {% set target_database = database_override if database_override else 'SALES_DB' %}
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
            drop table if exists {{ target_database }}.{{ target_schema }}.SRC_ORDERS;
        {% endset %}
        {% do run_query(drop_query) %}
        {{ log("Dropped existing orders table", info=true) }}
    {% endif %}
    
    {% set create_table_query %}
        create table if not exists {{ target_database }}.{{ target_schema }}.SRC_ORDERS (
            order_id varchar(36),
            customer_id varchar(36),
            order_date timestamp,
            order_status varchar(20),
            order_total float,
            region_id int
        );
    {% endset %}
    {% do run_query(create_table_query) %}
    {{ log("Created orders table", info=true) }}
    
    {% set statuses = ["COMPLETED", "RETURNED", "PROCESSING", "CANCELLED"] %}
    
    {% for days_ago in range(days_back, 0, -1) %}
        {% set insert_query %}
            insert into {{ target_database }}.{{ target_schema }}.SRC_ORDERS 
            select 
                uuid_string() as order_id,
                uuid_string() as customer_id,
                dateadd('day', -{{ days_ago }}, current_date()) as order_date,
                case 
                    when uniform(0, 100, random()) < 70 then 'COMPLETED'
                    when uniform(0, 100, random()) < 85 then 'PROCESSING'
                    when uniform(0, 100, random()) < 95 then 'RETURNED'
                    else 'CANCELLED'
                end as order_status,
                round(uniform(10, 500, random()), 2) as order_total,
                uniform(1, 11, random()) as region_id
            from table(generator(rowcount => {{ orders_per_day }}));
        {% endset %}
        {% do run_query(insert_query) %}
        {{ log("Inserted " ~ orders_per_day ~ " orders for " ~ days_ago ~ " days ago", info=true) }}
    {% endfor %}
    
    -- Also create regions reference table
    {% set create_regions_query %}
        create table if not exists {{ target_database }}.{{ target_schema }}.SRC_REGIONS (
            region_id int,
            region_name varchar(50),
            country varchar(50)
        );
        
        -- Clear existing data if table exists
        truncate table if exists {{ target_database }}.{{ target_schema }}.SRC_REGIONS;
        
        -- Insert region data
        insert into {{ target_database }}.{{ target_schema }}.SRC_REGIONS (region_id, region_name, country)
        values
            (1, 'Northeast', 'USA'),
            (2, 'Southeast', 'USA'),
            (3, 'Midwest', 'USA'),
            (4, 'Southwest', 'USA'),
            (5, 'West', 'USA'),
            (6, 'Northwest', 'USA'),
            (7, 'East', 'Canada'),
            (8, 'West', 'Canada'),
            (9, 'Central', 'Canada'),
            (10, 'North', 'Mexico'),
            (11, 'South', 'Mexico');
    {% endset %}
    {% do run_query(create_regions_query) %}
    {{ log("Created and populated regions table", info=true) }}
    
    {{ log("Successfully generated " ~ (days_back * orders_per_day) ~ " test orders", info=true) }}
{% endmacro %}

{% macro add_incremental_test_data(days=1, orders_per_day=100, database_override=none, schema_override=none) %}
    {% set target_database = database_override if database_override else 'SALES_DB' %}
    {% set target_schema = schema_override if schema_override else 'RAW' %}
    
    {% set max_date_query %}
        select coalesce(max(order_date), current_date() - 30) as max_date 
        from {{ target_database }}.{{ target_schema }}.SRC_ORDERS
    {% endset %}
    
    {% set max_date_results = run_query(max_date_query) %}
    {% set max_date = max_date_results.columns['MAX_DATE'][0] %}
    
    {% for day in range(1, days + 1) %}
        {% set insert_query %}
            insert into {{ target_database }}.{{ target_schema }}.SRC_ORDERS 
            select 
                uuid_string() as order_id,
                uuid_string() as customer_id,
                dateadd('day', {{ day }}, '{{ max_date }}') as order_date,
                case 
                    when uniform(0, 100, random()) < 70 then 'COMPLETED'
                    when uniform(0, 100, random()) < 85 then 'PROCESSING'
                    when uniform(0, 100, random()) < 95 then 'RETURNED'
                    else 'CANCELLED'
                end as order_status,
                round(uniform(10, 500, random()), 2) as order_total,
                uniform(1, 11, random()) as region_id
            from table(generator(rowcount => {{ orders_per_day }}));
        {% endset %}
        {% do run_query(insert_query) %}
        {{ log("Added " ~ orders_per_day ~ " orders for day " ~ day ~ " after previous max date", info=true) }}
    {% endfor %}
    
    {{ log("Successfully added " ~ (days * orders_per_day) ~ " incremental test orders", info=true) }}
{% endmacro %}

{% macro validate_dynamic_table_refresh(model_name) %}
    -- Get the relation for the model
    {% set relation = ref(model_name) %}
    
    -- Query to get refresh history 
    {% set refresh_history_query %}
        select * from table(information_schema.dynamic_table_refresh_history(
            table_name => '{{ relation.identifier }}'
        ))
        order by refresh_start_time desc
        limit 5;
    {% endset %}
    
    -- Run the query and get the refresh history
    {% set refresh_history = run_query(refresh_history_query) %}
    
    -- Output the refresh history
    {{ log("Recent refresh history for " ~ model_name ~ ":", info=true) }}
    {{ log(refresh_history, info=true) }}
    
    -- Query to get refresh stats from the dynamic table
    {% set refresh_stats_query %}
        select 
            last_refresh_timestamp, 
            count(*) as total_rows,
            max(order_date) as latest_date,
            min(order_date) as earliest_date
        from {{ relation }}
        group by 1;
    {% endset %}
    
    -- Run the query and get the refresh stats
    {% set refresh_stats = run_query(refresh_stats_query) %}
    
    -- Output the refresh stats
    {{ log("Refresh stats for " ~ model_name ~ ":", info=true) }}
    {{ log(refresh_stats, info=true) }}
    
    {{ return(refresh_history) }}
{% endmacro %}