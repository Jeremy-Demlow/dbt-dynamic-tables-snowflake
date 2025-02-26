{% macro create_sample_data() %}
    {% set drop_query %}
        drop table if exists raw.sales.orders;
    {% endset %}
    {% do run_query(drop_query) %}
    {{ log("Dropped existing orders table", info=true) }}
    
    {% set create_table_query %}
        create table if not exists raw.sales.orders (
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
    
    {% set insert_query %}
        insert into raw.sales.orders
        select 
            uuid_string() as order_id,
            uuid_string() as customer_id,
            dateadd('day', -seq4(), current_date()) as order_date,
            case 
                when uniform(0, 100, random()) < 70 then 'COMPLETED'
                when uniform(0, 100, random()) < 85 then 'PROCESSING'
                when uniform(0, 100, random()) < 95 then 'RETURNED'
                else 'CANCELLED'
            end as order_status,
            round(uniform(10, 500, random()), 2) as order_total,
            uniform(1, 11, random()) as region_id
        from table(generator(rowcount => 1000));
    {% endset %}
    {% do run_query(insert_query) %}
    {{ log("Inserted 1000 orders with dates spanning the last 30 days", info=true) }}
    
    {{ log("Successfully generated test data", info=true) }}
{% endmacro %}

{% macro add_more_data() %}
    {% set insert_query %}
        insert into raw.sales.orders
        select 
            uuid_string() as order_id,
            uuid_string() as customer_id,
            current_date() as order_date,
            case 
                when uniform(0, 100, random()) < 70 then 'COMPLETED'
                when uniform(0, 100, random()) < 85 then 'PROCESSING'
                when uniform(0, 100, random()) < 95 then 'RETURNED'
                else 'CANCELLED'
            end as order_status,
            round(uniform(10, 500, random()), 2) as order_total,
            uniform(1, 11, random()) as region_id
        from table(generator(rowcount => 50));
    {% endset %}
    {% do run_query(insert_query) %}
    {{ log("Added 50 new orders for today", info=true) }}
{% endmacro %}