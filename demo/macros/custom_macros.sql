{% macro get_dynamic_table_info() %}
    {% set query %}
        show dynamic tables in schema {{ target.database }}.{{ target.schema }};
    {% endset %}
    {% set results = run_query(query) %}
    {{ return(results) }}
{% endmacro %}

{% macro setup_raw_schema() %}
    {% set create_db_query %}
        create database if not exists raw;
    {% endset %}
    {% do run_query(create_db_query) %}
    
    {% set create_schema_query %}
        create schema if not exists raw.sales;
    {% endset %}
    {% do run_query(create_schema_query) %}
    
    {{ log("Created raw.sales schema", info=true) }}
{% endmacro %}

{% macro refresh_dynamic_table(model_name) %}
    {% set dynamic_table_relation = ref(model_name) %}
    {% set refresh_query %}
        alter dynamic table {{ dynamic_table_relation }} refresh;
    {% endset %}
    {% do run_query(refresh_query) %}
    {{ log("Dynamic table " ~ model_name ~ " refreshed", info=True) }}
{% endmacro %}

{% macro get_dynamic_table_refresh_history(model_name) %}
    {% set dynamic_table_relation = ref(model_name) %}
    {% set query %}
        select * 
        from table(information_schema.dynamic_table_refresh_history(
            table_name => '{{ dynamic_table_relation.identifier }}'
        ))
        order by refresh_start_time desc
        limit 10;
    {% endset %}
    {% set results = run_query(query) %}
    {{ return(results) }}
{% endmacro %}