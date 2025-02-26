{% macro dynamic_table_config(
    target_lag='1 hour',
    snowflake_warehouse='dbt_wh_xs',
    refresh_mode='INCREMENTAL',
    initialize='ON_CREATE',
    on_configuration_change='apply'
) %}
    {{ config(
        materialized='dynamic_table',
        target_lag=target_lag,
        snowflake_warehouse=snowflake_warehouse,
        refresh_mode=refresh_mode,
        initialize=initialize,
        on_configuration_change=on_configuration_change
    ) }}
{% endmacro %}