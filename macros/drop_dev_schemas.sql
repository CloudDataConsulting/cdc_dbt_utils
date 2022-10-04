{% macro drop_dev_schemas(username) %}
{% set sql %}
    drop schema if exists {{username}}_dw_util; 
    drop schema if exists {{username}}_alumni;
    drop schema if exists {{username}}_stage;
    drop schema if exists {{username}}_seed_data; 
    drop schema if exists {{username}}_example; 
    drop schema if exists {{username}}; 
{% endset %}

{% do run_query(sql) %}
{% do log( " Schemas dropped", info=True) %}
{% endmacro %}
