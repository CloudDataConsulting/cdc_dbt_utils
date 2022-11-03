/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - CONFIDENTIAL
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{% macro drop_dev_schemas(username) %}
{% set sql %}
    drop schema if exists {{username}}_dw_util; 
    drop schema if exists {{username}}_stage;
    drop schema if exists {{username}}_seed_data;
    drop schema if exists {{username}}_example;
    drop schema if exists {{username}};
{% endset %}

{% do run_query(sql) %}
{% do log( " Schemas dropped", info=True) %}
{% endmacro %}
