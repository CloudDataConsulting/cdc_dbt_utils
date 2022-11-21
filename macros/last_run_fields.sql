{% macro last_run_fields() -%}
    ,current_user::varchar(50) as dw_created_by
    ,current_timestamp dw_created_ts
    ,current_user::varchar(50) as dw_modified_by
    ,current_timestamp dw_modified_ts
{%- endmacro %}