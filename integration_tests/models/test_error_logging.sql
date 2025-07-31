-- Test error logging functionality
-- This model demonstrates error handling and logging

{{ config(
    tags=['logging_test', 'error_handling'],
    post_hook=[
        -- Simulate a data quality check that logs an error
        """
        {% set check_result = 5 %}
        {% if check_result > 3 %}
            {{ cdc_dbt_utils.log_error(
                'Test data quality issue: Found ' ~ check_result ~ ' problematic records',
                severity_level=3,
                error_category='test_data_quality'
            ) }}
        {% endif %}
        """,
        "{{ cdc_dbt_utils.process_stop() }}"
    ]
) }}

with test_data as (
    select 
        row_number() over (order by 1) as id,
        case 
            when row_number() over (order by 1) <= 5 then 'ERROR'
            else 'OK'
        end as status,
        current_timestamp() as created_at
    from table(generator(rowcount => 10))
)

select 
    id,
    status,
    created_at,
    '{{ var("test_run_id") }}' as test_run_id
from test_data