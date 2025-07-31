-- Test model that intentionally fails to test error logging
-- This model will fail if var 'force_test_failure' is true

{{ config(
    tags=['logging_test', 'failure_test'],
    enabled=var('test_failures', false)  -- Disabled by default
) }}

select 
    1 as id,
    'This model tests failure logging' as description,
    current_timestamp() as failed_at,
    
    -- This will cause a SQL error if force_test_failure is true
    {% if var('force_test_failure', false) %}
        1 / 0 as forced_error,
    {% endif %}
    
    '{{ var("test_run_id") }}' as test_run_id