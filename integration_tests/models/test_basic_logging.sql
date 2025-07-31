-- Test basic logging functionality
-- This model tests the simplest use case: automatic logging via pre/post hooks

{{ config(
    tags=['logging_test', 'basic']
) }}

-- Simple select to test logging
select 
    1 as test_id,
    'basic_logging_test' as test_name,
    current_timestamp() as test_timestamp,
    '{{ var("test_run_id") }}' as test_run_id