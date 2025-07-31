-- Test custom parameters and row counting
-- This model tests passing custom parameters to logging functions

{{ config(
    tags=['logging_test', 'custom_params'],
    pre_hook="{{ cdc_dbt_utils.process_start(
        application_name='integration_test_suite',
        process_name='custom_params_test',
        module_name='testing',
        rows_expected=1000
    ) }}"
) }}

-- Generate exactly 1000 rows to match expected
select 
    row_number() over (order by 1) as row_id,
    uniform(1, 100, random()) as random_value,
    dateadd('day', -uniform(1, 365, random()), current_date()) as random_date,
    case 
        when uniform(1, 10, random()) > 8 then 'premium'
        when uniform(1, 10, random()) > 5 then 'standard'
        else 'basic'
    end as tier,
    current_timestamp() as processed_at,
    '{{ var("test_run_id") }}' as test_run_id
from table(generator(rowcount => 1000))