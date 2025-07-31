# CDC dbt Logging Integration Setup Guide

This guide explains how to set up and use the CDC Snowflake logging integration with your dbt projects.

## Prerequisites

1. **Snowflake Logging Framework**: Ensure one of the CDC logging frameworks is installed:
   - Consolidated (recommended for new projects)
   - Hybrid (for AWS with hybrid tables)
   - Standard (for multi-cloud)

2. **Permissions**: Your dbt role needs:
   ```sql
   GRANT EXECUTE ON PROCEDURE dw_util.process_start_p TO ROLE <dbt_role>;
   GRANT EXECUTE ON PROCEDURE dw_util.process_stop_p TO ROLE <dbt_role>;
   GRANT EXECUTE ON PROCEDURE dw_util.write_error_log_p TO ROLE <dbt_role>;
   GRANT INSERT ON TABLE dw_util.process_instance TO ROLE <dbt_role>;
   GRANT INSERT ON TABLE dw_util.error_log TO ROLE <dbt_role>;
   ```

## Configuration

### 1. Add to packages.yml

```yaml
packages:
  - git: "https://github.com/CloudDataConsulting/cdc_dbt_utils.git"
    revision: claude-enhance  # or specific version tag
```

### 2. Configure in dbt_project.yml

```yaml
# Global logging configuration
vars:
  cdc_logging:
    enabled: true                    # Enable/disable all logging
    schema: 'dw_util'               # Schema where logging tables exist
    capture_row_counts: true        # Automatically count rows
    log_tests: false                # Log test results (can be verbose)
    parent_piid: 0                  # Parent process ID if part of larger workflow
    thread_number: 0                # Thread number for parallel processing

# Apply logging to all models (recommended approach)
models:
  +pre-hook: "{{ cdc_dbt_utils.process_start() }}"
  +post-hook: "{{ cdc_dbt_utils.process_stop() }}"
  
  # Or apply selectively
  my_project:
    staging:
      +pre-hook: "{{ cdc_dbt_utils.process_start() }}"
      +post-hook: "{{ cdc_dbt_utils.process_stop() }}"
```

### 3. Environment-Specific Configuration

```yaml
# Disable in dev, enable in prod
models:
  +pre-hook: 
    - "{% if target.name == 'prod' %}{{ cdc_dbt_utils.process_start() }}{% endif %}"
  +post-hook: 
    - "{% if target.name == 'prod' %}{{ cdc_dbt_utils.process_stop() }}{% endif %}"
```

## Usage Examples

### Basic Model with Automatic Logging

```sql
-- models/staging/stg_customers.sql
-- Logging happens automatically via pre/post hooks

{{ config(
    materialized='table'
) }}

select 
    customer_id,
    customer_name,
    created_date
from {{ source('erp', 'customers') }}
where is_active = true
```

### Model with Custom Parameters

```sql
-- models/marts/fct_daily_sales.sql
{{ config(
    pre_hook="{{ cdc_dbt_utils.process_start(
        application_name='sales_mart',
        rows_expected=1000000
    ) }}"
) }}

select 
    date_key,
    product_key,
    sum(sales_amount) as total_sales
from {{ ref('stg_sales') }}
group by date_key, product_key
```

### Model with Error Handling

```sql
-- models/staging/stg_orders.sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

-- Validate data quality inline
{% if is_incremental() %}
  {% set check_query %}
    select count(*) as dupe_count
    from (
      select order_id, count(*) 
      from {{ source('erp', 'orders') }}
      where order_date > (select max(order_date) from {{ this }})
      group by order_id
      having count(*) > 1
    )
  {% endset %}
  
  {% set results = run_query(check_query) %}
  {% if execute and results.rows[0][0] > 0 %}
    {{ cdc_dbt_utils.log_error(
        'Found ' ~ results.rows[0][0] ~ ' duplicate orders in incremental data',
        severity_level=4,
        error_category='data_quality'
    ) }}
  {% endif %}
{% endif %}

select * from {{ source('erp', 'orders') }}
{% if is_incremental() %}
  where order_date > (select max(order_date) from {{ this }})
{% endif %}
```

### Manual Logging Control

```sql
-- models/complex/complex_transformation.sql
-- For complex models where you need more control

{{
  config(
    pre_hook=[
      "{{ cdc_dbt_utils.process_start(application_name='complex_etl') }}",
      "CREATE TEMP TABLE metrics (step VARCHAR, row_count INTEGER)"
    ],
    post_hook=[
      "{{ cdc_dbt_utils.log_error('Metrics: ' || (SELECT LISTAGG(step || '=' || row_count, ', ') FROM metrics), severity_level=1) }}",
      "{{ cdc_dbt_utils.process_stop() }}"
    ]
  )
}}

WITH source_data AS (
  SELECT * FROM {{ ref('raw_data') }}
),

-- Log intermediate step
step1 AS (
  SELECT * FROM source_data WHERE status = 'active'
),
step1_logged AS (
  SELECT 
    (INSERT INTO metrics SELECT 'step1_active', COUNT(*) FROM step1) AS log_result,
    s.*
  FROM step1 s
)

SELECT * FROM step1_logged
```

## Run-Level Logging

### Option 1: Manual Operations

```bash
# Start logging for entire run
dbt run-operation start_run_logging --vars '{run_name: "daily_refresh"}'

# Run your dbt commands
dbt run --models tag:daily
dbt test --models tag:daily

# Stop logging
dbt run-operation stop_run_logging
```

### Option 2: Wrapper Script

```bash
#!/bin/bash
# run_with_logging.sh

# Start logging
RUN_ID=$(dbt run-operation start_run_logging --vars '{run_name: "'"$1"'"}' | grep "process ID:" | awk '{print $NF}')

# Run dbt
dbt run ${@:2}
RUN_STATUS=$?

# Stop logging with appropriate status
if [ $RUN_STATUS -eq 0 ]; then
  dbt run-operation stop_run_logging --vars "{run_id: $RUN_ID, status: 'completed'}"
else
  dbt run-operation stop_run_logging --vars "{run_id: $RUN_ID, status: 'failed'}"
fi

exit $RUN_STATUS
```

Usage: `./run_with_logging.sh "daily_refresh" --models tag:daily`

## Monitoring and Reporting

### View Recent Process Executions

```sql
-- See all recent dbt model runs
SELECT * 
FROM dw_util.process_report_v
WHERE application_name IN ('dbt', '{{ project_name }}')
  AND start_ts >= CURRENT_DATE - 7
ORDER BY start_ts DESC;
```

### View Errors

```sql
-- See recent errors
SELECT 
  error_ts,
  process_name,
  module_name,
  severity_level,
  error_message,
  reference_info
FROM dw_util.error_log
WHERE application_name = '{{ project_name }}'
  AND error_ts >= CURRENT_DATE - 7
ORDER BY error_ts DESC;
```

### Performance Analysis

```sql
-- Model performance over time
SELECT 
  process_name,
  DATE(start_ts) as run_date,
  AVG(duration_seconds) as avg_duration,
  AVG(rows_inserted) as avg_rows,
  AVG(inserted_rps) as avg_rows_per_second,
  COUNT(*) as execution_count,
  SUM(CASE WHEN final_status = 'failed' THEN 1 ELSE 0 END) as failure_count
FROM dw_util.process_report_v
WHERE application_name = '{{ project_name }}'
  AND start_ts >= CURRENT_DATE - 30
GROUP BY process_name, DATE(start_ts)
ORDER BY run_date DESC, process_name;
```

## Troubleshooting

### Logging Not Working

1. Check if logging is enabled:
   ```sql
   dbt debug --vars '{cdc_logging: {enabled: true}}'
   ```

2. Verify permissions:
   ```sql
   SHOW GRANTS ON PROCEDURE dw_util.process_start_p;
   ```

3. Test manually:
   ```sql
   CALL dw_util.process_start_p('test', 'test', 'test', NULL, NULL, 0, 0);
   ```

### Process IDs Not Matching

The integration uses temporary tables to pass process IDs between pre/post hooks. If you see warnings about missing process IDs:

1. Ensure both pre and post hooks are configured
2. Check that the model completes successfully
3. Verify the temp table exists during execution

### Performance Impact

Logging adds minimal overhead (typically <100ms per model). To reduce impact:

1. Disable row counting: `capture_row_counts: false`
2. Log only in production: Use environment conditionals
3. Skip test logging: `log_tests: false`

## Best Practices

1. **Always use in production** - Provides invaluable debugging information
2. **Configure at project level** - Ensures consistent logging
3. **Monitor the process_report_v** - Set up alerts for failures
4. **Archive old logs** - Implement retention policies on log tables
5. **Use meaningful names** - Override default names for important processes
6. **Log errors proactively** - Use `log_error` for data quality issues

## Advanced Usage

### Custom Process Hierarchies

```yaml
# For complex workflows with parent/child relationships
vars:
  cdc_logging:
    parent_piid: "{{ var('parent_process_id', 0) }}"
```

### Thread-Safe Parallel Execution

```yaml
# When running dbt with threads > 1
vars:
  cdc_logging:
    thread_number: "{{ thread_id }}"
```

### Integration with Orchestrators

```python
# Airflow example
def run_dbt_with_logging(model_selector, **context):
    # Start parent process
    start_result = subprocess.run(
        ["dbt", "run-operation", "start_run_logging", 
         "--vars", f"{{run_name: 'airflow_{context['ds']}'}}"],
        capture_output=True, text=True
    )
    
    # Extract process ID
    process_id = extract_process_id(start_result.stdout)
    
    # Run dbt models
    run_result = subprocess.run(
        ["dbt", "run", "--models", model_selector,
         "--vars", f"{{cdc_logging: {{parent_piid: {process_id}}}}}"],
        capture_output=True, text=True
    )
    
    # Stop logging
    status = 'completed' if run_result.returncode == 0 else 'failed'
    subprocess.run(
        ["dbt", "run-operation", "stop_run_logging",
         "--vars", f"{{run_id: {process_id}, status: '{status}'}}"]
    )
    
    return run_result.returncode
```