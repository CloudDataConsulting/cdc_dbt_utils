{% macro check_logging_results(test_run_id=none) %}
  {% set query %}
    -- Check process logging results
    WITH process_results AS (
        SELECT 
            COUNT(*) as total_processes,
            SUM(CASE WHEN final_status = 'completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN final_status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN final_status IS NULL THEN 1 ELSE 0 END) as running,
            AVG(duration_seconds) as avg_duration,
            SUM(rows_inserted) as total_rows
        FROM {{ var('cdc_logging.schema', 'dw_util') }}.process_report_v
        WHERE start_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
          AND application_name IN ('cdc_dbt_utils_integration_tests', 'integration_test_suite')
    ),
    error_results AS (
        SELECT 
            COUNT(*) as error_count,
            COUNT(DISTINCT process_name) as models_with_errors
        FROM {{ var('cdc_logging.schema', 'dw_util') }}.error_log
        WHERE error_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
          AND application_name = 'cdc_dbt_utils_integration_tests'
    )
    SELECT 
        p.total_processes,
        p.completed,
        p.failed,
        p.running,
        ROUND(p.avg_duration, 2) as avg_duration_seconds,
        p.total_rows,
        e.error_count,
        e.models_with_errors
    FROM process_results p
    CROSS JOIN error_results e
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {{ log("", info=true) }}
    {{ log("=== CDC Logging Integration Test Results ===", info=true) }}
    {% if results and results.rows %}
      {% set r = results.rows[0] %}
      {{ log("Total Processes: " ~ r[0], info=true) }}
      {{ log("Completed: " ~ r[1], info=true) }}
      {{ log("Failed: " ~ r[2], info=true) }}
      {{ log("Running: " ~ r[3], info=true) }}
      {{ log("Avg Duration: " ~ r[4] ~ " seconds", info=true) }}
      {{ log("Total Rows Processed: " ~ r[5], info=true) }}
      {{ log("Errors Logged: " ~ r[6], info=true) }}
      {{ log("Models with Errors: " ~ r[7], info=true) }}
    {% else %}
      {{ log("No results found - check that logging is enabled and tables exist", info=true) }}
    {% endif %}
    {{ log("==========================================", info=true) }}
  {% endif %}
  
{% endmacro %}