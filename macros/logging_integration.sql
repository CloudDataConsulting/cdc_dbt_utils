/* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved
*
* You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
* or in any other way exploit any part of copyrighted material without permission.
* 
*/

{#
  Logging Integration Macros for CDC Snowflake Process Tracking
  
  These macros integrate dbt models with CDC's Snowflake logging framework,
  providing comprehensive tracking of model executions, performance metrics,
  and error handling.
#}

-- ========================================================================
-- Core Process Logging Macros
-- ========================================================================

{% macro process_start(application_name=none, process_name=none, module_name=none, rows_expected=none) %}
  {%- if var('cdc_logging.enabled', true) and execute -%}
    {%- set app_name = application_name or project_name -%}
    {%- set proc_name = process_name or this.name -%}
    {%- set mod_name = module_name or this.schema -%}
    
    {%- set start_query -%}
      CALL {{ var('cdc_logging.schema', 'dw_util') }}.process_start_p(
        '{{ app_name }}',
        '{{ proc_name }}',
        '{{ mod_name }}',
        '{{ invocation_id }}',  -- Use dbt's invocation_id as process parameters
        {{ rows_expected or 'NULL' }},
        {{ var('cdc_logging.parent_piid', 0) }},
        {{ var('cdc_logging.thread_number', 0) }}
      )
    {%- endset -%}
    
    {%- set results = run_query(start_query) -%}
    {%- if results and results.rows -%}
      {%- set process_instance_id = results.rows[0][0] -%}
      {{ log("CDC Logging: Started process " ~ proc_name ~ " with ID: " ~ process_instance_id, info=true) }}
      
      {#- Store in a way that persists between pre/post hooks -#}
      {%- do var().update({proc_name ~ '_piid': process_instance_id}) -%}
      
      -- Store process ID in temp table for post-hook retrieval
      CREATE TEMPORARY TABLE IF NOT EXISTS _dbt_process_tracking (
        model_name VARCHAR(200),
        process_instance_id INTEGER,
        start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
      );
      
      INSERT INTO _dbt_process_tracking (model_name, process_instance_id)
      VALUES ('{{ proc_name }}', {{ process_instance_id }});
      
    {%- else -%}
      {{ log("CDC Logging: Warning - process_start_p did not return a process ID", info=true) }}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}


{% macro process_stop(status='completed', rows_inserted=none, rows_updated=none, rows_deleted=none) %}
  {%- if var('cdc_logging.enabled', true) and execute -%}
    {%- set proc_name = this.name -%}
    
    -- Retrieve process ID from temp table
    {%- set get_piid_query -%}
      SELECT process_instance_id 
      FROM _dbt_process_tracking 
      WHERE model_name = '{{ proc_name }}'
      ORDER BY start_time DESC
      LIMIT 1
    {%- endset -%}
    
    {%- set piid_results = run_query(get_piid_query) -%}
    
    {%- if piid_results and piid_results.rows -%}
      {%- set process_instance_id = piid_results.rows[0][0] -%}
      
      -- Get row count if requested and not provided
      {%- set actual_rows_inserted = rows_inserted -%}
      {%- if actual_rows_inserted is none and var('cdc_logging.capture_row_counts', true) -%}
        {%- set count_query -%}
          SELECT COUNT(*) FROM {{ this }}
        {%- endset -%}
        {%- set count_results = run_query(count_query) -%}
        {%- if count_results and count_results.rows -%}
          {%- set actual_rows_inserted = count_results.rows[0][0] -%}
        {%- endif -%}
      {%- endif -%}
      
      -- Stop the process
      {%- set stop_query -%}
        CALL {{ var('cdc_logging.schema', 'dw_util') }}.process_stop_p(
          {{ process_instance_id }},
          '{{ status }}',
          {{ actual_rows_inserted or 'NULL' }},
          {{ rows_updated or 'NULL' }},
          {{ rows_deleted or 'NULL' }},
          NULL,  -- rows_errored
          NULL,  -- rows_processed
          NULL,  -- rows_ignored
          NULL,  -- error_count
          NULL,  -- ending_count
          NULL   -- rows_exported
        )
      {%- endset -%}
      
      {%- do run_query(stop_query) -%}
      {{ log("CDC Logging: Stopped process " ~ proc_name ~ " (ID: " ~ process_instance_id ~ ") with status: " ~ status, info=true) }}
      
      -- Clean up temp table entry
      {%- set cleanup_query -%}
        DELETE FROM _dbt_process_tracking WHERE model_name = '{{ proc_name }}'
      {%- endset -%}
      {%- do run_query(cleanup_query) -%}
      
    {%- else -%}
      {{ log("CDC Logging: Warning - No process ID found for " ~ proc_name, info=true) }}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}


-- ========================================================================
-- Error Handling Macros
-- ========================================================================

{% macro log_error(error_message, severity_level=5, error_code=none, error_category='dbt') %}
  {%- if var('cdc_logging.enabled', true) and execute -%}
    {%- set proc_name = this.name if this is defined else 'unknown' -%}
    
    -- Try to get current process instance ID
    {%- set piid = 0 -%}
    {%- set get_piid_query -%}
      SELECT process_instance_id 
      FROM _dbt_process_tracking 
      WHERE model_name = '{{ proc_name }}'
      ORDER BY start_time DESC
      LIMIT 1
    {%- endset -%}
    
    {%- set piid_results = run_query(get_piid_query) -%}
    {%- if piid_results and piid_results.rows -%}
      {%- set piid = piid_results.rows[0][0] -%}
    {%- endif -%}
    
    {%- set error_log_query -%}
      CALL {{ var('cdc_logging.schema', 'dw_util') }}.write_error_log_p(
        in_process_name => '{{ proc_name }}',
        in_module_name => '{{ this.schema if this is defined else "dbt" }}',
        in_process_instance_id => {{ piid }},
        in_severity_level => {{ severity_level }},
        in_error_code => {{ error_code or 'NULL' }},
        in_error_message => '{{ error_message | replace("'", "''") | truncate(3900) }}',
        in_reference_info => 'dbt model: {{ this }}',
        in_error_category => '{{ error_category }}',
        in_application_name => '{{ project_name }}'
      )
    {%- endset -%}
    
    {%- do run_query(error_log_query) -%}
    {{ log("CDC Logging: Error logged for " ~ proc_name ~ ": " ~ error_message, info=true) }}
  {%- endif -%}
{% endmacro %}


{% macro process_stop_with_error(error_message, error_code=none) %}
  {%- if var('cdc_logging.enabled', true) and execute -%}
    -- Log the error first
    {{ cdc_dbt_utils.log_error(error_message, severity_level=5, error_code=error_code) }}
    
    -- Then stop the process with failed status
    {{ cdc_dbt_utils.process_stop(status='failed') }}
  {%- endif -%}
{% endmacro %}


-- ========================================================================
-- Convenience Wrappers
-- ========================================================================

{% macro with_logging() %}
  {#
    Wrapper macro to add logging to a model without using hooks.
    Usage:
      {{ cdc_dbt_utils.with_logging() }}
      select * from {{ ref('source_table') }}
  #}
  
  {{ cdc_dbt_utils.process_start() }}
  
  -- Create a CTE with the model SQL
  WITH _logged_model AS (
    {{ caller() }}
  ),
  _row_count AS (
    SELECT COUNT(*) as row_count FROM _logged_model
  )
  
  -- Output the model results
  SELECT * FROM _logged_model;
  
  -- Log completion (this will be a separate statement)
  {{ cdc_dbt_utils.process_stop() }}
  
{% endmacro %}


-- ========================================================================
-- Run-Level Operations
-- ========================================================================

{% macro start_run_logging(run_name=none) %}
  {#
    Start logging for an entire dbt run.
    Usage: dbt run-operation start_run_logging --vars '{run_name: "daily_refresh"}'
  #}
  
  {%- set run_label = run_name or 'dbt_run_' ~ modules.datetime.datetime.now().strftime('%Y%m%d_%H%M%S') -%}
  
  {%- set start_query -%}
    CALL {{ var('cdc_logging.schema', 'dw_util') }}.process_start_p(
      'dbt_orchestration',
      '{{ run_label }}',
      'run_operation',
      '{{ invocation_id }}',
      NULL,
      0,
      0
    )
  {%- endset -%}
  
  {%- set results = run_query(start_query) -%}
  {%- if results and results.rows -%}
    {%- set run_piid = results.rows[0][0] -%}
    {{ log("CDC Logging: Started dbt run '" ~ run_label ~ "' with process ID: " ~ run_piid, info=true) }}
    {{ log("Store this ID for stop_run_logging: " ~ run_piid, info=true) }}
    
    -- Store in a persistent table for later retrieval
    CREATE TABLE IF NOT EXISTS {{ var('cdc_logging.schema', 'dw_util') }}._dbt_run_tracking (
      run_id VARCHAR(200),
      process_instance_id INTEGER,
      start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    
    INSERT INTO {{ var('cdc_logging.schema', 'dw_util') }}._dbt_run_tracking 
    VALUES ('{{ invocation_id }}', {{ run_piid }}, CURRENT_TIMESTAMP());
    
    {{ return(run_piid) }}
  {%- endif -%}
  
{% endmacro %}


{% macro stop_run_logging(run_id=none, status='completed') %}
  {#
    Stop logging for a dbt run.
    Usage: dbt run-operation stop_run_logging --vars '{run_id: 12345}'
  #}
  
  {%- if run_id -%}
    {%- set piid = run_id -%}
  {%- else -%}
    -- Try to get the most recent run
    {%- set get_piid_query -%}
      SELECT process_instance_id 
      FROM {{ var('cdc_logging.schema', 'dw_util') }}._dbt_run_tracking
      WHERE run_id = '{{ invocation_id }}'
      ORDER BY start_time DESC
      LIMIT 1
    {%- endset -%}
    
    {%- set results = run_query(get_piid_query) -%}
    {%- if results and results.rows -%}
      {%- set piid = results.rows[0][0] -%}
    {%- else -%}
      {{ log("CDC Logging: Error - No run process ID found", info=true) }}
      {{ return(none) }}
    {%- endif -%}
  {%- endif -%}
  
  {%- set stop_query -%}
    CALL {{ var('cdc_logging.schema', 'dw_util') }}.process_stop_p(
      {{ piid }},
      '{{ status }}',
      NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    )
  {%- endset -%}
  
  {%- do run_query(stop_query) -%}
  {{ log("CDC Logging: Stopped dbt run (process ID: " ~ piid ~ ") with status: " ~ status, info=true) }}
  
  -- Clean up tracking record
  {%- set cleanup_query -%}
    DELETE FROM {{ var('cdc_logging.schema', 'dw_util') }}._dbt_run_tracking
    WHERE process_instance_id = {{ piid }}
  {%- endset -%}
  {%- do run_query(cleanup_query) -%}
  
{% endmacro %}


-- ========================================================================
-- Test Logging
-- ========================================================================

{% macro log_test_result(test_name, test_status, failure_count=0, warn_count=0) %}
  {%- if var('cdc_logging.enabled', true) and var('cdc_logging.log_tests', false) and execute -%}
    
    {%- if test_status == 'fail' or failure_count > 0 -%}
      {%- set severity = 4 -%}
      {%- set error_msg = test_name ~ ' failed with ' ~ failure_count ~ ' failures' -%}
    {%- elif test_status == 'warn' or warn_count > 0 -%}
      {%- set severity = 2 -%}
      {%- set error_msg = test_name ~ ' warned with ' ~ warn_count ~ ' warnings' -%}
    {%- else -%}
      {%- set severity = 1 -%}
      {%- set error_msg = test_name ~ ' passed' -%}
    {%- endif -%}
    
    {{ cdc_dbt_utils.log_error(
        error_message=error_msg,
        severity_level=severity,
        error_category='data_quality'
    ) }}
    
  {%- endif -%}
{% endmacro %}