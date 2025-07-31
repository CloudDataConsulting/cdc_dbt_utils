# dbt Integration with CDC Snowflake Logging Framework

## Overview

This document outlines how to integrate CDC's Snowflake process logging framework with dbt models, providing comprehensive tracking of dbt runs and individual model executions.

## Integration Approaches

### 1. Model-Level Logging with Pre/Post Hooks

**Implementation**: Use dbt's pre-hook and post-hook functionality to call logging procedures.

```yaml
# In dbt_project.yml or model config
models:
  my_project:
    +pre-hook: "{{ cdc_dbt_utils.process_start() }}"
    +post-hook: "{{ cdc_dbt_utils.process_stop() }}"
```

**Macro Implementation**:
```sql
{% macro process_start() %}
  {% if execute %}
    {% set process_id_query %}
      CALL dw_util.process_start_p(
        '{{ project_name }}',
        '{{ this.name }}',
        '{{ this.schema }}',
        '{{ var("run_id", "manual") }}',
        NULL,
        0,
        0
      )
    {% endset %}
    
    {% set results = run_query(process_id_query) %}
    {% if results %}
      {% set process_instance_id = results.rows[0][0] %}
      {{ log("Process started with ID: " ~ process_instance_id, info=true) }}
      
      -- Store in dbt context for post-hook
      {% do var().update({'process_instance_id': process_instance_id}) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro process_stop() %}
  {% if execute %}
    {% set process_instance_id = var().get('process_instance_id', 0) %}
    {% if process_instance_id > 0 %}
      {% set stop_query %}
        CALL dw_util.process_stop_p(
          {{ process_instance_id }},
          'completed',
          {{ adapter.get_relation(this).row_count or 'NULL' }},
          NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
      {% endset %}
      
      {% do run_query(stop_query) %}
      {{ log("Process stopped for ID: " ~ process_instance_id, info=true) }}
    {% endif %}
  {% endif %}
{% endmacro %}
```

### 2. Run-Level Logging with Operations

**Implementation**: Create dbt operations for manual logging control.

```bash
# Start logging for entire dbt run
dbt run-operation log_run_start --vars '{run_name: "daily_transform"}'

# Run your models
dbt run --models +my_model

# Stop logging
dbt run-operation log_run_stop --vars '{run_id: 12345}'
```

**Macro Implementation**:
```sql
{% macro log_run_start(run_name='dbt_run') %}
  {% set start_query %}
    CALL dw_util.process_start_p(
      'dbt',
      '{{ run_name }}',
      'orchestration',
      '{{ invocation_id }}',
      NULL, 0, 0
    )
  {% endset %}
  
  {% set results = run_query(start_query) %}
  {% if results %}
    {% set process_id = results.rows[0][0] %}
    {{ log("Started dbt run with process ID: " ~ process_id, info=true) }}
    {{ return(process_id) }}
  {% endif %}
{% endmacro %}
```

### 3. Automatic Model Instrumentation

**Implementation**: Create a wrapper macro that automatically adds logging to any model.

```sql
-- In your model
{{ cdc_dbt_utils.logged_model() }}

select 
  customer_id,
  sum(amount) as total_spent
from {{ ref('orders') }}
group by customer_id
```

**Macro Implementation**:
```sql
{% macro logged_model(model_sql=None) %}
  {% if execute %}
    -- Start logging
    {% set piid = cdc_dbt_utils.start_model_logging() %}
    
    -- Create temp view for row counting
    CREATE OR REPLACE TEMPORARY VIEW _dbt_metrics_{{ this.name }} AS (
      {% if model_sql %}
        {{ model_sql }}
      {% else %}
        {{ caller() }}
      {% endif %}
    );
    
    -- Get row count
    {% set count_query %}
      SELECT COUNT(*) FROM _dbt_metrics_{{ this.name }}
    {% endset %}
    {% set row_count = run_query(count_query).rows[0][0] %}
    
    -- Return the actual model SQL
    SELECT * FROM _dbt_metrics_{{ this.name }};
    
    -- Stop logging with metrics
    {% do cdc_dbt_utils.stop_model_logging(piid, row_count) %}
  {% else %}
    -- During parsing, just return the SQL
    {% if model_sql %}
      {{ model_sql }}
    {% else %}
      {{ caller() }}
    {% endif %}
  {% endif %}
{% endmacro %}
```

### 4. Integration with dbt Artifacts

**Implementation**: Parse dbt artifacts (manifest.json, run_results.json) to log comprehensive metrics.

```sql
{% macro log_dbt_run_results() %}
  {% if execute %}
    -- This would be called as a post-run-hook
    -- Parse run_results.json and log each model's performance
    {% set run_results = fromjson(source('dbt_artifacts', 'run_results')) %}
    
    {% for result in run_results.results %}
      CALL dw_util.process_start_insert_p(
        {{ result.unique_id }},
        'dbt',
        '{{ result.node.name }}',
        '{{ result.node.schema }}',
        '{{ result.node.config }}',
        {{ result.adapter_response.rows_affected or 'NULL' }},
        0,
        {{ result.thread_id }}
      );
    {% endfor %}
  {% endif %}
{% endmacro %}
```

## Recommended Approach

For CDC's use case, I recommend a **hybrid approach**:

1. **Default**: Use pre/post hooks for automatic model-level logging
2. **Optional**: Provide macros for manual instrumentation when needed
3. **Orchestration**: Create operations for run-level logging that can be called from your orchestration tool

### Implementation Priority

1. **Phase 1**: Basic pre/post hook logging
   - Simple process_start/stop calls
   - Capture model name, schema, and basic metadata

2. **Phase 2**: Enhanced metrics
   - Row count tracking
   - Execution time calculation
   - Error handling and logging

3. **Phase 3**: Advanced features
   - Parent/child process tracking for model dependencies
   - Integration with dbt artifacts
   - Custom metrics and parameters

## Configuration

Add to `dbt_project.yml`:

```yaml
# Enable logging for all models
models:
  +pre-hook: 
    - "{{ cdc_dbt_utils.ensure_logging_schema() }}"
    - "{{ cdc_dbt_utils.process_start() }}"
  +post-hook: 
    - "{{ cdc_dbt_utils.process_stop() }}"
  
  # Or enable selectively
  my_project:
    staging:
      +tags: ['logged']  # Use tags to control logging

# Variables for logging configuration
vars:
  cdc_logging:
    enabled: true
    schema: 'dw_util'
    capture_row_counts: true
    log_level: 'info'
```

## Benefits

1. **Automatic tracking** of all dbt model executions
2. **Performance monitoring** with execution times and row counts
3. **Audit trail** for compliance and debugging
4. **Integration** with existing CDC logging infrastructure
5. **Minimal overhead** - logging calls are fast
6. **Configurable** - can be enabled/disabled per environment

## Considerations

1. **Performance**: Pre/post hooks add minimal overhead
2. **Error handling**: Logging failures shouldn't break model runs
3. **Schema dependencies**: Ensure logging schema exists
4. **Permissions**: dbt role needs EXECUTE on procedures
5. **Testing**: Disable logging in unit tests

## Example Usage

```sql
-- models/staging/stg_customers.sql
{{ config(
    pre_hook="{{ cdc_dbt_utils.process_start(rows_expected=1000000) }}",
    post_hook="{{ cdc_dbt_utils.process_stop() }}"
) }}

select 
    customer_id,
    customer_name,
    created_date
from {{ source('erp', 'customers') }}
where is_active = true
```

This integration provides comprehensive logging while maintaining dbt's declarative approach and minimizing code changes.