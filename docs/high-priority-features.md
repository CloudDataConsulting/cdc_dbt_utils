# High Priority Features - Detailed Specifications

This document provides detailed specifications for the high-priority features that are NOT already available in dbt-utils and would add real value to the cdc_dbt_utils package.

## 1. limit_in_dev Macro

### Problem It Solves
When developing dbt models against large production datasets, developers often:
- Wait long times for models to build during testing
- Consume unnecessary compute resources
- Risk running expensive queries by accident
- Have to manually add/remove LIMIT clauses

### How It Works
```sql
-- In your model
select 
    customer_id,
    order_date,
    total_amount
from {{ ref('large_orders_table') }}
where order_date >= '2020-01-01'
{{ cdc_dbt_utils.limit_in_dev(10000) }}
```

**In development** (target.name = 'dev'):
```sql
select customer_id, order_date, total_amount
from large_orders_table
where order_date >= '2020-01-01'
limit 10000
```

**In production** (target.name = 'prod'):
```sql
select customer_id, order_date, total_amount
from large_orders_table
where order_date >= '2020-01-01'
-- no limit applied
```

### Implementation Details
```sql
{% macro limit_in_dev(row_limit=10000) %}
  {%- if target.name in ['dev', 'development', 'sandbox'] -%}
    limit {{ row_limit }}
  {%- endif -%}
{% endmacro %}
```

### Configuration Options
- Allow project-level default limit in dbt_project.yml
- Support for custom environment names
- Option to sample instead of limit for better data distribution

## 2. Assert Macro

### Problem It Solves
Currently, dbt tests run AFTER models are built. This means:
- Bad data can propagate through multiple models before being caught
- Entire pipeline runs complete before failures are detected
- Debugging requires checking separate test results

The assert macro fails DURING model execution if conditions aren't met.

### How It Works
```sql
-- In your model
{{ cdc_dbt_utils.assert(
    condition="(select count(*) from " ~ ref('upstream_model') ~ ") > 0",
    error_message="Upstream model is empty - halting execution"
) }}

select 
    customer_id,
    sum(order_total) as lifetime_value
from {{ ref('upstream_model') }}
group by customer_id
```

If the assertion fails, the model stops building immediately with a clear error message.

### Common Use Cases
```sql
-- Ensure no duplicates before aggregating
{{ cdc_dbt_utils.assert(
    condition="(select count(*) from source) = (select count(distinct id) from source)",
    error_message="Source contains duplicate IDs"
) }}

-- Verify critical dimensions exist
{{ cdc_dbt_utils.assert(
    condition="(select count(*) from " ~ ref('dim_product') ~ " where product_key = -1) = 1",
    error_message="Missing default 'Unknown' record in dim_product"
) }}

-- Check data freshness inline
{{ cdc_dbt_utils.assert(
    condition="(select max(updated_at) from source) >= current_date - 1",
    error_message="Source data is stale (more than 1 day old)"
) }}
```

### Implementation Approach
```sql
{% macro assert(condition, error_message) %}
  {% if execute %}
    {% set result = run_query("select case when " ~ condition ~ " then 1 else 0 end as assertion_result") %}
    {% if result.rows[0][0] != 1 %}
      {{ exceptions.raise_compiler_error(error_message) }}
    {% endif %}
  {% endif %}
{% endmacro %}
```

## 3. Business Days Between

### Problem It Solves
Many business metrics require working day calculations:
- SLA tracking (response time in business days)
- Financial settlement periods
- Manufacturing lead times
- HR metrics (vacation days, processing times)

Standard DATEDIFF doesn't account for weekends or holidays.

### How It Works
```sql
select 
    order_id,
    order_date,
    ship_date,
    {{ cdc_dbt_utils.business_days_between('order_date', 'ship_date') }} as fulfillment_business_days
from orders
```

### Features
- Excludes weekends (Saturday/Sunday)
- Optionally exclude holidays (via dim_date holiday flags)
- Handles negative values (if end_date < start_date)
- Timezone aware (optional)

### Implementation Strategy
```sql
{% macro business_days_between(start_date, end_date, exclude_holidays=false) %}
  -- This would join with dim_date to count only business days
  -- Uses the weekday_flag we already have in dim_date
  -- Optionally uses holiday flags once we add them
{% endmacro %}
```

## 4. Log Query Macro

### Problem It Solves
When debugging complex dbt models:
- Hard to know which CTEs are slow
- Difficult to track row counts through transformations
- No visibility into intermediate results
- Can't easily identify performance bottlenecks

### How It Works
```sql
with raw_orders as (
    select * from {{ source('erp', 'orders') }}
    {{ cdc_dbt_utils.log_query('raw_orders', show_sample=true) }}
),

filtered_orders as (
    select * from raw_orders 
    where order_status != 'cancelled'
    {{ cdc_dbt_utils.log_query('filtered_orders') }}
)

select * from filtered_orders
```

**Output in logs**:
```
[LOG] raw_orders: 1,234,567 rows, executed in 2.3s
[LOG] Sample (first 3 rows):
  order_id | customer_id | order_date  | total
  1001     | 5678       | 2024-01-01  | 99.99
  1002     | 5679       | 2024-01-01  | 149.99
  1003     | 5680       | 2024-01-02  | 75.00
[LOG] filtered_orders: 1,180,234 rows, executed in 0.8s
```

### Features
- Row count logging
- Execution time tracking
- Optional sample data display
- Configurable log levels (debug, info, warn)
- Only runs in development by default

## Why These Features Matter

### Not Duplicating dbt-utils
After research, these features are NOT in dbt-utils:
- `limit_in_dev` - Unique to our needs
- `assert` - Inline execution stopping (dbt tests run after)
- `business_days_between` - Business logic not in dbt-utils
- `log_query` - Enhanced debugging beyond dbt's native logging

### Real CDC Use Cases
1. **limit_in_dev**: Every CDC project works with large client datasets
2. **assert**: Critical for data quality in financial/regulatory contexts
3. **business_days_between**: Common in client SLA reporting
4. **log_query**: Essential for optimizing slow transformations

### Implementation Priority
1. Start with `limit_in_dev` (easiest, immediate value)
2. Then `assert` (high value for data quality)
3. Follow with `business_days_between` (after dim_date enhancements)
4. Finally `log_query` (nice to have for debugging)