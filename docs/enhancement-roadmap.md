# CDC dbt Utils Enhancement Roadmap

This document outlines proposed enhancements for the cdc_dbt_utils package, organized by priority and with verification of existing functionality in dbt-core and dbt-utils.

## High Priority Enhancements

### 1. generate_surrogate_key

**Purpose**: Create consistent hash-based surrogate keys for dimension tables, ensuring the same input always produces the same key across runs.

**Why it's needed**: 
- Snowflake doesn't have a built-in hash function that's consistent across warehouses
- Need deterministic keys for slowly changing dimensions (SCD Type 2)
- Essential for matching records across environments

**Example usage**:
```sql
select 
    {{ cdc_dbt_utils.generate_surrogate_key(['customer_id', 'effective_date']) }} as customer_key,
    customer_id,
    customer_name,
    effective_date
from raw_customers
```

**Existing alternatives**:
- ✅ **dbt-utils has this**: `dbt_utils.generate_surrogate_key()` - Creates an MD5 hash of concatenated fields
- ❌ **We may NOT need to build this** - dbt-utils already provides this functionality

### 2. test_recency

**Purpose**: Generic test to ensure data freshness by checking the maximum value of a timestamp column.

**Why it's needed**:
- Catch stale data before it impacts downstream models
- Alert when source systems stop sending data
- Ensure SLAs are met

**Example usage**:
```yaml
models:
  - name: orders
    tests:
      - cdc_dbt_utils.test_recency:
          date_column: created_at
          threshold_days: 2
          error_if: "older_than"
```

**Existing alternatives**:
- ✅ **dbt-utils has this**: `dbt_utils.recency` test - Checks if data is fresh
- ❌ **We may NOT need to build this** - Already available in dbt-utils

### 3. pivot

**Purpose**: Transform row data into columns dynamically, similar to SQL PIVOT but works across all databases.

**Why it's needed**:
- Not all databases support PIVOT natively
- Need consistent syntax across Snowflake, BigQuery, Redshift, etc.
- Common requirement for reporting tables

**Example usage**:
```sql
select * from (
    {{ cdc_dbt_utils.pivot(
        column_name='product_category',
        values=['Electronics', 'Clothing', 'Food'],
        agg='sum(sales_amount)',
        then_value='sales_amount'
    ) }}
) 
from sales_data
group by customer_id
```

**Existing alternatives**:
- ✅ **dbt-utils has this**: `dbt_utils.pivot()` - Provides cross-database pivot functionality
- ❌ **We may NOT need to build this** - Already available in dbt-utils

### 4. limit_in_dev

**Purpose**: Automatically limit query results in development environments to speed up model development and testing.

**Why it's needed**:
- Prevent accidentally processing billions of rows during development
- Speed up development cycle
- Save on compute costs
- Maintain full data in production

**Example usage**:
```sql
select *
from large_table
{{ cdc_dbt_utils.limit_in_dev(10000) }}
```

Would compile to:
- In dev: `select * from large_table limit 10000`
- In prod: `select * from large_table`

**Existing alternatives**:
- ❌ **dbt-utils does NOT have this** specific functionality
- ⚠️ **Partial alternative**: Can use `{% if target.name == 'dev' %}` but it's verbose
- ✅ **We SHOULD build this** - Provides cleaner syntax and standardization

## Medium Priority Enhancements

### Date/Time Enhancements

#### Add fiscal year support to dim_date
- Currently commented out in the existing model
- Add configurable fiscal year start month
- Include fiscal quarter, fiscal week calculations

#### Add timezone support to dim_time  
- Add UTC offset columns
- Support for DST transitions
- Timezone name columns

#### Add holiday flags to dim_date
- Major US holidays
- Configurable for other countries
- Business day calculations

### SQL Helper Macros

#### unpivot
**Purpose**: Transform columns into rows (opposite of pivot)

**Existing alternatives**:
- ✅ **dbt-utils has this**: `dbt_utils.unpivot()` 
- ❌ **We may NOT need to build this**

#### clean_stale_models
**Purpose**: Remove models from production that no longer exist in code

**Existing alternatives**:
- ⚠️ **Partial solution**: `dbt run-operation drop_old_relations` in dbt-utils
- ✅ **We MIGHT enhance this** with better filtering options

#### log_query
**Purpose**: Log queries for debugging with timing and row count information

**Existing alternatives**:
- ❌ **Not available in dbt-utils**
- ✅ **We SHOULD build this** for better debugging capabilities

#### assert
**Purpose**: Runtime assertions that fail the model if conditions aren't met

**Example**:
```sql
{{ cdc_dbt_utils.assert(
    condition="count(*) > 0",
    message="Orders table should not be empty"
) }}
```

**Existing alternatives**:
- ⚠️ **Tests exist but run separately** - This would fail during model execution
- ✅ **We SHOULD build this** for inline data quality checks

### Date/Time Utilities

#### date_spine
**Purpose**: Generate continuous date ranges

**Existing alternatives**:
- ✅ **dbt-utils has this**: `dbt_utils.date_spine()`
- ❌ **We may NOT need to build this**

#### business_days_between
**Purpose**: Calculate working days between two dates

**Existing alternatives**:
- ❌ **Not in dbt-utils**
- ✅ **We SHOULD build this** - Common business requirement

## Lower Priority Enhancements

### Developer Experience
1. GitHub Actions for automated testing
2. Example project demonstrating all features
3. Macro unit tests using dbt's testing framework
4. Contribution guidelines (CONTRIBUTING.md)

### Documentation Improvements
1. Comprehensive CHANGELOG.md
2. Macro reference documentation
3. Migration guide from raw SQL to using these utilities
4. Performance considerations guide

## Summary: What We Should Actually Build

Based on this analysis, here are the features we should focus on that aren't already in dbt-utils:

### High Value Additions
1. **limit_in_dev** - Unique value proposition, not in dbt-utils
2. **assert** - Inline data quality checks during model runs
3. **log_query** - Enhanced debugging capabilities
4. **business_days_between** - Common business logic need

### Enhance Existing Models
1. **Fiscal calendar support** in dim_date (already started)
2. **Holiday flags** in dim_date
3. **Timezone support** in dim_time

### Skip These (Already in dbt-utils)
- generate_surrogate_key ✅
- test_recency ✅
- pivot ✅
- unpivot ✅
- date_spine ✅

## Recommended Next Steps

1. Implement `limit_in_dev` macro first (high value, easy to implement)
2. Add fiscal year support to dim_date (already partially there)
3. Create `assert` macro for inline data quality
4. Build `business_days_between` for date calculations

This approach avoids duplicating dbt-utils functionality while adding genuine value for CDC's specific needs.