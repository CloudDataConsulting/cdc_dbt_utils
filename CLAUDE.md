# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a dbt utilities package that provides reusable macros and time and date dimensional models for Cloud Data Consulting (CDC) dbt projects. It's designed to be imported as a dependency in other dbt projects.

## Key Architecture

### Package Structure
- **macros/**: Reusable dbt macros for schema management, column generation, and dimensional modeling
- **models/dw_util/**: Pre-built reusable dimensional models (date and time dimensions)

### Core Macros

1. **generate_schema_name**: Creates developer-specific schemas in format `{username}_{schema_name}` to avoid collisions in development
2. **star**: Generates role-playing dimensions by prefixing columns (requires dbt_utils dependency)
3. **drop_dev_schemas**: Drops all developer-specific schemas for a given username
4. **last_run_fields**: Appends audit columns (dw_created_by, dw_created_ts, dw_modified_by, dw_modified_ts)

### Models

#### Standard Calendar Dimensions
- **dim_date**: Daily grain date dimension with comprehensive date attributes (base for other date dimensions)
- **dim_week**: Weekly grain dimension derived from dim_date, includes ISO week
- **dim_month**: Monthly grain dimension derived from dim_date
- **dim_quarter**: Quarterly grain dimension derived from dim_date
- **dim_time**: Time dimension for intraday analysis

#### Trade Calendar Dimensions
- **dim_trade_date**: Daily grain with trade/retail calendar support (4-4-5, 4-5-4, 5-4-4 patterns)
- **dim_trade_week**: Weekly grain dimension derived from dim_trade_date
- **dim_trade_month**: Monthly grain dimension derived from dim_trade_date (separate records per pattern)
- **dim_trade_quarter**: Quarterly grain dimension derived from dim_trade_date

**Important**: Standard calendar dimensions (dim_week, dim_month, dim_quarter) derive from dim_date. Trade calendar dimensions (dim_trade_week, dim_trade_month, dim_trade_quarter) derive from dim_trade_date. This ensures:
- Consistency across all date dimensions
- Automatic updates when dim_date date range changes
- Single source of truth for date calculations

## Development Commands

### Installing in a Project
To do development we make an exception to the normal process.
We do NOT add the package to the consuming project's `packages.yml`!
We then cd dbt_packages && git clone git@github.com:CloudDataConsulting/cdc_dbt_utils.git

### Configuration in Consuming Project
Add to `dbt_project.yml`:
```yaml
models:
  cdc_dbt_utils:
    dw_util:
      +materialized: table  # or view as needed
      +schema: dw_util
```

### Running Macros
```bash
# Drop developer schemas
dbt run-operation drop_dev_schemas --args '{username: developer_name}'

# Build dimensions in consuming project
dbt run --models cdc_dbt_utils.dw_util.*
or
dbt run -s +package:cdc_dbt_utils
```

### Testing
```bash
# Run data tests defined in yml files
dbt test --models cdc_dbt_utils.dw_util.*
or
dbt test -s +package:cdc_dbt_utils
```

### Run & Test
```bash
# to run and test as we run
dbt build -s +package:cdc_dbt_utils
```

## Column Naming Convention Standards

The package follows strict naming conventions where the class word (data type indicator) appears at the END of the column name:
- `*_num` - Numeric values (e.g., `day_of_week_num`, `month_overall_num`)
- `*_nm` - Names (e.g., `month_nm`, `quarter_nm`)
- `*_dt` - Date/datetime values (e.g., `week_begin_dt`, `first_day_of_month_dt`)
- `*_key` - Date keys in YYYYMMDD format (e.g., `week_begin_key`, `date_key`)
- `*_id` - Other identifiers (e.g., `user_id`, `product_id`)
- `*_flg` - Boolean flags (e.g., `weekday_flg`, `end_of_month_flg`)
- `*_txt` - Text strings (e.g., `day_suffix_txt`, `iso_week_of_year_txt`)
- `*_abbr` - Abbreviations (e.g., `month_abbr`, `day_abbr`)

ISO-related columns have `iso_` prefix: `iso_day_of_week_num`, `iso_year_num`, `iso_week_of_year_txt`

For dim_trade_date and its aggregates
trade_*
Should have the same name for the same thing.
for instance
trade_first_day_dt

For dim_date and its aggregates
No prefix.

"Overall" metrics (since 1970-01-01) use pattern: `{measure}_overall_{class}` (e.g., `day_overall_num`, `month_overall_num`)

## CTE Standards

All dbt models in this package must follow CDC CTE standards:

### CTE Structure Rules
1. **First CTE must reference source models**: Always start with CTEs that reference source models using `{{ ref() }}`
2. **Meaningful aliases only**: Use full, descriptive names for CTEs - never abbreviations
3. **All refs at the top**: All `{{ ref() }}` statements must be in the first CTEs, with any filters applied there
4. **Descriptive comments in YAML**: Model descriptions belong in the `.yml` files, not as comments in SQL files

### CTE Naming Examples (includes aliases)
- ✅ GOOD: `trade_date`, `date_dimension`, `monthly_aggregated_data`, `quarter_with_retail_calendar`
- ❌ BAD: `td`, `dd`, `month_agg`, `qtr_retail`

### Standard CTE Pattern
- leading comma's
- ) on the same line as last bit of sql, not on new line.
- No extra blank lines
```sql
{{ config(materialized='table') }}

-- All refs must be at the top
with source_model_alias as ( select * from {{ ref('source_model') }} where date_key > 0 )  -- Any filters on the ref go here

,transformed_data as (
    -- Main transformations
    select ...
    from source_model_alias)
, final as (
    -- Final structure
    select ...
    from transformed_data)
select * from final
```

## Important Notes

- This package has an implicit dependency on `dbt_utils` (used in star macro)
- Snowflake-specific SQL is used in dimensional models
- Version compatibility: dbt >=1.0.0, <2.0.0
- Uses `data_tests:` syntax (dbt v1.8+) instead of deprecated `tests:`
- All models materialize as tables by default but can be overridden in consuming project
- when making iterative changes to a version beta or otherwise package-lock.yml stores the hash so it has to be deleted after the dbt clean before the dbp deps.

## Version History
- Current version: 1.0.0 (preparing for release)
- Last published version: 0.1.4
- See CHANGELOG.md for complete version history
- Major breaking changes in v1.0.0: Standardized column naming conventions
