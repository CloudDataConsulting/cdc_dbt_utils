# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a dbt utilities package that provides reusable macros and dimensional models for Cloud Data Consulting (CDC) dbt projects. It's designed to be imported as a dependency in other dbt projects.

## Key Architecture

### Package Structure
- **macros/**: Reusable dbt macros for schema management, column generation, and dimensional modeling
- **models/dw_util/**: Pre-built dimensional models (date and time dimensions)

### Core Macros

1. **generate_schema_name**: Creates developer-specific schemas in format `{username}_{schema_name}` to avoid collisions in development
2. **star**: Generates role-playing dimensions by prefixing columns (requires dbt_utils dependency)
3. **drop_dev_schemas**: Drops all developer-specific schemas for a given username
4. **last_run_fields**: Appends audit columns (dw_created_by, dw_created_ts, dw_modified_by, dw_modified_ts)

### Models
- **dim_date**: Daily grain date dimension with comprehensive date attributes (base for other date dimensions)
- **dim_date_retail**: Daily grain with retail calendar support (4-4-5, 4-5-4, 5-4-4 patterns)
- **dim_week**: Weekly grain dimension derived from dim_date, includes ISO week and retail calendar
- **dim_month**: Monthly grain dimension derived from dim_date, includes fiscal and retail attributes  
- **dim_quarter**: Quarterly grain dimension derived from dim_date, includes fiscal year support
- **dim_time**: Time dimension for intraday analysis

**Important**: dim_week, dim_month, and dim_quarter are all derived from dim_date. This ensures:
- Consistency across all date dimensions
- Automatic updates when dim_date date range changes
- Single source of truth for date calculations

## Development Commands

### Installing in a Project
Add to the consuming project's `packages.yml`:
```yaml
packages:
  - git: "https://github.com/CloudDataConsulting/cdc_dbt_utils.git"
    revision: main
```

Then run: `dbt deps`

### Configuration in Consuming Project
Add to `dbt_project.yml`:
```yaml
models:
  cdc_dbt_utils:
    dw_util:
      +materialized: view  # or table as needed
      +schema: dw_util
```

### Running Macros
```bash
# Drop developer schemas
dbt run-operation drop_dev_schemas --args '{username: developer_name}'

# Build dimensions in consuming project
dbt run --models cdc_dbt_utils.dw_util.*
```

### Testing
```bash
# Run data tests defined in yml files
dbt test --models cdc_dbt_utils.dw_util.*
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

"Overall" metrics (since 1970-01-01) use pattern: `{measure}_overall_{class}` (e.g., `day_overall_num`, `month_overall_num`)

## Important Notes

- This package has an implicit dependency on `dbt_utils` (used in star macro)
- Snowflake-specific SQL is used in dimensional models
- Version compatibility: dbt >=1.0.0, <2.0.0
- Uses `data_tests:` syntax (dbt v1.8+) instead of deprecated `tests:`
- All models materialize as tables by default but can be overridden in consuming project

## Version History
- Current version: 1.0.0 (preparing for release)
- Last published version: 0.1.4
- See CHANGELOG.md for complete version history
- Major breaking changes in v1.0.0: Standardized column naming conventions