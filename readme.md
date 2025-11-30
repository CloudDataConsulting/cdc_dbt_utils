# Cloud Data Consulting (CDC) dbt Utilities

Comprehensive dbt utility package with reusable macros and time-based dimensional models at multiple grains (date, week, month, quarter).

## Installation

Include in your `packages.yml`:
```yaml
packages:
  - git: "https://github.com/CloudDataConsulting/cdc_dbt_utils.git"
    revision: v1.0.0  # or main for latest
```

Then add to your `dbt_project.yml`:
```yaml
models:
  cdc_dbt_utils:
    dw_util:
      +materialized: table  # or view as needed
      +schema: dw_util
```

## Dimensional Models

### Standard Calendar Dimensions
All standard calendar dimensions derive from `dim_date` for consistency:

- **dim_date**: Daily grain with comprehensive calendar attributes (base dimension)
- **dim_week**: Weekly grain derived from dim_date with ISO week standards
- **dim_month**: Monthly grain derived from dim_date
- **dim_quarter**: Quarterly grain derived from dim_date
- **dim_time**: Intraday time dimension (one row per second)

### Trade/Retail Calendar Dimensions
All trade calendar dimensions derive from `dim_trade_date` for consistency:

- **dim_trade_date**: Daily grain with retail calendar support (4-4-5, 4-5-4, 5-4-4 patterns)
- **dim_trade_week**: Weekly grain derived from dim_trade_date
- **dim_trade_month**: Monthly grain derived from dim_trade_date
- **dim_trade_quarter**: Quarterly grain derived from dim_trade_date

## Macros

### drop_dev_schemas
Allows the user to drop all user-specific development schemas.

```bash
dbt run-operation drop_dev_schemas --args '{username: bpruss}'
```

### generate_schema_name
Allows for the creation of username-specific development schemas in the target database.
We use this macro to create a separate set of schemas for each developer in the form of:
`{username}_schema`

This gives each developer their own private namespace to work in, avoiding collisions between developers.

### star
Generates a role-playing dimension from a dimension by prefixing each column with the role. Example below creates `dim_start_date` where the role is "start" and the dimension is `dim_date`.

**Model: dim_start_date.sql**
```sql
select
   {{ star(from=ref('dim_date'), column_alias='start_') }}
from {{ ref('dim_date') }}
```

### last_run_fields
This macro appends 4 audit columns:
```sql
,current_user::varchar(50) as dw_created_by
,current_timestamp as dw_created_ts
,current_user::varchar(50) as dw_modified_by
,current_timestamp as dw_modified_ts
```
These record valuable timestamps related to when the database objects are created/modified.

## Column Naming Conventions

All models follow CDC standardized naming with class words at the end:

| Suffix | Usage | Examples |
|--------|-------|----------|
| `_num` | Numeric values | `day_of_week_num`, `quarter_num` |
| `_nm` | Names | `month_nm`, `day_nm` |
| `_dt` | Date values | `full_dt`, `week_start_dt` |
| `_key` | Date keys (YYYYMMDD) | `date_key`, `week_start_key` |
| `_flg` | Boolean flags | `weekday_flg`, `leap_year_flg` |
| `_txt` | Text strings | `day_suffix_txt` |
| `_abbr` | Abbreviations | `month_abbr`, `day_abbr` |
| `_ts` | Timestamps | `dw_synced_ts`, `create_ts` |

## Breaking Changes in v1.0.0

The `dim_date` model column naming convention has been standardized:
- All columns ending in `_number` are now `_num`
- All columns ending in `_name` are now `_nm`
- Date keys use `_key` suffix (not `_id`)
- ISO columns have `iso_` prefix

## Migration from v0.x to v1.0.0

Update your models to use the new column names:
- `quarter_number` → `quarter_num`
- `quarter_name` → `quarter_nm`
- `day_of_week_number` → `day_of_week_num`
- `week_begin_date_id` → `week_start_key`
- etc.

## Change Log

See [CHANGELOG.md](CHANGELOG.md) for full version history.

- v1.0.0 - Major release with standardized naming conventions, new aggregate dimensions, and trade calendar support
- v0.1.4 - Changed `tests:` to `data_tests:` per dbt v1.8+ requirements
