# Cloud Data Consulting (CDC) dbt Utilities

Comprehensive dbt utility package with reusable macros and time-based dimensional models at multiple grains (date, week, month, quarter).

## Installation

Include in your `packages.yml`:
```yaml
packages:
  - git: "https://github.com/CloudDataConsulting/cdc_dbt_utils.git"
    revision: 2.0.0  # or main for latest
```

Then add to your `dbt_project.yml`:
```yaml
models:
  cdc_dbt_utils:
    dw_util:
      +materialized: view  # or table as needed
      +schema: dw_util
```

## Dimensional Models

### Standard Calendar Dimensions
- **dim_date**: Daily grain with comprehensive calendar attributes
- **dim_week**: Weekly grain with ISO week standards
- **dim_month**: Monthly grain with fiscal attributes
- **dim_quarter**: Quarterly grain with fiscal year support
- **dim_time**: Intraday time dimension (86,400 rows - one per second)

### Trade/Retail Calendar Dimensions
- **dim_trade_date**: Daily grain with trade/retail calendar (4-4-5, 4-5-4, 5-4-4 patterns)
- **dim_trade_week**: Weekly grain for trade calendar
- **dim_trade_month**: Monthly grain for trade calendar (separate rows per pattern)
- **dim_trade_quarter**: Quarterly grain for trade calendar

## Macros

### drop_dev_schemas
Allows the user to drop all user specific development databases.

`dbt run-operation drop_dev_schemas --args '{username: bpruss}' `

### generate_schema_name
Allows for the creation of user-name specific development schemas in the target database.
We use this macro to create a separate set of schemas for each develolper in the form of: 
{username}_schema 
This gives each developer their own private namespace to work in, thus avoiding collisions between developers.  

### star
This generates a role playing dimension from a dimension, by prefixing each column in the target with the role. Our example below is for dim_start_date where the role is "start"
and the dimension is dim_date.  

Model:<br> 
dim_start_date.sql <br>
`select
   {{ star(from=ref('dim_date'), column_alias='start_') }}
from {{ ref('dim_date') }}`

### Last run fields
This macro appends 4 columns:
    ,current_user::varchar(50) as dw_created_by
    ,current_timestamp dw_created_ts
    ,current_user::varchar(50) as dw_modified_by
    ,current_timestamp dw_modified_ts
which record valuable timestamps related to when the database objects are created/modified.

## Breaking Changes in v2.0.0

All dimension model column naming conventions have been standardized:
- All columns ending in `_number` are now `_num` 
- All columns ending in `_name` are now `_nm`
- Date keys use `_key` suffix (not `_id`)
- ISO columns have `iso_` prefix

## Migration from v1.x to v2.0.0

Update your models to use the new column names:
- `quarter_number` → `quarter_num`
- `quarter_name` → `quarter_nm`
- `day_of_week_number` → `day_of_week_num`
- `week_begin_date_id` → `week_begin_key`
- `create_timestamp` → `create_ts`
- `full_time` → `full_tm` (dim_time)
- `week_start_dt` → `week_begin_dt` (dim_week)
- etc.

## Change Log
- v2.0.0 - Standardized column naming conventions across all dimensions (class word abbreviations at end)
- v1.0.0 - Major release with standardized naming conventions and new time dimensions
- v0.1.4 - Changed tests: to data_tests: per https://docs.getdbt.com/docs/build/data-tests#new-data_tests-syntax

