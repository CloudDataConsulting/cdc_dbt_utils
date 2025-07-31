# Cloud Data Consulting (CDC) dbt Utilities

Includes Macros and Date/Time Dimensions for use in a dbt project.

## Installation

Include in the models code block reference to the cdc_dbt_utils:

models:
  cdc_dbt_utils:
    dw_util:
      +materialized: view
      +schema: dw_util

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

### logging_integration
Comprehensive integration with CDC's Snowflake process logging framework. Provides automatic tracking of dbt model executions, performance metrics, and error handling.

Key features:
- `process_start` - Begin logging a model execution
- `process_stop` - Complete logging with row counts and status
- `log_error` - Log errors and data quality issues
- `process_stop_with_error` - Convenience method for failure handling
- Run-level operations for orchestration tools

See [Logging Setup Guide](docs/logging-setup-guide.md) for detailed configuration and usage.

# Change Log
- v 0.3.2 - Add comprehensive logging integration with Snowflake process tracking
- v 0.3.1 - Add dbt_utils dependency, fix documentation, add column descriptions
- v 0.1.4 - change tests: to data_tests: per https://docs.getdbt.com/docs/build/data-tests#new-data_tests-syntax

