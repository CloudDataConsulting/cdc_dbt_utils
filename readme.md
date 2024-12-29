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

### drop_dev_scheama
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

# Change Log
- v 1.3.1 - change tests: to data_tests: per https://docs.getdbt.com/docs/build/data-tests#new-data_tests-syntax

