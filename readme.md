# Cloud Data Consulting (CDC) dbt Utilities

Includes Macros and Date/Time Dimensions for use in a dbt project.

## Macros

### drop_dev_scheama
Allows the user to drop all user specific development databases.

`dbt run-operation drop_dev_schemas`

Note: this is a work in progress. Not tested.  

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
