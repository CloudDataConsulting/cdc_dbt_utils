# Cloud Data Consulting (CDC) dbt Utilities

Includes Macros and Date/Time Dimensions for use in a dbt project.

## Macros

### drop_dev_scheama
Allows the user to drop all user specific development databases.

### generate_schema_name
Allows for the creation of user-name specific development schemas in the target database.

### star
Manages PK/FK key relationships when building physical dimensional model.