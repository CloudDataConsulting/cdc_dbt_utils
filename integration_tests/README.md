# CDC dbt Utils - Integration Tests

This directory contains integration tests for the cdc_dbt_utils package, specifically testing the logging integration features.

## Prerequisites

1. **Snowflake Access**: You need access to a Snowflake account with:
   - A database where you can create test schemas
   - The CDC logging framework installed (process_start_p, process_stop_p, etc.)
   - Appropriate permissions

2. **Environment Variables**: Set the following:
   ```bash
   export SNOWFLAKE_ACCOUNT=clouddata-prd  # or your account
   export SNOWFLAKE_USER=your.email@clouddataconsulting.com
   export SNOWFLAKE_PASSWORD=your_password  # or use key-pair auth
   export SNOWFLAKE_ROLE=SYSADMIN  # or appropriate role
   export SNOWFLAKE_WAREHOUSE=COMPUTE_WH  # or your warehouse
   export SNOWFLAKE_DATABASE=DEV_BPRUSS  # or your test database
   ```

3. **Logging Framework**: Ensure the CDC logging framework is installed:
   ```sql
   -- Check if procedures exist
   SHOW PROCEDURES LIKE 'process_start_p' IN SCHEMA dw_util;
   SHOW PROCEDURES LIKE 'process_stop_p' IN SCHEMA dw_util;
   SHOW PROCEDURES LIKE 'write_error_log_p' IN SCHEMA dw_util;
   ```

## Running Tests

### Quick Test
```bash
cd integration_tests
dbt deps  # Install the parent package
dbt run --models tag:logging_test  # Run all test models
```

### Full Test Suite
```bash
cd integration_tests
./run_tests.sh
```

### Test with Failures
```bash
cd integration_tests
./run_tests.sh --test-failures
```

## Test Models

1. **test_basic_logging.sql** - Tests basic pre/post hook logging
2. **test_error_logging.sql** - Tests error logging functionality
3. **test_custom_params.sql** - Tests custom parameters and row counting
4. **test_failed_model.sql** - Tests failure scenarios (disabled by default)

## Checking Results

After running tests, check the results in Snowflake:

```sql
-- View recent process executions
SELECT * 
FROM dw_util.process_report_v
WHERE application_name = 'cdc_dbt_utils_integration_tests'
  AND start_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY start_ts DESC;

-- View any errors logged
SELECT *
FROM dw_util.error_log
WHERE application_name = 'cdc_dbt_utils_integration_tests'
  AND error_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY error_ts DESC;

-- Summary statistics
SELECT 
    COUNT(*) as total_processes,
    SUM(CASE WHEN final_status = 'completed' THEN 1 ELSE 0 END) as completed,
    SUM(CASE WHEN final_status = 'failed' THEN 1 ELSE 0 END) as failed,
    AVG(duration_seconds) as avg_duration,
    SUM(rows_inserted) as total_rows
FROM dw_util.process_report_v
WHERE application_name = 'cdc_dbt_utils_integration_tests'
  AND start_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP());
```

## Troubleshooting

### "Could not find profile named 'cdc_dbt_utils'"
- Ensure you're running from the `integration_tests` directory
- Check that `profiles.yml` exists in the parent directory

### "Procedure does not exist"
- Verify the logging framework is installed in your target database
- Check that the `cdc_logging.schema` variable matches where procedures are installed

### "Permission denied"
- Ensure your role has EXECUTE permissions on the logging procedures
- Verify your role can create tables in the target schema

## Development Workflow

1. Make changes to logging macros in `../macros/logging_integration.sql`
2. Run `dbt deps` to refresh the local package reference
3. Test changes with individual models: `dbt run --models test_basic_logging`
4. Run full test suite: `./run_tests.sh`
5. Check results in Snowflake tables