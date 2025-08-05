# Changelog

All notable changes to the cdc_dbt_utils package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0-beta.2] - 2024-08-05

### Changed
- Fixed dbt_utils version constraint to use proper range syntax: [">=1.1.1", "<2.0.0"]

### Removed
- Removed unnecessary package-lock.yml file
- Added ai-logs and .user.yml to .gitignore

## [0.2.0-beta.1] - 2024-08-05

### Added
- **Comprehensive Snowflake Logging Integration** - New macro suite for process tracking
  - `process_start` - Begin logging a model execution with customizable parameters
  - `process_stop` - Complete logging with row counts and status
  - `log_error` - Log errors and data quality issues to error_log table
  - `process_stop_with_error` - Combined error logging and process stop
  - `with_logging` - Wrapper macro for manual logging control
  - `start_run_logging` / `stop_run_logging` - Run-level operations for orchestration tools
- **Integration Test Suite** - Complete test framework in `integration_tests/`
  - Test models for various logging scenarios
  - Automated test runner script
  - Results verification macros
- **Comprehensive Documentation**
  - Logging setup guide with configuration examples
  - Enhancement roadmap for future development
  - High-priority features analysis
  - Integration test documentation

### Changed
- Added `dbt_utils` as explicit dependency in packages.yml
- Enhanced dim_date.yml with complete column descriptions (42 columns)
- Enhanced dim_time.yml with complete column descriptions (16 columns)
- Added comprehensive tests to dim_time model
- Updated .gitignore to exclude test artifacts and profiles.yml

### Fixed
- Fixed typo in readme.md: "drop_dev_scheama" → "drop_dev_schemas"
- Resolved hidden dependency on dbt_utils in star macro

## [0.1.4] - 2024-01-15

### Changed
- Updated test syntax from `tests:` to `data_tests:` per dbt v1.0+ requirements
- Ensures compatibility with latest dbt versions

## [0.1.3] - 2023-11-20

### Added
- Enhanced dim_date model with fiscal year support (commented out for customization)
- Additional date attributes for comprehensive time intelligence

### Changed
- Optimized dim_date generation for better performance
- Improved documentation in dimensional models

## [0.1.2] - 2023-09-15

### Added
- `last_run_fields` macro for audit column generation
- Adds dw_created_by, dw_created_ts, dw_modified_by, dw_modified_ts columns

### Changed
- Updated dim_time model to use last_run_fields macro
- Improved macro documentation

## [0.1.1] - 2023-07-10

### Fixed
- Fixed issue with generate_schema_name macro in production environments
- Corrected schema naming pattern for multi-developer scenarios

## [0.1.0] - 2023-06-01

### Added
- Initial release of cdc_dbt_utils package
- **Core Macros:**
  - `generate_schema_name` - Creates user-specific development schemas
  - `drop_dev_schemas` - Cleans up development schemas
  - `star` - Advanced SELECT * with column prefixing and exclusion
- **Dimensional Models:**
  - `dim_date` - Comprehensive date dimension (50,000+ rows, ~137 years)
  - `dim_time` - Time dimension with second-level granularity (86,400 rows)
- Basic project structure and documentation

## Testing Instructions for 0.2.0-beta.1

### What to Test

1. **Logging Integration**
   - Pre/post hooks automatically log model executions
   - Error logging captures failures appropriately
   - Row counts are captured when enabled
   - Process IDs link correctly between start/stop

2. **Configuration**
   - Test with `cdc_logging.enabled: false` to ensure it doesn't break models
   - Verify schema configuration works with your logging tables location
   - Test environment-specific settings (dev vs prod)

3. **Error Handling**
   - Models should continue to work even if logging procedures don't exist
   - Logging failures shouldn't break model execution
   - Error messages are properly captured in error_log table

4. **Performance**
   - Minimal overhead from logging calls
   - Row counting doesn't significantly slow large models

### How to Test

1. Add to your packages.yml:
   ```yaml
   packages:
     - git: "https://github.com/CloudDataConsulting/cdc_dbt_utils.git"
       revision: 0.2.0-beta.1
   ```

2. Configure in dbt_project.yml:
   ```yaml
   vars:
     cdc_logging:
       enabled: true
       schema: 'dw_util'  # Your logging schema
   
   models:
     +pre-hook: "{{ cdc_dbt_utils.process_start() }}"
     +post-hook: "{{ cdc_dbt_utils.process_stop() }}"
   ```

3. Run your models and check:
   - `dw_util.process_report_v` for execution tracking
   - `dw_util.error_log` for any errors
   - Model output remains unchanged

### Known Limitations

- Requires CDC Snowflake logging framework to be installed
- Process IDs may not persist correctly between pre/post hooks in all scenarios
- Row counting requires additional SELECT COUNT(*) query

### Feedback

Please report issues or feedback on the GitHub repository or contact the CDC team.