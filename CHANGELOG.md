# Changelog

All notable changes to the CDC dbt Utilities package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.2] - 2025-12-05
### Fixed
- Extended dim_trade_date range from 2040 to 2045 to ensure complete trade calendar coverage
- Fixed dim_time "Not Set" record with null flags (now uses false instead of null)
- Fixed dim_trade_week `denserank()` → `dense_rank()` syntax error
- Fixed dim_week ISO week parsing error in `split_part` expression
- Fixed dim_trade_month `expected_weeks_in_month_num` calculation to account for leap weeks in 53-week years
- Updated dim_trade_month tests to allow 6-week months and 42-day months in 53-week years

### Changed
- Upgraded to dbt 1.10.15 with pyproject.toml and uv for dependency management
- Added packages.yml with dbt_utils dependency
- Added sqlfluff configuration in pyproject.toml following CDC standards

## [1.0.1] - 2025 (tagged but not released)
### Fixed
- Fixed dbt 1.10 deprecation warnings for generic tests

## [1.0.0] - 2025
### Added
- New dimensional models at different time grains (all derived from dim_date for consistency):
  - **dim_week**: Week-level dimension derived from dim_date with ISO week standards and retail calendar
  - **dim_month**: Month-level dimension derived from dim_date with fiscal and retail attributes
  - **dim_quarter**: Quarter-level dimension derived from dim_date including fiscal year support
- dim_date_retail model with retail calendar support and comprehensive date features:
  - All standard calendar features from dim_date
  - Retail/trade calendar with 4-4-5, 4-5-4, and 5-4-4 patterns
  - Complete set of date, week, month, quarter, and year attributes
  - ISO week standards and date formatting options
- dim_date_retail.yml with model documentation and tests
- CHANGELOG.md for tracking version history
- CLAUDE.md for repository guidance

### Changed
- **BREAKING**: Standardized all column naming conventions with class words at the end:
  - All `_number` columns renamed to `_num` (e.g., `quarter_number` → `quarter_num`)
  - All `_name` columns renamed to `_nm` (e.g., `quarter_name` → `quarter_nm`)
  - Date identifiers use `_key` suffix for YYYYMMDD format (e.g., `week_begin_key`)
  - ISO columns prefixed with `iso_` (e.g., `iso_day_of_week_num`, `iso_year_num`)
  - "Overall" metrics follow pattern `{measure}_overall_{class}` (e.g., `day_overall_num`)
  - Proper class word suffixes: `_dt` for dates, `_key` for date keys, `_flg` for flags, `_txt` for text

### Removed
- **BREAKING**: All verbose column names from dim_date (e.g., `quarter_number`, `quarter_name`)
- Users must update to use new abbreviated naming convention

## [0.1.4] - 2024
### Fixed
- Fixed version number in project.yml
- Updated test syntax from `tests:` to `data_tests:` per dbt v1.8+ requirements (CDC-595)

## [0.1.3] - 2024
### Fixed
- Fixed varchar(16M) issue in dim_date model (CDC-540)

## [0.1.2] - 2024
### Added
- Added last_run_fields macro for audit columns (PD-69)
  - Appends dw_created_by, dw_created_ts, dw_modified_by, dw_modified_ts columns

## [0.1.1] - 2024
### Added
- Updated README to include dbt_project.yml installation instructions

## [0.1.0] - 2024 - Initial Release
### Added
- Core macros:
  - generate_schema_name: Developer-specific schema generation
  - star: Role-playing dimension generation (requires dbt_utils)
  - drop_dev_schemas: Drop developer-specific schemas
- Dimensional models:
  - dim_date: Comprehensive date dimension
  - dim_time: Time dimension for intraday analysis
- Copyright notices to all CDC created resources