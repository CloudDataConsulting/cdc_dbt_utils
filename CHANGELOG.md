# Changelog

All notable changes to the CDC dbt Utilities package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024

### Added
- New standard calendar aggregate dimensions (all derived from dim_date for consistency):
  - **dim_week**: Week-level dimension with ISO week standards
  - **dim_month**: Month-level dimension
  - **dim_quarter**: Quarter-level dimension
- Trade/retail calendar dimensions with 4-4-5, 4-5-4, and 5-4-4 pattern support:
  - **dim_trade_date**: Daily grain with all three trade calendar patterns
  - **dim_trade_week**: Weekly grain derived from dim_trade_date
  - **dim_trade_month**: Monthly grain derived from dim_trade_date
  - **dim_trade_quarter**: Quarterly grain derived from dim_trade_date
- Multiple year-over-year comparison methods for 53-week year handling:
  - NRF Standard: Week 53 maps to prior year week 52
  - Walmart Method: Week 53 maps to same year week 1
  - 364-Day Method: Always compare to exactly 52 weeks prior
- Comprehensive data tests for all models in .yml files
- CHANGELOG.md for tracking version history
- CLAUDE.md for repository guidance

### Changed
- **BREAKING**: Standardized all column naming conventions with class words at the end:
  - All `_number` columns renamed to `_num` (e.g., `quarter_number` → `quarter_num`)
  - All `_name` columns renamed to `_nm` (e.g., `quarter_name` → `quarter_nm`)
  - Date identifiers use `_key` suffix for YYYYMMDD format (e.g., `week_start_key`)
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
