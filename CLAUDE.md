# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a dbt utility package that provides reusable macros and dimensional models for data warehouse projects. It's designed to be installed as a dependency in other dbt projects.

## Common Development Commands

```bash
# Install dependencies (if any are added to packages.yml)
dbt deps

# Run all models in the package
dbt run

# Run tests on all models
dbt test

# Run a specific model
dbt run --models dim_date

# Test a specific model
dbt test --models dim_date

# Generate and serve documentation
dbt docs generate
dbt docs serve

# Clean up development schemas (replace 'username' with actual username)
dbt run-operation drop_dev_schemas --args '{username: bpruss}'
```

## Architecture and Key Components

### Macros (located in `/macros/`)

1. **`generate_schema_name`**: Creates user-specific development schemas to prevent developer collisions. This macro is automatically invoked by dbt and creates schemas like `{username}_{schema_name}`.

2. **`drop_dev_schemas`**: Cleans up all development schemas for a specific user. Essential for maintaining a clean development environment.

3. **`star`**: Generates SELECT statements with optional column prefixing for role-playing dimensions. Supports column exclusion and relation aliasing.

4. **`last_run_fields`**: Adds audit columns (`dw_created_by`, `dw_created_ts`, `dw_modified_by`, `dw_modified_ts`) to track data lineage.

### Models (located in `/models/dw_util/`)

- **`dim_date`**: Comprehensive date dimension with 50,000+ rows covering ~137 years
- **`dim_time`**: Time dimension with one row per second (86,400 rows)

Both models include extensive attributes and are optimized for Snowflake.

## Development Workflow

### Adding New Macros
1. Create new `.sql` file in `/macros/`
2. Document the macro in the file header
3. Update `readme.md` with usage examples
4. Test the macro in a sample project

### Adding New Models
1. Create new `.sql` file in appropriate subdirectory under `/models/`
2. Create corresponding `.yml` file for documentation and tests
3. Define tests using `data_tests:` syntax (not the deprecated `tests:`)
4. Run `dbt run --models new_model_name` to test

### Testing Conventions
- Use dbt's built-in test framework
- Define tests in YAML files alongside models
- Common tests: `unique`, `not_null`, `relationships`, `accepted_values`
- Run all tests with `dbt test` or specific model tests with `dbt test --models model_name`

## Package Usage

To use this package in other dbt projects:

```yaml
# In packages.yml
packages:
  - git: "https://github.com/CloudDataConsulting/cdc_dbt_utils.git"
    revision: main  # or specific version tag
```

Then run `dbt deps` to install.

## Important Notes

- This package has a soft dependency on `dbt-utils` (the `star` macro references it)
- All models materialize as tables by default
- The package supports dbt versions `>=1.0.0, <2.0.0`
- Development schemas follow the pattern `{username}_{schema_name}` to avoid conflicts
- Fiscal year support in `dim_date` is commented out but can be enabled by uncommenting the relevant sections