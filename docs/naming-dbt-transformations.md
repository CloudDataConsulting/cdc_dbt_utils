# dbt Transformation Naming Standards

## Overview

This document extends the Cloud Data Consulting SQL Naming Standards for dbt-specific transformations and data warehouse patterns. It covers naming conventions for models through transformation layers, from raw data to analytical outputs.

**Prerequisite**: This document assumes familiarity with the [Core SQL Naming Standards](naming-core.md). All base SQL rules apply unless specifically overridden here.

## Core dbt Principles

1. **Transformation transparency** - Names should indicate the transformation stage
2. **Source lineage** - Staging models should clearly indicate their source
3. **Grain clarity** - Aggregated models must indicate their granularity
4. **Consistency across layers** - Predictable patterns through the transformation pipeline
5. **SINGULAR always** - All models use singular names (extends base SQL standard)

## Model Naming by Layer

### Layer Prefix Standards

| Layer | Prefix | Pattern | Example |
|-------|--------|---------|---------|
| Staging | `stg_` | `stg_<source>__<entity>` | `stg_salesforce__account` |
| Base | `base_` | `base_<source>__<entity>` | `base_stripe__payment` |
| Intermediate | `int_` | `int_<entity>[__<qualifier>]` | `int_customer__unioned` |
| Warehouse | (none) | `<entity>` | `customer` |
| Marts - Dimension | `dim_` | `dim_<entity>` | `dim_customer` |
| Marts - Fact | `fct_` | `fct_<business_process>` | `fct_transaction_line` |
| Marts - Aggregate | `agg_` | `agg_<metric>_<grain>` | `agg_sales_day_store` |
| Marts - Report | `rpt_` | `rpt_<purpose>` | `rpt_scorecard_by_day` |
| Marts - Reference | `ref_` | `ref_<lookup_purpose>` | `ref_payroll_plan_by_week` |
| Marts - Bridge | `brg_` | `brg_<relationship>` | `brg_customer_product` |
| Snapshots | `snap_` | `snap_<entity>` | `snap_inventory` |

### Double Underscore Convention

**Rule**: Use double underscore (`__`) to separate source system from entity in staging/base layers:
- ✅ `stg_salesforce__account`
- ✅ `base_stripe__payment`
- ❌ `stg_salesforce_account`
- ❌ `base-stripe-payment`

**Rationale**: Clear visual separation between source system and business entity.

## Column Transformations Through Layers

### Staging Layer (`stg_`)

**Purpose**: Light transformations, 1:1 with source

**Column Rules**:
```sql
-- Standard staging transformations
select
    -- IDs: Rename generic 'id' to entity_id
    id as customer_id,  -- was: id

    -- Strings: Trim whitespace
    trim(name) as customer_nm,  -- was: name

    -- Codes: Uppercase and trim
    upper(trim(status)) as status_cd,  -- was: status

    -- Dates: Cast to proper type
    created_date::date as created_dt,  -- was: created_date

    -- Add source identifier
    'salesforce' as source_nm
from {{ source('salesforce', 'accounts') }}
```

**Required Transformations**:
1. Trim all string columns: `trim(column_name)`
2. Rename `id` to `<table>_id`
3. Add source identifier column
4. Apply class word suffixes where clear
5. Cast data types appropriately

### Base Layer (`base_`)

**Purpose**: Clean and standardize individual sources before joining

**When to Use Base Layer**:
- Multiple related tables from same source need joining
- Complex cleaning logic would clutter staging models
- Source data needs significant restructuring

**Column Rules**:
```sql
-- Base layer: Heavier transformations
select
    -- Standardize naming across similar sources
    customer_id,
    customer_nm,

    -- Create consistent status values
    case
        when status_cd in ('A', 'ACTIVE', '1') then 'ACTIVE'
        when status_cd in ('I', 'INACTIVE', '0') then 'INACTIVE'
        else 'UNKNOWN'
    end as status_cd,

    -- Calculate derived fields
    datediff('day', created_dt, current_date) as account_age_num,

    source_nm
from {{ ref('stg_salesforce__account') }}
```

### Intermediate Layer (`int_`)

**Purpose**: Business logic, unions, complex joins

**Naming Patterns**:
- Simple intermediate: `int_<entity>`
- With qualifier: `int_<entity>__<action>`

**Common Qualifiers**:
| Qualifier | Usage | Example |
|-----------|-------|---------|
| `__unioned` | Combining multiple sources | `int_customer__unioned` |
| `__deduped` | After deduplication | `int_order__deduped` |
| `__enhanced` | After enrichment | `int_product__enhanced` |
| `__pivoted` | After pivot operation | `int_sales__pivoted` |
| `__aggregated` | Pre-aggregation step | `int_transactions__aggregated` |

**Column Standardization**:
```sql
-- Intermediate: Standardize across sources
with customers_unioned as (
    select
        -- Standardized column names
        customer_id,
        customer_nm,
        email_txt,
        phone_num,

        -- Consistent status values
        status_cd,

        -- Standard timestamps
        created_at_ts,
        updated_at_ts,

        -- Track source
        source_nm
    from {{ ref('int_salesforce__customer') }}

    union all

    select
        customer_id,
        customer_nm,
        email_txt,
        phone_num,
        status_cd,
        created_at_ts,
        updated_at_ts,
        source_nm
    from {{ ref('int_stripe__customer') }}
)
select * from customers_unioned
```

### Warehouse Layer (no prefix)

**Purpose**: Clean, normalized entities ready for analysis

**Rules**:
- No prefix needed - these are your core business entities
- SINGULAR table names
- Full business logic applied
- Ready to be referenced by marts

**Example**: `customer`, `product`, `order`, `employee`

### Marts Layer

#### Dimension Tables (`dim_`)

**Pattern**: `dim_<entity>[_<type>]`

**Column Conventions**:
```sql
-- Dimension table with surrogate key
select
    -- Surrogate key (for star schema)
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'source_nm']) }} as customer_key,

    -- Natural key (preserved)
    customer_id,

    -- Attributes with consistent suffixes
    customer_nm,
    customer_type_cd,
    tier_cd,

    -- SCD Type 2 fields (if applicable)
    effective_from_dt,
    effective_to_dt,
    is_current_flg,

    -- Metadata
    source_nm,
    loaded_at_ts
from {{ ref('customer') }}
```

#### Fact Tables (`fct_`)

**Pattern**: `fct_<business_process>[_<grain>]`

**Grain in Name**:
- If grain is not obvious, include it: `fct_sales_daily`
- If grain is transaction-level, omit: `fct_sales`

**Column Conventions**:
```sql
-- Fact table with foreign keys and measures
select
    -- Foreign keys (match dimension surrogate keys)
    customer_key,
    product_key,
    date_key,
    store_key,

    -- Degenerate dimensions
    transaction_id,
    line_num,

    -- Measures with clear suffixes
    quantity_qty,
    unit_price_amt,
    discount_amt,
    tax_amt,
    total_amt,

    -- Calculated measures
    margin_amt,
    margin_pct,

    -- Metadata
    loaded_at_ts
from {{ ref('int_transaction_line') }}
```

#### Aggregate Tables (`agg_`)

**Pattern**: `agg_<metrics>_<time_grain>_<dimensional_grain>`

**Grain Naming Rules**:
1. **Time grain** (required): `hour`, `day`, `week`, `month`, `quarter`, `year`
2. **Dimensional grain** (required): Dimensions included in the aggregate
3. **Order**: Metrics → Time → Dimensions

**Examples**:
- `agg_sales_day_store` - Daily sales by store
- `agg_sales_day_store_product` - Daily sales by store and product
- `agg_payroll_week_store_department` - Weekly payroll by store and department
- `agg_inventory_month_warehouse` - Monthly inventory by warehouse

**Self-Documenting Nature**:
The name indicates exactly what dimensions are available:
- `agg_sales_day_store` - Can filter/group by day and store only
- Missing dimensions (like product) are NOT available at this grain

#### Report Tables (`rpt_`)

**Pattern**: `rpt_<report_name>` or `rpt_<system>__<purpose>`

**Rules**:
- Include target system if relevant: `rpt_tableau__executive_dashboard`
- Include business purpose: `rpt_monthly_scorecard`
- These are denormalized for specific reporting needs

#### Reference Tables (`ref_`)

**Pattern**: `ref_<lookup_purpose>`

**Common Uses**:
- Planning data: `ref_sales_plan_by_day`
- Security configuration: `ref_row_level_security`
- Business rules: `ref_pricing_tiers`
- Calendar/date dimensions: `ref_fiscal_calendar`

#### Bridge Tables (`brg_`)

**Pattern**: `brg_<entity1>_<entity2>`

**Rules**:
- SINGULAR forms for both entities
- Alphabetical order preferred
- Used for many-to-many relationships

**Example**:
```sql
-- Bridge table for many-to-many
select
    customer_key,
    product_key,
    relationship_type_cd,
    effective_from_dt,
    effective_to_dt
from {{ ref('int_customer_product_associations') }}
```

## Snapshot Naming

**Pattern**: `snap_<entity>`

**Location**: Typically in `snapshots/` directory

**Column Additions**:
```sql
-- Standard snapshot columns (added by dbt)
dbt_scd_id,
dbt_updated_at,
dbt_valid_from,
dbt_valid_to
```

## Dynamic Table Naming

**Rule**: No special suffix for dynamic tables

**Rationale**: Materialization strategy shouldn't affect naming

✅ `dim_customer` (happens to be a dynamic table)
❌ `dim_customer_dt`
❌ `dim_customer_dynamic`

Document materialization in config:
```sql
{{ config(
    materialized='dynamic_table',
    target_lag='10 minutes',
    warehouse='dev_elt_xs_wh'
) }}
```

## Test Naming

### Schema Tests
**Location**: `models/<layer>/schema.yml`

**Naming**: Use descriptive test names
```yaml
models:
  - name: stg_salesforce__account
    tests:
      - name: status_values_are_valid
        test: accepted_values
        column_name: status_cd
        values: ['ACTIVE', 'INACTIVE', 'PENDING']
```

### Data Tests
**Pattern**: `test_<description>.sql`
**Location**: `tests/`

**Examples**:
- `test_orders_have_customers.sql`
- `test_no_orphaned_transactions.sql`

## Macro Naming

**Pattern**: `<action>_<object>`

**Examples**:
- `generate_schema_name`
- `get_fiscal_quarter`
- `calculate_business_days`
- `union_relations`

**Prefix Conventions**:
| Prefix | Usage |
|--------|--------|
| `get_` | Retrieve/calculate a value |
| `generate_` | Create new SQL/values |
| `test_` | Generic test macros |
| `clean_` | Data cleansing operations |

## File Organization

### Directory Structure
```
models/
├── staging/
│   ├── salesforce/
│   │   ├── _salesforce__models.yml
│   │   ├── _salesforce__sources.yml
│   │   ├── stg_salesforce__account.sql
│   │   └── stg_salesforce__contact.sql
│   └── stripe/
│       ├── _stripe__models.yml
│       ├── _stripe__sources.yml
│       └── stg_stripe__payment.sql
├── intermediate/
│   ├── finance/
│   │   ├── _int_finance__models.yml
│   │   ├── int_customer__unioned.sql
│   │   └── int_revenue__calculated.sql
├── warehouse/
│   ├── _warehouse__models.yml
│   ├── customer.sql
│   └── product.sql
└── marts/
    ├── core/
    │   ├── _core__models.yml
    │   ├── dim_customer.sql
    │   ├── dim_product.sql
    │   └── fct_sales.sql
    └── finance/
        ├── _finance__models.yml
        ├── agg_revenue_month_customer.sql
        └── rpt_monthly_financial_summary.sql
```

### Schema YAML File Naming
**Pattern**: `_<layer>__models.yml`

**Rules**:
- Leading underscore to sort first in directory
- Double underscore before 'models'
- One schema file per subdirectory

## Special Considerations

### Incremental Models

No special naming convention - use config:
```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail'
) }}
```

### Ephemeral Models

**Pattern**: Same as layer they belong to
**Documentation**: Note ephemeral nature in schema.yml

### Seeds

**Pattern**: `seed_<purpose>.csv`
**Examples**:
- `seed_country_codes.csv`
- `seed_profit_centers.csv`

### Variables and Environment Patterns

**Development Schemas**:
```sql
-- In dbt_project.yml or macro
{% if target.name == 'dev' %}
    {{ target.user }}_{{ this.name }}
{% else %}
    analytics_{{ this.name }}
{% endif %}
```

Results in:
- Dev: `bpruss_dim_customer`
- Prod: `analytics_dim_customer`

## Anti-Patterns to Avoid

❌ **Plural table names**: `customers`, `orders`, `products`
❌ **Hungarian notation**: `tbl_customer`, `vw_sales`
❌ **Environment in model name**: `dev_dim_customer.sql`
❌ **Timestamps without timezone**: Use `_ts` with `timestamp_ntz`
❌ **Ambiguous abbreviations**: `cust_ord_dtl` instead of `customer_order_detail`
❌ **Missing grain in aggregates**: `agg_sales` instead of `agg_sales_day_store`
❌ **Single underscore in staging**: `stg_salesforce_account`

## Migration from Legacy Patterns

### Common Conversions
| Legacy Pattern | New Standard |
|---------------|--------------|
| `customers` | `customer` |
| `tbl_orders` | `order` |
| `v_sales_daily` | `agg_sales_day` |
| `customer_dims` | `dim_customer` |
| `sales_facts` | `fct_sales` |
| `staging_products` | `stg_source__product` |

### Deprecation Strategy
1. Create new models with correct names
2. Create views with old names pointing to new models
3. Update downstream dependencies
4. Add deprecation notices
5. Remove deprecated views after transition period

---

**Document Status**: CDC dbt Transformation Standard
**Version**: 1.0
**Last Updated**: 2025-01-24
**Next Review**: 2025-07-01
**Author**: Cloud Data Consulting Architecture Team
**Parent Standard**: [CDC SQL Naming Standards v2.0](../cdc-sql-naming-standards.md)

## Quick Reference Card

### Model Prefixes
- `stg_` - Staging (light transforms)
- `base_` - Base (heavy source cleaning)
- `int_` - Intermediate (business logic)
- `dim_` - Dimension (star schema)
- `fct_` - Fact (star schema)
- `agg_` - Aggregate (pre-calculated)
- `rpt_` - Report (denormalized)
- `ref_` - Reference (lookups)
- `brg_` - Bridge (many-to-many)
- `snap_` - Snapshot (SCD Type 2)

### Must Remember
- **SINGULAR always** - `customer` not `customers`
- **Double underscore** - `stg_stripe__payment`
- **Grain in aggregates** - `agg_sales_day_store`
- **No environment names** - Handled by schema config
- **Class word suffixes** - `_id`, `_dt`, `_amt`, `_flg`, etc.
