/* 
  Example dbt model demonstrating CDC logging integration
  
  This model shows various logging features:
  - Automatic process tracking via hooks
  - Error handling and logging
  - Row count capture
  - Custom parameters
*/

{{ config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='sync_all_columns',
    
    -- Logging configuration
    pre_hook=[
        -- Start process logging with expected row count
        "{{ cdc_dbt_utils.process_start(
            application_name='customer_analytics',
            process_name='customer_lifetime_value',
            rows_expected=50000
        ) }}",
        
        -- Create temp table for metrics
        "CREATE TEMPORARY TABLE IF NOT EXISTS _model_metrics (
            metric_name VARCHAR(100),
            metric_value NUMBER,
            logged_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"
    ],
    
    post_hook=[
        -- Log final metrics
        """
        INSERT INTO _model_metrics (metric_name, metric_value)
        SELECT 'final_row_count', COUNT(*) FROM {{ this }}
        """,
        
        -- Log any data quality issues found
        """
        {% if var('check_data_quality', true) %}
            {% set quality_check %}
                SELECT COUNT(*) as issue_count
                FROM {{ this }}
                WHERE lifetime_value < 0
                    OR customer_id IS NULL
            {% endset %}
            
            {% set results = run_query(quality_check) %}
            {% if execute and results.rows[0][0] > 0 %}
                {{ cdc_dbt_utils.log_error(
                    'Data quality issues found: ' ~ results.rows[0][0] ~ ' rows with negative LTV or null customer_id',
                    severity_level=3,
                    error_category='data_quality'
                ) }}
            {% endif %}
        {% endif %}
        """,
        
        -- Stop process logging
        "{{ cdc_dbt_utils.process_stop() }}"
    ]
) }}


-- Main model logic
WITH customer_orders AS (
    SELECT 
        customer_id,
        order_date,
        order_amount,
        order_status
    FROM {{ ref('stg_orders') }}
    WHERE order_status NOT IN ('cancelled', 'refunded')
    {% if is_incremental() %}
        AND order_date > (SELECT MAX(last_order_date) FROM {{ this }})
    {% endif %}
),

customer_metrics AS (
    SELECT 
        customer_id,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        COUNT(DISTINCT order_date) as order_count,
        SUM(order_amount) as lifetime_value,
        AVG(order_amount) as avg_order_value,
        DATEDIFF('day', MIN(order_date), MAX(order_date)) as customer_lifetime_days
    FROM customer_orders
    GROUP BY customer_id
),

-- Add customer attributes
enriched_customers AS (
    SELECT 
        cm.*,
        c.customer_segment,
        c.acquisition_channel,
        c.customer_status,
        
        -- Calculate additional metrics
        CASE 
            WHEN cm.customer_lifetime_days = 0 THEN cm.lifetime_value
            ELSE cm.lifetime_value / NULLIF(cm.customer_lifetime_days, 0)
        END as daily_value,
        
        CASE
            WHEN cm.order_count >= 10 AND cm.lifetime_value >= 1000 THEN 'VIP'
            WHEN cm.order_count >= 5 OR cm.lifetime_value >= 500 THEN 'Regular'
            ELSE 'New'
        END as customer_tier
        
    FROM customer_metrics cm
    LEFT JOIN {{ ref('dim_customer') }} c
        ON cm.customer_id = c.customer_id
)

-- Validate and log any issues before final select
{% if execute %}
    {% set validation_query %}
        WITH validation AS (
            SELECT 
                COUNT(*) FILTER (WHERE customer_id IS NULL) as null_customers,
                COUNT(*) FILTER (WHERE lifetime_value < 0) as negative_ltv,
                COUNT(*) FILTER (WHERE customer_segment IS NULL) as missing_segment,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(lifetime_value) as total_ltv
            FROM enriched_customers
        )
        SELECT * FROM validation
    {% endset %}
    
    {% set validation_results = run_query(validation_query) %}
    {% if validation_results and validation_results.rows %}
        {% set v = validation_results.rows[0] %}
        
        -- Log validation metrics
        {% if v[0] > 0 or v[1] > 0 or v[2] > 0 %}
            {{ cdc_dbt_utils.log_error(
                'Validation issues - Null customers: ' ~ v[0] ~ 
                ', Negative LTV: ' ~ v[1] ~ 
                ', Missing segment: ' ~ v[2],
                severity_level=3,
                error_category='data_validation'
            ) }}
        {% endif %}
        
        -- Log summary metrics
        {{ log('Customer LTV Summary - Unique customers: ' ~ v[3] ~ ', Total LTV: $' ~ v[4], info=true) }}
    {% endif %}
{% endif %}

-- Final select with all metrics
SELECT 
    customer_id,
    first_order_date,
    last_order_date,
    order_count,
    lifetime_value,
    avg_order_value,
    customer_lifetime_days,
    daily_value,
    customer_segment,
    acquisition_channel,
    customer_status,
    customer_tier,
    
    -- Add processing metadata
    CURRENT_TIMESTAMP() as processed_at,
    '{{ invocation_id }}' as dbt_invocation_id
    
FROM enriched_customers
WHERE customer_id IS NOT NULL  -- Ensure no nulls in primary key

-- Example of handling specific error conditions
{% if var('fail_on_duplicates', false) %}
    {% set duplicate_check %}
        SELECT customer_id, COUNT(*) as cnt
        FROM enriched_customers
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        LIMIT 1
    {% endset %}
    
    {% set dup_results = run_query(duplicate_check) %}
    {% if execute and dup_results.rows|length > 0 %}
        {{ cdc_dbt_utils.process_stop_with_error(
            'Duplicate customer_id found: ' ~ dup_results.rows[0][0],
            error_code=-100
        ) }}
        {{ exceptions.raise_compiler_error('Duplicate customer_id found in results') }}
    {% endif %}
{% endif %}