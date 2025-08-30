{{ config(materialized='table') }}

WITH trade_months AS (SELECT * FROM {{ ref('dim_trade_month') }} )
,quarter_aggregated AS (
    SELECT
        -- Natural key: Year + Quarter
        trade_year_num * 10 + trade_quarter_num as trade_quarter_key,

        -- Core identifiers
        trade_year_num,
        trade_quarter_num,
        trade_quarter_nm,

        -- Quarter boundaries
        MIN(trade_month_start_dt) as trade_quarter_start_dt,
        MAX(trade_month_end_dt) as trade_quarter_end_dt,
        MIN(trade_month_start_key) as trade_quarter_start_key,
        MAX(trade_month_end_key) as trade_quarter_end_key,

        -- Month metrics
        MIN(trade_month_num) as first_month_of_quarter_num,
        MAX(trade_month_num) as last_month_of_quarter_num,
        COUNT(DISTINCT trade_month_num) as months_in_quarter_num,  -- Should always be 3

        -- Week metrics (summed from months)
        SUM(weeks_in_month_num) as weeks_in_quarter_num,
        MIN(first_week_of_month_num) as first_week_of_quarter_num,
        MAX(last_week_of_month_num) as last_week_of_quarter_num,

        -- Day metrics
        SUM(days_in_month_num) as days_in_quarter_num,

        -- Pattern-specific week counts for the quarter -- all the same
        SUM(trade_weeks_in_month_445_num) as trade_weeks_in_quarter_num,
        {# SUM(trade_weeks_in_month_454_num) as trade_weeks_in_quarter_454_num,
        SUM(trade_weeks_in_month_544_num) as trade_weeks_in_quarter_544_num, #}

        -- Leap week flag
        MAX(contains_leap_week_flg) as contains_leap_week_flg

    FROM trade_months
    GROUP BY trade_year_num, trade_quarter_num, trade_quarter_nm
)
, final AS (
    SELECT
        trade_quarter_key,
        trade_year_num,
        trade_quarter_num,
        trade_quarter_nm,
        trade_quarter_start_dt,
        trade_quarter_end_dt,
        trade_quarter_start_key,
        trade_quarter_end_key,

        -- Month metrics
        first_month_of_quarter_num,
        last_month_of_quarter_num,
        months_in_quarter_num,

        -- Week metrics
        weeks_in_quarter_num,
        first_week_of_quarter_num,
        last_week_of_quarter_num,

        -- Pattern validation (all should be 13 weeks)
        trade_weeks_in_quarter_num,  -- Should be 13 (4+4+5)
--        trade_weeks_in_quarter_454_num,  -- Should be 13 (4+5+4)
--        trade_weeks_in_quarter_544_num,  -- Should be 13 (5+4+4)

        -- Day metrics
        days_in_quarter_num,

        -- Display formats
        'Q' || trade_quarter_num::varchar as trade_quarter_txt,
        trade_quarter_nm || ' ' || trade_year_num::varchar as trade_quarter_year_nm,
        'TY' || trade_year_num::varchar || '-Q' || trade_quarter_num::varchar as trade_year_quarter_txt,
        trade_year_num::varchar || '-Q' || trade_quarter_num::varchar as year_quarter_txt,

        -- Quarter of year position (for sorting across years)
        CASE trade_quarter_num
            WHEN 1 THEN 'First Quarter'
            WHEN 2 THEN 'Second Quarter'
            WHEN 3 THEN 'Third Quarter'
            WHEN 4 THEN 'Fourth Quarter'
        END as trade_quarter_position_txt,

        -- Leap week flag
        contains_leap_week_flg,

        -- Overall numbering
        DENSE_RANK() OVER (ORDER BY trade_year_num, trade_quarter_num) as trade_quarter_overall_num,

        -- Commented out current period flags (will be in view)
        -- CASE
        --     WHEN trade_quarter_start_dt <= CURRENT_DATE()
        --         AND trade_quarter_end_dt >= CURRENT_DATE()
        --     THEN 1 ELSE 0
        -- END as is_current_trade_quarter_flg,

        -- CASE
        --     WHEN trade_year_num = YEAR(CURRENT_DATE())
        --     THEN 1 ELSE 0
        -- END as is_current_trade_year_flg,

        -- CASE
        --     WHEN trade_quarter_end_dt < CURRENT_DATE()
        --     THEN 1 ELSE 0
        -- END as is_past_trade_quarter_flg,

        -- DATEDIFF(quarter, trade_quarter_start_dt, CURRENT_DATE()) as trade_quarters_ago_num,

        -- Metadata
        CURRENT_TIMESTAMP as dw_synced_ts,
        'dim_trade_quarter' as dw_source_nm,
        CURRENT_USER as create_user_id,
        CURRENT_TIMESTAMP as create_timestamp
    FROM quarter_aggregated)
SELECT * FROM final
