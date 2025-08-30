{{ config(materialized='table') }}

WITH trade_weeks AS (SELECT * FROM {{ ref('dim_trade_week') }}),
month_aggregated AS (
    SELECT
        -- Natural key: Year + Month
        trade_year_num * 100 + MIN(trade_month_445_num) as trade_month_key,

        -- Core identifiers (same across all patterns)
        trade_year_num,
        MIN(trade_month_445_num) as trade_month_num,  -- All three should be the same
        MIN(trade_month_445_nm) as trade_month_nm,     -- All three should be the same
        trade_quarter_num,
        trade_quarter_nm,

        -- Common boundaries (same regardless of pattern)
        MIN(trade_week_start_dt) as trade_month_start_dt,
        MAX(trade_week_end_dt) as trade_month_end_dt,
        MIN(trade_week_start_key) as trade_month_start_key,
        MAX(trade_week_end_key) as trade_month_end_key,

        -- Common metrics
        MIN(trade_week_num) as first_week_of_month_num,
        MAX(trade_week_num) as last_week_of_month_num,
        COUNT(DISTINCT trade_week_num) as weeks_in_month_num,
        SUM(days_in_week_num) as days_in_month_num,

        -- 445 Pattern specifics
        MAX(trade_week_of_month_445_num) as trade_weeks_in_month_445_num,
        MAX(CASE WHEN trade_week_of_month_445_num = 5 THEN 1 ELSE 0 END) as is_5_week_month_445_flg,

        -- 454 Pattern specifics
        MAX(trade_week_of_month_454_num) as trade_weeks_in_month_454_num,
        MAX(CASE WHEN trade_week_of_month_454_num = 5 THEN 1 ELSE 0 END) as is_5_week_month_454_flg,

        -- 544 Pattern specifics
        MAX(trade_week_of_month_544_num) as trade_weeks_in_month_544_num,
        MAX(CASE WHEN trade_week_of_month_544_num = 5 THEN 1 ELSE 0 END) as is_5_week_month_544_flg,

        -- Leap week flag
        MAX(is_trade_leap_week_flg) as contains_leap_week_flg

    FROM trade_weeks
    GROUP BY trade_year_num, trade_quarter_num, trade_quarter_nm)
    -- Group by year and quarter since month num should be derivable,
, final AS (
    SELECT
        trade_month_key,
        trade_year_num,
        trade_month_num,
        trade_month_nm,
        LEFT(trade_month_nm, 3) as trade_month_abbr,
        trade_quarter_num,
        trade_quarter_nm,
        trade_month_start_dt,
        trade_month_end_dt,
        trade_month_start_key,
        trade_month_end_key,

        -- Position metrics
        CASE
            WHEN trade_month_num IN (1, 4, 7, 10) THEN 1
            WHEN trade_month_num IN (2, 5, 8, 11) THEN 2
            ELSE 3
        END as trade_month_in_quarter_num,

        -- Common week/day metrics
        weeks_in_month_num,
        days_in_month_num,
        first_week_of_month_num,
        last_week_of_month_num,
        contains_leap_week_flg,

        -- Pattern-specific columns
        trade_weeks_in_month_445_num,
        is_5_week_month_445_flg,
        trade_weeks_in_month_454_num,
        is_5_week_month_454_flg,
        trade_weeks_in_month_544_num,
        is_5_week_month_544_flg,

        -- Display formats
        trade_month_nm || ' ' || trade_year_num::varchar as trade_month_year_nm,
        'TY' || trade_year_num::varchar || '-M' || LPAD(trade_month_num::varchar, 2, '0') as trade_year_month_txt,

        {# -- Current period flags
        CASE
            WHEN trade_month_start_dt <= CURRENT_DATE()
                AND trade_month_end_dt >= CURRENT_DATE()
            THEN 1 ELSE 0
        END as is_current_trade_month_flg,

        CASE
            WHEN trade_year_num = YEAR(CURRENT_DATE())
            THEN 1 ELSE 0
        END as is_current_trade_year_flg,

        CASE
            WHEN trade_month_end_dt < CURRENT_DATE()
            THEN 1 ELSE 0
        END as is_past_trade_month_flg, #}

        -- Overall numbering
        DENSE_RANK() OVER (ORDER BY trade_year_num, trade_month_num) as trade_month_overall_num,

        -- Metadata
        CURRENT_TIMESTAMP as dw_synced_ts,
        'dim_trade_month' as dw_source_nm,
        CURRENT_USER as create_user_id,
        CURRENT_TIMESTAMP as create_timestamp
    FROM month_aggregated)
select * from final
