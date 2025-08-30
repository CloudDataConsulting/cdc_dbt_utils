{{ config(materialized='table') }}

WITH dim_trade_date as (select * from  {{ ref('dim_trade_date') }})
,  trade_weeks AS (
    SELECT
        -- Use the monday of each week as the natural key
        trade_week_start_key as trade_week_key,

        -- Core week identifiers
        MIN(calendar_full_dt) as trade_week_start_dt,
        MAX(calendar_full_dt) as trade_week_end_dt,
        MIN(date_key) as trade_week_start_key,
        MAX(date_key) as trade_week_end_key,

        -- Week attributes (constant within week, so just use MAX)
        MAX(trade_year_num) as trade_year_num,
        MAX(trade_week_num) as trade_week_num,
        MAX(trade_week_of_year_num) as trade_week_of_year_num,
        MAX(trade_week_of_quarter_num) as trade_week_of_quarter_num,
        MAX(trade_week_overall_num) as trade_week_overall_num,

        -- Month variants (only need these, not quarter variants)
        MAX(trade_month_445_num) as trade_month_445_num,
        MAX(trade_month_445_nm) as trade_month_445_nm,
        MAX(trade_week_of_month_445_num) as trade_week_of_month_445_num,

        MAX(trade_month_454_num) as trade_month_454_num,
        MAX(trade_month_454_nm) as trade_month_454_nm,
        MAX(trade_week_of_month_454_num) as trade_week_of_month_454_num,

        MAX(trade_month_544_num) as trade_month_544_num,
        MAX(trade_month_544_nm) as trade_month_544_nm,
        MAX(trade_week_of_month_544_num) as trade_week_of_month_544_num,

        -- Quarter (only one variant needed)
        MAX(trade_quarter_num) as trade_quarter_num,
        MAX(trade_quarter_nm) as trade_quarter_nm,

        -- Week metrics
        COUNT(*) as days_in_week_num,
        MAX(weeks_in_trade_year_num) as weeks_in_trade_year_num,
        MAX(is_trade_leap_week_flg) as is_trade_leap_week_flg

    FROM dim_trade_date
    GROUP BY trade_week_start_key),
final AS (
    SELECT
        *,

        -- Derived display columns
        'TY' || trade_year_num::varchar || '-W' || LPAD(trade_week_num::varchar, 2, '0') as trade_year_week_txt,

        -- Current period flags
        CASE WHEN trade_week_start_dt <= CURRENT_DATE()
             AND trade_week_end_dt >= CURRENT_DATE()
             THEN 1 ELSE 0 END as is_current_trade_week_flg,

        CASE WHEN trade_week_start_dt <= DATEADD(week, -1, CURRENT_DATE())
             AND trade_week_end_dt >= DATEADD(week, -1, CURRENT_DATE())
             THEN 1 ELSE 0 END as is_prior_trade_week_flg,

        CASE WHEN trade_year_num = YEAR(CURRENT_DATE())
             THEN 1 ELSE 0 END as is_current_trade_year_flg,

        CASE WHEN trade_week_end_dt < CURRENT_DATE()
             THEN 1 ELSE 0 END as is_past_trade_week_flg,

        -- Relative date calculations
        DATEDIFF(week, trade_week_start_dt, CURRENT_DATE()) as trade_weeks_ago_num,

        -- Metadata
        CURRENT_TIMESTAMP as dw_synced_ts,
        'dim_trade_week' as dw_source_nm,
        CURRENT_USER as create_user_id,
        CURRENT_TIMESTAMP as create_timestamp
    FROM trade_weeks)
SELECT * FROM final
