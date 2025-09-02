{{ config(materialized='table') }}
with trade_weeks as (select * from {{ ref('dim_trade_week') }})
, month_aggregated as (
    select
        -- Natural key: Year + Month
        trade_year_num * 100 + min(trade_month_445_num) as trade_month_key
        -- Core identifiers (same across all patterns)
        , trade_year_num
        , min(trade_month_445_num) as trade_month_num  -- All three should be the same
        , min(trade_month_445_nm) as trade_month_nm     -- All three should be the same
        , trade_quarter_num
        , trade_quarter_nm
        -- Common boundaries (same regardless of pattern)
        , min(trade_week_start_dt) as trade_month_start_dt
        , max(trade_week_end_dt) as trade_month_end_dt
        , min(trade_week_start_key) as trade_month_start_key
        , max(trade_week_end_key) as trade_month_end_key
        -- Common metrics
        , min(trade_week_num) as first_week_of_month_num
        , max(trade_week_num) as last_week_of_month_num
        , count(distinct trade_week_num) as weeks_in_month_num
        , sum(days_in_week_num) as days_in_month_num
        -- 445 Pattern specifics
        , max(trade_week_of_month_445_num) as trade_weeks_in_month_445_num
        , max(case when trade_week_of_month_445_num = 5 then 1 else 0 end) as is_5_week_month_445_flg
        -- 454 Pattern specifics
        , max(trade_week_of_month_454_num) as trade_weeks_in_month_454_num
        , max(case when trade_week_of_month_454_num = 5 then 1 else 0 end) as is_5_week_month_454_flg
        -- 544 Pattern specifics
        , max(trade_week_of_month_544_num) as trade_weeks_in_month_544_num
        , max(case when trade_week_of_month_544_num = 5 then 1 else 0 end) as is_5_week_month_544_flg
        -- Leap week flag
        , max(trade_leap_week_flg) as contains_leap_week_flg
    from trade_weeks
    group by trade_year_num, trade_quarter_num, trade_quarter_nm)
    -- Group by year and quarter since month num should be derivable,
, final as (
    select
        trade_month_key
        , trade_year_num
        , trade_month_num
        , trade_month_nm
        , left(trade_month_nm, 3) as trade_month_abbr
        , trade_quarter_num
        , trade_quarter_nm
        , trade_month_start_dt
        , trade_month_end_dt
        , trade_month_start_key
        , trade_month_end_key
        -- Position metrics
        , case
            when trade_month_num in (1, 4, 7, 10) then 1
            when trade_month_num in (2, 5, 8, 11) then 2
            else 3
        end as trade_month_in_quarter_num
        -- Common week/day metrics
        , weeks_in_month_num
        , days_in_month_num
        , first_week_of_month_num
        , last_week_of_month_num
        , contains_leap_week_flg
        -- Pattern-specific columns
        , trade_weeks_in_month_445_num
        , is_5_week_month_445_flg
        , trade_weeks_in_month_454_num
        , is_5_week_month_454_flg
        , trade_weeks_in_month_544_num
        , is_5_week_month_544_flg
        -- Display formats
        , trade_month_nm || ' ' || trade_year_num::varchar as trade_month_year_nm
        , 'TY' || trade_year_num::varchar || '-M' || lpad(trade_month_num::varchar, 2, '0') as trade_year_month_txt
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
        , dense_rank() over (order by trade_year_num, trade_month_num) as trade_month_overall_num
        -- Metadata
        , current_timestamp as dw_synced_ts
        , 'dim_trade_month' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from month_aggregated)
select * from final
