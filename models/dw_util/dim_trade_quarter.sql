{{ config(materialized='table') }}
with trade_months as (select * from {{ ref('dim_trade_month') }})
, quarter_aggregated as (
    select
        -- Natural key: Year + Quarter
        trade_year_num * 10 + trade_quarter_num as trade_quarter_key
        -- Core identifiers
        , trade_year_num
        , trade_quarter_num
        , trade_quarter_nm
        -- Quarter boundaries
        , min(trade_month_start_dt) as trade_quarter_start_dt
        , max(trade_month_end_dt) as trade_quarter_end_dt
        , min(trade_month_start_key) as trade_quarter_start_key
        , max(trade_month_end_key) as trade_quarter_end_key
        -- Month metrics
        , min(trade_month_num) as first_month_of_quarter_num
        , max(trade_month_num) as last_month_of_quarter_num
        , count(distinct trade_month_num) as months_in_quarter_num  -- Should always be 3
        -- Week metrics (summed from months)
        , sum(weeks_in_month_num) as weeks_in_quarter_num
        , min(first_week_of_month_num) as first_week_of_quarter_num
        , max(last_week_of_month_num) as last_week_of_quarter_num
        -- Day metrics
        , sum(days_in_month_num) as days_in_quarter_num
        -- Pattern-specific week counts for the quarter -- all the same
        , sum(trade_weeks_in_month_445_num) as trade_weeks_in_quarter_num
        {# SUM(trade_weeks_in_month_454_num) as trade_weeks_in_quarter_454_num,
        SUM(trade_weeks_in_month_544_num) as trade_weeks_in_quarter_544_num, #}
        -- Leap week flag
        , max(contains_leap_week_flg) as contains_leap_week_flg
    from trade_months
    group by trade_year_num, trade_quarter_num, trade_quarter_nm
)
, final as (
    select
        trade_quarter_key
        , trade_year_num
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_start_dt
        , trade_quarter_end_dt
        , trade_quarter_start_key
        , trade_quarter_end_key
        -- Month metrics
        , first_month_of_quarter_num
        , last_month_of_quarter_num
        , months_in_quarter_num
        -- Week metrics
        , weeks_in_quarter_num
        , first_week_of_quarter_num
        , last_week_of_quarter_num
        -- Pattern validation (all should be 13 weeks)
        , trade_weeks_in_quarter_num  -- Should be 13 (4+4+5)
--        trade_weeks_in_quarter_454_num,  -- Should be 13 (4+5+4)
--        trade_weeks_in_quarter_544_num,  -- Should be 13 (5+4+4)
        -- Day metrics
        , days_in_quarter_num
        -- Display formats
        , 'Q' || trade_quarter_num::varchar as trade_quarter_txt
        , trade_quarter_nm || ' ' || trade_year_num::varchar as trade_quarter_year_nm
        , 'TY' || trade_year_num::varchar || '-Q' || trade_quarter_num::varchar as trade_year_quarter_txt
        , trade_year_num::varchar || '-Q' || trade_quarter_num::varchar as year_quarter_txt
        -- Quarter of year position (for sorting across years)
        , case trade_quarter_num
            when 1 then 'First Quarter'
            when 2 then 'Second Quarter'
            when 3 then 'Third Quarter'
            when 4 then 'Fourth Quarter'
        end as trade_quarter_position_txt
        -- Leap week flag
        , contains_leap_week_flg
        -- Overall numbering
        , dense_rank() over (order by trade_year_num, trade_quarter_num) as trade_quarter_overall_num
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
        , current_timestamp as dw_synced_ts
        , 'dim_trade_quarter' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from quarter_aggregated)
select * from final
