{{ config(materialized='table') }}
{{ config( post_hook="alter table {{ this }} add primary key (date_key)", ) }}

-- Enhanced dim_trade_date with proper 53-week year handling
-- Implements NRF standard: Week 53 maps to Week 52 of prior year
-- Alternative Walmart method available via trade_date_last_year_walmart_key

with base_trade_dates as (
    select * from {{ ref('dim_trade_date')  }} where date_key > 0)
-- Add enhanced last year comparisons following retail best practices
, enhanced_comparisons as (
    select
        td.*

        -- METHOD 1: NRF Standard (Week Shift Method)
        -- In 53-week years, Week 53 → Prior Year Week 52
        -- This maintains holiday alignment
        , case
            when td.trade_week_num = 53 then
                -- Week 53 compares to prior year Week 52
                ly52.date_key
            when td.trade_date_last_year_key is not null then
                -- Standard match exists
                td.trade_date_last_year_key
            else
                -- No match (shouldn't happen in normal data)
                null
        end as trade_date_last_year_nrf_key

        , case
            when td.trade_week_num = 53 then
                ly52.full_dt
            when td.trade_date_last_year_key is not null then
                to_date(td.trade_date_last_year_key::varchar, 'YYYYMMDD')
            else
                null
        end as trade_date_last_year_nrf_dt

        -- METHOD 2: Walmart Method
        -- Week 53 compares to Week 1 of SAME year
        , case
            when td.trade_week_num = 53 then
                -- Week 53 compares to Week 1 of same year
                w1.date_key
            when td.trade_date_last_year_key is not null then
                -- Standard match exists
                td.trade_date_last_year_key
            else
                null
        end as trade_date_last_year_walmart_key

        , case
            when td.trade_week_num = 53 then
                w1.full_dt
            when td.trade_date_last_year_key is not null then
                to_date(td.trade_date_last_year_key::varchar, 'YYYYMMDD')
            else
                null
        end as trade_date_last_year_walmart_dt

        -- METHOD 3: 364-Day Method (52 weeks exactly)
        -- Always look back exactly 364 days for same day of week
        , to_char(dateadd('day', -364, td.full_dt), 'YYYYMMDD')::int as trade_date_last_year_364_key
        , dateadd('day', -364, td.full_dt) as trade_date_last_year_364_dt

    from base_trade_dates td

    -- Join to get Week 52 of prior year for Week 53 comparisons
    left join base_trade_dates ly52
        on ly52.trade_year_num = td.trade_year_num - 1
        and ly52.trade_week_num = 52
        and ly52.date_key = to_char(
            dateadd('day',
                dayofweek(td.full_dt) - dayofweek(ly52.trade_week_start_dt),
                ly52.trade_week_start_dt
            ), 'YYYYMMDD')::int

    -- Join to get Week 1 of same year for Walmart method
    left join base_trade_dates w1
        on w1.trade_year_num = td.trade_year_num
        and w1.trade_week_num = 1
        and w1.date_key = to_char(
            dateadd('day',
                dayofweek(td.full_dt) - dayofweek(w1.trade_week_start_dt),
                w1.trade_week_start_dt
            ), 'YYYYMMDD')::int
)
, final as (
    select
        -- All original columns from dim_trade_date
        date_key
        , full_dt
        , trade_date_last_year_key as trade_date_last_year_simple_key  -- Original simple logic

        -- Enhanced comparison columns
        , trade_date_last_year_nrf_key      -- NRF standard (recommended)
        , trade_date_last_year_nrf_dt
        , trade_date_last_year_walmart_key   -- Walmart method
        , trade_date_last_year_walmart_dt
        , trade_date_last_year_364_key       -- 364-day method
        , trade_date_last_year_364_dt

        -- All other original columns
        , trade_day_of_year_num
        , trade_week_num
        , trade_week_of_year_num
        , trade_week_of_month_445_num
        , trade_week_of_month_454_num
        , trade_week_of_month_544_num
        , trade_week_of_quarter_num
        , trade_week_overall_num
        , trade_week_start_dt
        , trade_week_start_key
        , trade_week_end_dt
        , trade_week_end_key
        , trade_month_445_num
        , trade_month_454_num
        , trade_month_544_num
        , trade_month_445_nm
        , trade_month_454_nm
        , trade_month_544_nm
        , trade_month_abbr
        , trade_month_overall_num
        , trade_yearmonth_num
        , trade_month_445_start_dt
        , trade_month_445_start_key
        , trade_month_445_end_dt
        , trade_month_445_end_key
        , trade_month_454_start_dt
        , trade_month_454_start_key
        , trade_month_454_end_dt
        , trade_month_454_end_key
        , trade_month_544_start_dt
        , trade_month_544_start_key
        , trade_month_544_end_dt
        , trade_month_544_end_key
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        , trade_quarter_start_dt
        , trade_quarter_start_key
        , trade_quarter_end_dt
        , trade_quarter_end_key
        , trade_year_num
        , trade_year_start_dt
        , trade_year_start_key
        , trade_year_end_dt
        , trade_year_end_key
        , trade_leap_week_flg
        , weeks_in_trade_year_num
        , dw_synced_ts
        , dw_source_nm
        , create_user_id
        , create_ts
    from enhanced_comparisons
)
select * from final
