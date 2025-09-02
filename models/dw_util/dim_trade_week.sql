{{ config(materialized='table') }}
with dim_trade_date as (select * from {{ ref('dim_trade_date') }}
where date_key > 0)  -- exclude special records
, trade_weeks as (
    select
        -- Use the Sunday of each week as the natural key
        min(date_key) as trade_week_key
        -- Core week identifiers
        , min(full_dt) as trade_week_start_dt
        , max(full_dt) as trade_week_end_dt
        , min(date_key) as trade_week_start_key
        , max(date_key) as trade_week_end_key
        -- Week attributes (constant within week, so just use MAX)
        , max(trade_year_num) as trade_year_num
        , max(trade_week_num) as trade_week_num
        , max(trade_week_of_year_num) as trade_week_of_year_num
        , max(trade_week_of_quarter_num) as trade_week_of_quarter_num
        , max(trade_week_overall_num) as trade_week_overall_num
        -- Month variants (only need these, not quarter variants)
        , max(trade_month_445_num) as trade_month_445_num
        , max(trade_month_445_nm) as trade_month_445_nm
        , max(trade_week_of_month_445_num) as trade_week_of_month_445_num
        , max(trade_month_454_num) as trade_month_454_num
        , max(trade_month_454_nm) as trade_month_454_nm
        , max(trade_week_of_month_454_num) as trade_week_of_month_454_num
        , max(trade_month_544_num) as trade_month_544_num
        , max(trade_month_544_nm) as trade_month_544_nm
        , max(trade_week_of_month_544_num) as trade_week_of_month_544_num
        -- Quarter (only one variant needed)
        , max(trade_quarter_num) as trade_quarter_num
        , max(trade_quarter_nm) as trade_quarter_nm
        -- Week metrics
        , count(*) as days_in_week_num
        , max(weeks_in_trade_year_num) as weeks_in_trade_year_num
        , max(trade_leap_week_flg) as trade_leap_week_flg
    from dim_trade_date
    group by trade_week_start_dt, trade_week_end_dt)
, final as (
    select
        *
        -- Derived display columns
        , 'TY' || trade_year_num::varchar || '-W' || lpad(trade_week_num::varchar, 2, '0') as trade_year_week_txt
        -- Current period flags
        , case when trade_week_start_dt <= current_date()
             and trade_week_end_dt >= current_date()
             then 1 else 0 end as is_current_trade_week_flg
        , case when trade_week_start_dt <= dateadd(week, -1, current_date())
             and trade_week_end_dt >= dateadd(week, -1, current_date())
             then 1 else 0 end as is_prior_trade_week_flg
        , case when trade_year_num = year(current_date())
             then 1 else 0 end as is_current_trade_year_flg
        , case when trade_week_end_dt < current_date()
             then 1 else 0 end as is_past_trade_week_flg
        -- Relative date calculations
        , datediff(week, trade_week_start_dt, current_date()) as trade_weeks_ago_num
        -- Metadata
        , current_timestamp() as dw_synced_ts
        , 'dim_trade_week' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp() as create_timestamp
    from trade_weeks)
select * from final
