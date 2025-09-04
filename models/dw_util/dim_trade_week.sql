{{ config(materialized='table') }}

with date as ( select * from {{ ref('dim_trade_date') }} where date_key > 0 )
,all_cols as (
  select
    date.date_key
    , date.full_dt
    , date.trade_date_last_year_key
    , date.trade_day_of_year_num
    , date.trade_week_num
    , date.trade_week_of_year_num
    , date.trade_week_of_month_445_num
    , date.trade_week_of_month_454_num
    , date.trade_week_of_month_544_num
    , date.trade_week_of_quarter_num
    , date.trade_week_overall_num
    , date.trade_week_start_dt
    , date.trade_week_start_key
    , date.trade_week_end_dt
    , date.trade_week_end_key
    , date.trade_month_445_num
    , date.trade_month_454_num
    , date.trade_month_544_num
    , date.trade_month_445_nm
    , date.trade_month_454_nm
    , date.trade_month_544_nm
    , date.trade_month_abbr
    , date.trade_month_overall_num
    , date.trade_yearmonth_num
    , date.trade_month_445_start_dt
    , date.trade_month_445_start_key
    , date.trade_month_445_end_dt
    , date.trade_month_445_end_key
    , date.trade_month_454_start_dt
    , date.trade_month_454_start_key
    , date.trade_month_454_end_dt
    , date.trade_month_454_end_key
    , date.trade_month_544_start_dt
    , date.trade_month_544_start_key
    , date.trade_month_544_end_dt
    , date.trade_month_544_end_key
    , date.trade_quarter_num
    , date.trade_quarter_nm
    , date.trade_quarter_full_nm
    , date.trade_quarter_start_dt
    , date.trade_quarter_start_key
    , date.trade_quarter_end_dt
    , date.trade_quarter_end_key
    , date.trade_year_num
    , date.trade_year_start_dt
    , date.trade_year_start_key
    , date.trade_year_end_dt
    , date.trade_year_end_key
    , date.trade_leap_week_flg
    , date.weeks_in_trade_year_num
    , date.dw_synced_ts
    , date.dw_source_nm
    , date.create_user_id
    , date.create_ts
from
    date)
, trade_week_base as (
    select
        trade_year_num * 100 + trade_week_num as trade_week_key
        , trade_week_start_key  -- alternate KEY
        , trade_year_num
        , trade_week_num
        , min(full_dt) as trade_week_start_dt
--        , trade_week_start_key
        , max(full_dt) as trade_week_end_dt
        , trade_week_end_key
        , max(trade_week_overall_num) as trade_week_overall_num
        , max(trade_week_of_quarter_num) as trade_week_of_quarter_num  -- NEW
        , max(trade_week_of_month_445_num) as trade_week_of_month_445_num  -- NEW
        , max(trade_week_of_month_454_num) as trade_week_of_month_454_num  -- NEW
        , max(trade_week_of_month_544_num) as trade_week_of_month_544_num  -- NEW
        , max(trade_quarter_num) as trade_quarter_num
        , max(trade_quarter_nm) as trade_quarter_nm
        , max(trade_quarter_full_nm) as trade_quarter_full_nm
        , min(trade_year_start_dt) as trade_year_start_dt  -- NEW
        , min(trade_year_start_key) as trade_year_start_key  -- NEW
        , max(trade_year_end_dt) as trade_year_end_dt  -- NEW
        , max(trade_year_end_key) as trade_year_end_key  -- NEW
        , max(trade_leap_week_flg) as trade_leap_week_flg
        , max(weeks_in_trade_year_num) as weeks_in_trade_year_num
        , count(*) as days_in_week
        , 'W' || lpad(trade_week_num::varchar, 2, '0') as trade_week_label
        , trade_year_num::varchar || '-W' || lpad(trade_week_num::varchar, 2, '0') as trade_week_full_label
        , lag(trade_week_key) over (order by trade_week_key) as prior_trade_week_key
        , lead(trade_week_key) over (order by trade_week_key) as next_trade_week_key
        , lag(trade_week_key, 52) over (order by trade_week_key) as trade_week_last_year_key
    from all_cols
    where date_key > 0  -- Exclude special records
    group by
        trade_week_start_key
        , trade_year_num
        , trade_week_num
        , trade_week_end_key)
, special_records as (
    select * from (values
        (
            -1                      -- trade_week_key
            , -1                    -- trade_week_start_key (alternate key)
            , -1                    -- trade_year_num
            , -1                    -- trade_week_num
            , '1900-01-01'::date    -- trade_week_start_dt
            , '1900-01-07'::date    -- trade_week_end_dt
            , -1                    -- trade_week_end_key
            , -1                    -- trade_week_overall_num
            , -1                    -- trade_week_of_quarter_num
            , -1                    -- trade_week_of_month_445_num
            , -1                    -- trade_week_of_month_454_num
            , -1                    -- trade_week_of_month_544_num
            , -1                    -- trade_quarter_num
            , 'UNK'                 -- trade_quarter_nm
            , 'Unknown'             -- trade_quarter_full_nm
            , '1900-01-01'::date    -- trade_year_start_dt
            , -1                    -- trade_year_start_key
            , '1900-12-31'::date    -- trade_year_end_dt
            , -1                    -- trade_year_end_key
            , 0                     -- trade_leap_week_flg
            , -1                    -- weeks_in_trade_year_num
            , 0                     -- days_in_week
            , 'UNK'                 -- trade_week_label
            , 'Unknown'             -- trade_week_full_label
            , null                  -- prior_trade_week_key
            , null                  -- next_trade_week_key
            , null                  -- trade_week_last_year_key
        )
    ) as t (
        trade_week_key
        , trade_week_start_key
        , trade_year_num
        , trade_week_num
        , trade_week_start_dt
        , trade_week_end_dt
        , trade_week_end_key
        , trade_week_overall_num
        , trade_week_of_quarter_num
        , trade_week_of_month_445_num
        , trade_week_of_month_454_num
        , trade_week_of_month_544_num
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        , trade_year_start_dt
        , trade_year_start_key
        , trade_year_end_dt
        , trade_year_end_key
        , trade_leap_week_flg
        , weeks_in_trade_year_num
        , days_in_week
        , trade_week_label
        , trade_week_full_label
        , prior_trade_week_key
        , next_trade_week_key
        , trade_week_last_year_key
    )
)
, final as (
select * from special_records
union all
select * from trade_week_base
)
select * from final
