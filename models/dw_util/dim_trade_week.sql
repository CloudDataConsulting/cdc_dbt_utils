{{ config(materialized='table') }}

with trade_weeks_base as (
    select distinct
        trade_week_start_key as trade_week_key  -- PRIMARY KEY
        , trade_year_num
        , trade_week_num
        , trade_year_num * 100 + trade_week_num as trade_year_week_num  -- 202301
        , trade_week_start_dt
        , trade_week_start_key
        , trade_week_end_dt
        , trade_week_end_key
        , trade_week_overall_num
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        , trade_leap_week_flg
        , weeks_in_trade_year_num
        -- Week labels
        , 'W' || lpad(trade_week_num::varchar, 2, '0') as trade_week_label  -- W01
        , trade_year_num::varchar || '-W' || lpad(trade_week_num::varchar, 2, '0') as trade_week_full_label  -- 2023-W01
        -- Counts
        , 7 as days_in_week  -- Always 7 for complete weeks
        -- Relative weeks
        , lag(trade_week_start_key) over (order by trade_week_start_key) as prior_trade_week_key
        , lead(trade_week_start_key) over (order by trade_week_start_key) as next_trade_week_key
        , lag(trade_week_start_key, 52) over (order by trade_week_start_key) as trade_week_last_year_key
    from {{ ref('dim_trade_date') }}
    where date_key > 0  -- Exclude special records
)
, special_records as (
    select * from (values
        (
            -1                      -- trade_week_key
            , -1                    -- trade_year_num
            , -1                    -- trade_week_num
            , -1                    -- trade_year_week_num
            , '1900-01-01'::date    -- trade_week_start_dt
            , -1                    -- trade_week_start_key
            , '1900-01-01'::date    -- trade_week_end_dt
            , -1                    -- trade_week_end_key
            , -1                    -- trade_week_overall_num
            , -1                    -- trade_quarter_num
            , 'UNK'                 -- trade_quarter_nm
            , 'Unknown'             -- trade_quarter_full_nm
            , 0                     -- trade_leap_week_flg
            , -1                    -- weeks_in_trade_year_num
            , 'UNK'                 -- trade_week_label
            , 'Unknown'             -- trade_week_full_label
            , 0                     -- days_in_week
            , -1                    -- prior_trade_week_key
            , -1                    -- next_trade_week_key
            , -1                    -- trade_week_last_year_key
        )
        -- Similar records for -2 (Invalid) and -3 (Not Applicable)
    ) as t (
        trade_week_key
        , trade_year_num
        , trade_week_num
        , trade_year_week_num
        , trade_week_start_dt
        , trade_week_start_key
        , trade_week_end_dt
        , trade_week_end_key
        , trade_week_overall_num
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        , trade_leap_week_flg
        , weeks_in_trade_year_num
        , trade_week_label
        , trade_week_full_label
        , days_in_week
        , prior_trade_week_key
        , next_trade_week_key
        , trade_week_last_year_key
    )
)
select * from trade_weeks_base
union all
select * from special_records
