{{ config(materialized='table') }}
{{ config( post_hook="alter table {{ this }} add primary key (trade_week_key)", ) }}
with date as (
    select * from {{ ref('dim_trade_date') }}
    where date_key > 0
)
, trade_week_base as (
    select
        -- Primary key as YYYYWW format
        trade_year_num * 100 + trade_week_num as trade_week_key
        , trade_week_start_key  -- Alternate key
        , trade_year_num
        , trade_week_num
        , min(full_dt) as trade_week_start_dt
        , max(full_dt) as trade_week_end_dt
        , max(trade_week_end_key) as trade_week_end_key
        -- Week position metrics
        , max(trade_week_overall_num) as trade_week_overall_num
        , max(trade_week_of_quarter_num) as trade_week_of_quarter_num
        , max(trade_week_of_month_445_num) as trade_week_of_month_445_num
        , max(trade_week_of_month_454_num) as trade_week_of_month_454_num
        , max(trade_week_of_month_544_num) as trade_week_of_month_544_num
        -- Month attributes for all three patterns
        , max(trade_month_445_num) as trade_month_445_num
        , max(trade_month_454_num) as trade_month_454_num
        , max(trade_month_544_num) as trade_month_544_num
        , max(trade_month_445_nm) as trade_month_445_nm
        , max(trade_month_454_nm) as trade_month_454_nm
        , max(trade_month_544_nm) as trade_month_544_nm
        , max(trade_month_abbr) as trade_month_abbr
        , max(trade_month_overall_num) as trade_month_overall_num
        , max(trade_yearmonth_num) as trade_yearmonth_num
        -- Month boundaries for all patterns
        , min(trade_month_445_start_dt) as trade_month_445_start_dt
        , min(trade_month_445_start_key) as trade_month_445_start_key
        , max(trade_month_445_end_dt) as trade_month_445_end_dt
        , max(trade_month_445_end_key) as trade_month_445_end_key
        , min(trade_month_454_start_dt) as trade_month_454_start_dt
        , min(trade_month_454_start_key) as trade_month_454_start_key
        , max(trade_month_454_end_dt) as trade_month_454_end_dt
        , max(trade_month_454_end_key) as trade_month_454_end_key
        , min(trade_month_544_start_dt) as trade_month_544_start_dt
        , min(trade_month_544_start_key) as trade_month_544_start_key
        , max(trade_month_544_end_dt) as trade_month_544_end_dt
        , max(trade_month_544_end_key) as trade_month_544_end_key
        -- Quarter attributes
        , max(trade_quarter_num) as trade_quarter_num
        , max(trade_quarter_nm) as trade_quarter_nm
        , max(trade_quarter_full_nm) as trade_quarter_full_nm
        , min(trade_quarter_start_dt) as trade_quarter_start_dt
        , min(trade_quarter_start_key) as trade_quarter_start_key
        , max(trade_quarter_end_dt) as trade_quarter_end_dt
        , max(trade_quarter_end_key) as trade_quarter_end_key
        -- Year attributes
        , min(trade_year_start_dt) as trade_year_start_dt
        , min(trade_year_start_key) as trade_year_start_key
        , max(trade_year_end_dt) as trade_year_end_dt
        , max(trade_year_end_key) as trade_year_end_key
        , max(trade_leap_week_flg) as trade_leap_week_flg
        , max(weeks_in_trade_year_num) as weeks_in_trade_year_num
        -- Week metrics
        , count(*) as days_in_week
        -- Labels
        , 'W' || lpad(trade_week_num::varchar, 2, '0') as trade_week_label
        , trade_year_num::varchar || '-W' || lpad(trade_week_num::varchar, 2, '0') as trade_week_full_label
        -- Navigation keys
        , lag(trade_year_num * 100 + trade_week_num)
            over (order by trade_year_num, trade_week_num)
            as prior_trade_week_key
        , lead(trade_year_num * 100 + trade_week_num)
            over (order by trade_year_num, trade_week_num)
            as next_trade_week_key
        , lag(trade_year_num * 100 + trade_week_num, 52)
            over (order by trade_year_num, trade_week_num)
            as trade_week_last_year_key
        -- Metadata
        , max(dw_synced_ts) as dw_synced_ts
        , max(dw_source_nm) as dw_source_nm
        , max(create_user_id) as create_user_id
        , max(create_ts) as create_ts
    from date
    group by
        trade_week_start_key
        , trade_year_num
        , trade_week_num
)
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
            , -1                    -- trade_month_445_num
            , -1                    -- trade_month_454_num
            , -1                    -- trade_month_544_num
            , 'Unknown'             -- trade_month_445_nm
            , 'Unknown'             -- trade_month_454_nm
            , 'Unknown'             -- trade_month_544_nm
            , 'UNK'                 -- trade_month_abbr
            , -1                    -- trade_month_overall_num
            , -1                    -- trade_yearmonth_num
            , '1900-01-01'::date    -- trade_month_445_start_dt
            , -1                    -- trade_month_445_start_key
            , '1900-01-01'::date    -- trade_month_445_end_dt
            , -1                    -- trade_month_445_end_key
            , '1900-01-01'::date    -- trade_month_454_start_dt
            , -1                    -- trade_month_454_start_key
            , '1900-01-01'::date    -- trade_month_454_end_dt
            , -1                    -- trade_month_454_end_key
            , '1900-01-01'::date    -- trade_month_544_start_dt
            , -1                    -- trade_month_544_start_key
            , '1900-01-01'::date    -- trade_month_544_end_dt
            , -1                    -- trade_month_544_end_key
            , -1                    -- trade_quarter_num
            , 'UNK'                 -- trade_quarter_nm
            , 'Unknown'             -- trade_quarter_full_nm
            , '1900-01-01'::date    -- trade_quarter_start_dt
            , -1                    -- trade_quarter_start_key
            , '1900-01-01'::date    -- trade_quarter_end_dt
            , -1                    -- trade_quarter_end_key
            , '1900-01-01'::date    -- trade_year_start_dt
            , -1                    -- trade_year_start_key
            , '1900-01-01'::date    -- trade_year_end_dt
            , -1                    -- trade_year_end_key
            , 0                     -- trade_leap_week_flg
            , -1                    -- weeks_in_trade_year_num
            , 7                     -- days_in_week
            , 'UNK'                 -- trade_week_label
            , 'Unknown'             -- trade_week_full_label
            , null                  -- prior_trade_week_key
            , null                  -- next_trade_week_key
            , null                  -- trade_week_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -2                      -- trade_week_key
            , -2                    -- trade_week_start_key (alternate key)
            , -2                    -- trade_year_num
            , -2                    -- trade_week_num
            , '1900-01-02'::date    -- trade_week_start_dt
            , '1900-01-08'::date    -- trade_week_end_dt
            , -2                    -- trade_week_end_key
            , -2                    -- trade_week_overall_num
            , -2                    -- trade_week_of_quarter_num
            , -2                    -- trade_week_of_month_445_num
            , -2                    -- trade_week_of_month_454_num
            , -2                    -- trade_week_of_month_544_num
            , -2                    -- trade_month_445_num
            , -2                    -- trade_month_454_num
            , -2                    -- trade_month_544_num
            , 'Invalid'             -- trade_month_445_nm
            , 'Invalid'             -- trade_month_454_nm
            , 'Invalid'             -- trade_month_544_nm
            , 'INV'                 -- trade_month_abbr
            , -2                    -- trade_month_overall_num
            , -2                    -- trade_yearmonth_num
            , '1900-01-02'::date    -- trade_month_445_start_dt
            , -2                    -- trade_month_445_start_key
            , '1900-01-02'::date    -- trade_month_445_end_dt
            , -2                    -- trade_month_445_end_key
            , '1900-01-02'::date    -- trade_month_454_start_dt
            , -2                    -- trade_month_454_start_key
            , '1900-01-02'::date    -- trade_month_454_end_dt
            , -2                    -- trade_month_454_end_key
            , '1900-01-02'::date    -- trade_month_544_start_dt
            , -2                    -- trade_month_544_start_key
            , '1900-01-02'::date    -- trade_month_544_end_dt
            , -2                    -- trade_month_544_end_key
            , -2                    -- trade_quarter_num
            , 'INV'                 -- trade_quarter_nm
            , 'Invalid'             -- trade_quarter_full_nm
            , '1900-01-02'::date    -- trade_quarter_start_dt
            , -2                    -- trade_quarter_start_key
            , '1900-01-02'::date    -- trade_quarter_end_dt
            , -2                    -- trade_quarter_end_key
            , '1900-01-02'::date    -- trade_year_start_dt
            , -2                    -- trade_year_start_key
            , '1900-01-02'::date    -- trade_year_end_dt
            , -2                    -- trade_year_end_key
            , 0                     -- trade_leap_week_flg
            , -2                    -- weeks_in_trade_year_num
            , 7                     -- days_in_week
            , 'INV'                 -- trade_week_label
            , 'Invalid'             -- trade_week_full_label
            , null                  -- prior_trade_week_key
            , null                  -- next_trade_week_key
            , null                  -- trade_week_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -3                      -- trade_week_key
            , -3                    -- trade_week_start_key (alternate key)
            , -3                    -- trade_year_num
            , -3                    -- trade_week_num
            , '1900-01-03'::date    -- trade_week_start_dt
            , '1900-01-09'::date    -- trade_week_end_dt
            , -3                    -- trade_week_end_key
            , -3                    -- trade_week_overall_num
            , -3                    -- trade_week_of_quarter_num
            , -3                    -- trade_week_of_month_445_num
            , -3                    -- trade_week_of_month_454_num
            , -3                    -- trade_week_of_month_544_num
            , -3                    -- trade_month_445_num
            , -3                    -- trade_month_454_num
            , -3                    -- trade_month_544_num
            , 'Not Applicable'      -- trade_month_445_nm
            , 'Not Applicable'      -- trade_month_454_nm
            , 'Not Applicable'      -- trade_month_544_nm
            , 'N/A'                 -- trade_month_abbr
            , -3                    -- trade_month_overall_num
            , -3                    -- trade_yearmonth_num
            , '1900-01-03'::date    -- trade_month_445_start_dt
            , -3                    -- trade_month_445_start_key
            , '1900-01-03'::date    -- trade_month_445_end_dt
            , -3                    -- trade_month_445_end_key
            , '1900-01-03'::date    -- trade_month_454_start_dt
            , -3                    -- trade_month_454_start_key
            , '1900-01-03'::date    -- trade_month_454_end_dt
            , -3                    -- trade_month_454_end_key
            , '1900-01-03'::date    -- trade_month_544_start_dt
            , -3                    -- trade_month_544_start_key
            , '1900-01-03'::date    -- trade_month_544_end_dt
            , -3                    -- trade_month_544_end_key
            , -3                    -- trade_quarter_num
            , 'N/A'                 -- trade_quarter_nm
            , 'Not Applicable'      -- trade_quarter_full_nm
            , '1900-01-03'::date    -- trade_quarter_start_dt
            , -3                    -- trade_quarter_start_key
            , '1900-01-03'::date    -- trade_quarter_end_dt
            , -3                    -- trade_quarter_end_key
            , '1900-01-03'::date    -- trade_year_start_dt
            , -3                    -- trade_year_start_key
            , '1900-01-03'::date    -- trade_year_end_dt
            , -3                    -- trade_year_end_key
            , 0                     -- trade_leap_week_flg
            , -3                    -- weeks_in_trade_year_num
            , 7                     -- days_in_week
            , 'N/A'                 -- trade_week_label
            , 'Not Applicable'      -- trade_week_full_label
            , null                  -- prior_trade_week_key
            , null                  -- next_trade_week_key
            , null                  -- trade_week_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
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
        , dw_synced_ts
        , dw_source_nm
        , create_user_id
        , create_ts
    )
)
, final as (
    select * from special_records
    union all
    select * from trade_week_base
)
select * from final
