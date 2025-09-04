{{ config(materialized='table') }}
{{ config( post_hook="alter table {{ this }} add primary key (trade_quarter_key)", ) }}
with trade_months as (select * from {{ ref('dim_trade_month') }}
 where trade_month_key > 0)  -- Exclude special records
, quarter_aggregated as (
    select
        -- Primary key: YYYYQQ format (e.g., 202301 for 2023 Q1)
        trade_year_num * 100 + trade_quarter_num as trade_quarter_key
        -- Core identifiers
        , trade_year_num
        , trade_quarter_num
        , max(trade_quarter_nm) as trade_quarter_nm
        , max(trade_quarter_full_nm) as trade_quarter_full_nm
        -- Quarter boundaries
        , min(trade_month_start_dt) as trade_quarter_start_dt
        , max(trade_month_end_dt) as trade_quarter_end_dt
        , min(trade_month_start_key) as trade_quarter_start_key
        , max(trade_month_end_key) as trade_quarter_end_key
        -- Month metrics
        , min(trade_month_num) as first_month_of_quarter_num
        , max(trade_month_num) as last_month_of_quarter_num
        , count(distinct trade_month_num) as months_in_quarter_num
        -- Week metrics (summed from months)
        , sum(weeks_in_month_num) as weeks_in_quarter_num
        , min(first_week_of_month_num) as first_week_of_quarter_num
        , max(last_week_of_month_num) as last_week_of_quarter_num
        -- Day metrics
        , sum(days_in_month_num) as days_in_quarter_num
        -- Pattern-specific week counts (all patterns have same quarterly structure)
        , sum(trade_weeks_in_month_445_num) as trade_weeks_in_quarter_445_num
        , sum(trade_weeks_in_month_454_num) as trade_weeks_in_quarter_454_num
        , sum(trade_weeks_in_month_544_num) as trade_weeks_in_quarter_544_num
        -- Leap week flag
        , max(contains_leap_week_flg) as contains_leap_week_flg
        -- Year boundaries (for context)
        , min(trade_year_start_dt) as trade_year_start_dt
        , min(trade_year_start_key) as trade_year_start_key
        , max(trade_year_end_dt) as trade_year_end_dt
        , max(trade_year_end_key) as trade_year_end_key
        -- Metadata
        , max(dw_synced_ts) as dw_synced_ts
        , max(dw_source_nm) as dw_source_nm
        , max(create_user_id) as create_user_id
        , max(create_ts) as create_ts
    from trade_months
    group by
        trade_year_num
        , trade_quarter_num)
, regular_records as (
    select
        trade_quarter_key
        , trade_year_num
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        -- Quarter boundaries
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
        , trade_weeks_in_quarter_445_num
        , trade_weeks_in_quarter_454_num
        , trade_weeks_in_quarter_544_num
        -- Day metrics
        , days_in_quarter_num
        -- Display formats
        , 'Q' || trade_quarter_num::varchar as trade_quarter_txt
        , trade_quarter_nm || ' ' || trade_year_num::varchar as trade_quarter_year_nm
        , 'TY' || trade_year_num::varchar || '-Q' || trade_quarter_num::varchar as trade_year_quarter_txt
        , trade_year_num::varchar || '-Q' || trade_quarter_num::varchar as year_quarter_txt
        , trade_quarter_nm || ' Quarter' as trade_quarter_position_txt
        -- Leap week flag
        , contains_leap_week_flg
        -- Year context
        , trade_year_start_dt
        , trade_year_start_key
        , trade_year_end_dt
        , trade_year_end_key
        -- Overall numbering
        , (trade_year_num - 2000) * 4 + trade_quarter_num as trade_quarter_overall_num
        -- Navigation keys
        , lag(trade_quarter_key) over (order by trade_quarter_key) as prior_trade_quarter_key
        , lead(trade_quarter_key) over (order by trade_quarter_key) as next_trade_quarter_key
        , lag(trade_quarter_key, 4) over (order by trade_quarter_key) as trade_quarter_last_year_key
        -- Metadata
        , dw_synced_ts
        , 'TRADE_CALENDAR' as dw_source_nm
        , create_user_id
        , create_ts
    from quarter_aggregated)
, special_records as (
    select * from (values
        (
            -1                      -- trade_quarter_key
            , -1                    -- trade_year_num
            , -1                    -- trade_quarter_num
            , 'UNK'                 -- trade_quarter_nm
            , 'Unknown'             -- trade_quarter_full_nm
            , '1900-01-01'::date    -- trade_quarter_start_dt
            , '1900-01-01'::date    -- trade_quarter_end_dt
            , -1                    -- trade_quarter_start_key
            , -1                    -- trade_quarter_end_key
            , -1                    -- first_month_of_quarter_num
            , -1                    -- last_month_of_quarter_num
            , -1                    -- months_in_quarter_num
            , -1                    -- weeks_in_quarter_num
            , -1                    -- first_week_of_quarter_num
            , -1                    -- last_week_of_quarter_num
            , -1                    -- trade_weeks_in_quarter_445_num
            , -1                    -- trade_weeks_in_quarter_454_num
            , -1                    -- trade_weeks_in_quarter_544_num
            , -1                    -- days_in_quarter_num
            , 'UNK'                 -- trade_quarter_txt
            , 'Unknown'             -- trade_quarter_year_nm
            , 'Unknown'             -- trade_year_quarter_txt
            , 'Unknown'             -- year_quarter_txt
            , 'Unknown'             -- trade_quarter_position_txt
            , 0                     -- contains_leap_week_flg
            , '1900-01-01'::date    -- trade_year_start_dt
            , -1                    -- trade_year_start_key
            , '1900-01-01'::date    -- trade_year_end_dt
            , -1                    -- trade_year_end_key
            , -1                    -- trade_quarter_overall_num
            , null                  -- prior_trade_quarter_key
            , null                  -- next_trade_quarter_key
            , null                  -- trade_quarter_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -2                      -- trade_quarter_key
            , -2                    -- trade_year_num
            , -2                    -- trade_quarter_num
            , 'INV'                 -- trade_quarter_nm
            , 'Invalid'             -- trade_quarter_full_nm
            , '1900-01-02'::date    -- trade_quarter_start_dt
            , '1900-01-02'::date    -- trade_quarter_end_dt
            , -2                    -- trade_quarter_start_key
            , -2                    -- trade_quarter_end_key
            , -2                    -- first_month_of_quarter_num
            , -2                    -- last_month_of_quarter_num
            , -2                    -- months_in_quarter_num
            , -2                    -- weeks_in_quarter_num
            , -2                    -- first_week_of_quarter_num
            , -2                    -- last_week_of_quarter_num
            , -2                    -- trade_weeks_in_quarter_445_num
            , -2                    -- trade_weeks_in_quarter_454_num
            , -2                    -- trade_weeks_in_quarter_544_num
            , -2                    -- days_in_quarter_num
            , 'INV'                 -- trade_quarter_txt
            , 'Invalid'             -- trade_quarter_year_nm
            , 'Invalid'             -- trade_year_quarter_txt
            , 'Invalid'             -- year_quarter_txt
            , 'Invalid'             -- trade_quarter_position_txt
            , 0                     -- contains_leap_week_flg
            , '1900-01-02'::date    -- trade_year_start_dt
            , -2                    -- trade_year_start_key
            , '1900-01-02'::date    -- trade_year_end_dt
            , -2                    -- trade_year_end_key
            , -2                    -- trade_quarter_overall_num
            , null                  -- prior_trade_quarter_key
            , null                  -- next_trade_quarter_key
            , null                  -- trade_quarter_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -3                      -- trade_quarter_key
            , -3                    -- trade_year_num
            , -3                    -- trade_quarter_num
            , 'N/A'                 -- trade_quarter_nm
            , 'Not Applicable'      -- trade_quarter_full_nm
            , '1900-01-03'::date    -- trade_quarter_start_dt
            , '1900-01-03'::date    -- trade_quarter_end_dt
            , -3                    -- trade_quarter_start_key
            , -3                    -- trade_quarter_end_key
            , -3                    -- first_month_of_quarter_num
            , -3                    -- last_month_of_quarter_num
            , -3                    -- months_in_quarter_num
            , -3                    -- weeks_in_quarter_num
            , -3                    -- first_week_of_quarter_num
            , -3                    -- last_week_of_quarter_num
            , -3                    -- trade_weeks_in_quarter_445_num
            , -3                    -- trade_weeks_in_quarter_454_num
            , -3                    -- trade_weeks_in_quarter_544_num
            , -3                    -- days_in_quarter_num
            , 'N/A'                 -- trade_quarter_txt
            , 'Not Applicable'      -- trade_quarter_year_nm
            , 'Not Applicable'      -- trade_year_quarter_txt
            , 'Not Applicable'      -- year_quarter_txt
            , 'Not Applicable'      -- trade_quarter_position_txt
            , 0                     -- contains_leap_week_flg
            , '1900-01-03'::date    -- trade_year_start_dt
            , -3                    -- trade_year_start_key
            , '1900-01-03'::date    -- trade_year_end_dt
            , -3                    -- trade_year_end_key
            , -3                    -- trade_quarter_overall_num
            , null                  -- prior_trade_quarter_key
            , null                  -- next_trade_quarter_key
            , null                  -- trade_quarter_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
    ) as t (
        trade_quarter_key
        , trade_year_num
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        , trade_quarter_start_dt
        , trade_quarter_end_dt
        , trade_quarter_start_key
        , trade_quarter_end_key
        , first_month_of_quarter_num
        , last_month_of_quarter_num
        , months_in_quarter_num
        , weeks_in_quarter_num
        , first_week_of_quarter_num
        , last_week_of_quarter_num
        , trade_weeks_in_quarter_445_num
        , trade_weeks_in_quarter_454_num
        , trade_weeks_in_quarter_544_num
        , days_in_quarter_num
        , trade_quarter_txt
        , trade_quarter_year_nm
        , trade_year_quarter_txt
        , year_quarter_txt
        , trade_quarter_position_txt
        , contains_leap_week_flg
        , trade_year_start_dt
        , trade_year_start_key
        , trade_year_end_dt
        , trade_year_end_key
        , trade_quarter_overall_num
        , prior_trade_quarter_key
        , next_trade_quarter_key
        , trade_quarter_last_year_key
        , dw_synced_ts
        , dw_source_nm
        , create_user_id
        , create_ts
    )
)
, final as (
    select * from special_records
    union all
    select * from regular_records
)
select * from final
