{{ config(materialized='table') }}
{{ config( post_hook="alter table {{ this }} add primary key (trade_month_key)", ) }}
with trade_weeks as (
    select * from {{ ref('dim_trade_week') }}
    where trade_week_key > 0  -- Exclude special records
)
, month_aggregated as (
    select
        -- Primary key: YYYYMM format
        trade_year_num * 100 + trade_month_445_num as trade_month_key
        -- Core identifiers (all patterns have same month numbers)
        , trade_year_num
        , trade_month_445_num as trade_month_num
        , max(trade_month_445_nm) as trade_month_nm
        , max(trade_month_abbr) as trade_month_abbr
        -- Quarter information
        , max(trade_quarter_num) as trade_quarter_num
        , max(trade_quarter_nm) as trade_quarter_nm
        , max(trade_quarter_full_nm) as trade_quarter_full_nm
        -- Month boundaries (same for all patterns - based on week boundaries)
        , min(trade_week_start_dt) as trade_month_start_dt
        , max(trade_week_end_dt) as trade_month_end_dt
        , min(trade_week_start_key) as trade_month_start_key
        , max(trade_week_end_key) as trade_month_end_key
        -- Week metrics
        , min(trade_week_num) as first_week_of_month_num
        , max(trade_week_num) as last_week_of_month_num
        , count(distinct trade_week_num) as weeks_in_month_num
        , sum(days_in_week) as days_in_month_num
        -- Pattern-specific week counts (from the week dimension)
        , max(trade_week_of_month_445_num) as trade_weeks_in_month_445_num
        , max(case when trade_week_of_month_445_num = 5 then 1 else 0 end) as is_5_week_month_445_flg
        , max(trade_week_of_month_454_num) as trade_weeks_in_month_454_num
        , max(case when trade_week_of_month_454_num = 5 then 1 else 0 end) as is_5_week_month_454_flg
        , max(trade_week_of_month_544_num) as trade_weeks_in_month_544_num
        , max(case when trade_week_of_month_544_num = 5 then 1 else 0 end) as is_5_week_month_544_flg
        -- Leap week flag
        , max(trade_leap_week_flg) as contains_leap_week_flg
        -- Year and quarter counts
        , max(weeks_in_trade_year_num) as weeks_in_trade_year_num
        , max(days_in_trade_year_num) as days_in_trade_year_num
        , max(weeks_in_trade_quarter_num) as weeks_in_trade_quarter_num
        , max(days_in_trade_quarter_num) as days_in_trade_quarter_num
        -- Year boundaries (for context)
        , min(trade_year_start_dt) as trade_year_start_dt
        , min(trade_year_start_key) as trade_year_start_key
        , max(trade_year_end_dt) as trade_year_end_dt
        , max(trade_year_end_key) as trade_year_end_key
        -- Quarter boundaries
        , min(trade_quarter_start_dt) as trade_quarter_start_dt
        , min(trade_quarter_start_key) as trade_quarter_start_key
        , max(trade_quarter_end_dt) as trade_quarter_end_dt
        , max(trade_quarter_end_key) as trade_quarter_end_key
        -- Metadata
        , max(dw_synced_ts) as dw_synced_ts
        , max(dw_source_nm) as dw_source_nm
        , max(create_user_id) as create_user_id
        , max(create_ts) as create_ts
    from trade_weeks
    group by
        trade_year_num
        , trade_month_445_num
)
, month_with_navigation as (
    select
        m.*
        -- Navigation keys
        , lag(m.trade_month_key) over (order by m.trade_month_key) as prior_trade_month_key
        , lead(m.trade_month_key) over (order by m.trade_month_key) as next_trade_month_key
    from month_aggregated m
)
, month_with_yoy as (
    select
        m.*
        -- Year-over-year comparison keys
        -- For months containing week 53, we need special handling
        , case
            when m.contains_leap_week_flg = 1 then
                -- December of 53-week year maps to December of prior year
                ly.trade_month_key
            when lym.trade_month_key is not null then
                lym.trade_month_key
            else null
        end as trade_month_last_year_nrf_key
        -- Walmart method would keep same month mapping
        , lym.trade_month_key as trade_month_last_year_walmart_key
        -- 12-month back method
        , lag(m.trade_month_key, 12) over (order by m.trade_month_key) as trade_month_last_year_12m_key
    from month_with_navigation m
    -- Standard join to prior year same month
    left join month_with_navigation lym
        on lym.trade_year_num = m.trade_year_num - 1
        and lym.trade_month_num = m.trade_month_num
    -- For NRF method when month contains leap week
    left join month_with_navigation ly
        on ly.trade_year_num = m.trade_year_num - 1
        and ly.trade_month_num = 12  -- December
)
, regular_records as (
    select
        trade_month_key
        , trade_year_num
        , trade_month_num
        , trade_month_nm
        , trade_month_abbr
        -- Quarter
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        , trade_quarter_start_dt
        , trade_quarter_start_key
        , trade_quarter_end_dt
        , trade_quarter_end_key
        -- Month boundaries
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
        -- Week/day metrics
        , first_week_of_month_num
        , last_week_of_month_num
        , weeks_in_month_num
        , days_in_month_num
        , contains_leap_week_flg
        -- Year and quarter counts
        , weeks_in_trade_year_num
        , days_in_trade_year_num
        , weeks_in_trade_quarter_num
        , days_in_trade_quarter_num
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
        , trade_month_abbr || ' ' || trade_year_num::varchar as trade_month_year_abbr
        -- Year context
        , trade_year_start_dt
        , trade_year_start_key
        , trade_year_end_dt
        , trade_year_end_key
        -- Overall numbering
        , (trade_year_num - 2000) * 12 + trade_month_num as trade_month_overall_num
        -- Navigation keys
        , prior_trade_month_key
        , next_trade_month_key
        , trade_month_last_year_nrf_key
        , trade_month_last_year_walmart_key
        , trade_month_last_year_12m_key
        -- Metadata
        , dw_synced_ts
        , 'TRADE_CALENDAR' as dw_source_nm
        , create_user_id
        , create_ts
    from month_with_yoy
)
, special_records as (
    select * from (values
        (
            -1                      -- trade_month_key
            , -1                    -- trade_year_num
            , -1                    -- trade_month_num
            , 'Unknown'             -- trade_month_nm
            , 'UNK'                 -- trade_month_abbr
            , -1                    -- trade_quarter_num
            , 'UNK'                 -- trade_quarter_nm
            , 'Unknown'             -- trade_quarter_full_nm
            , '1900-01-01'::date    -- trade_quarter_start_dt
            , -1                    -- trade_quarter_start_key
            , '1900-01-01'::date    -- trade_quarter_end_dt
            , -1                    -- trade_quarter_end_key
            , '1900-01-01'::date    -- trade_month_start_dt
            , '1900-01-01'::date    -- trade_month_end_dt
            , -1                    -- trade_month_start_key
            , -1                    -- trade_month_end_key
            , -1                    -- trade_month_in_quarter_num
            , -1                    -- first_week_of_month_num
            , -1                    -- last_week_of_month_num
            , -1                    -- weeks_in_month_num
            , -1                    -- days_in_month_num
            , 0                     -- contains_leap_week_flg
            , -1                    -- weeks_in_trade_year_num
            , -1                    -- days_in_trade_year_num
            , -1                    -- weeks_in_trade_quarter_num
            , -1                    -- days_in_trade_quarter_num
            , -1                    -- trade_weeks_in_month_445_num
            , 0                     -- is_5_week_month_445_flg
            , -1                    -- trade_weeks_in_month_454_num
            , 0                     -- is_5_week_month_454_flg
            , -1                    -- trade_weeks_in_month_544_num
            , 0                     -- is_5_week_month_544_flg
            , 'Unknown'             -- trade_month_year_nm
            , 'Unknown'             -- trade_year_month_txt
            , 'UNK'                 -- trade_month_year_abbr
            , '1900-01-01'::date    -- trade_year_start_dt
            , -1                    -- trade_year_start_key
            , '1900-01-01'::date    -- trade_year_end_dt
            , -1                    -- trade_year_end_key
            , -1                    -- trade_month_overall_num
            , null                  -- prior_trade_month_key
            , null                  -- next_trade_month_key
            , -1                    -- trade_month_last_year_nrf_key
            , -1                    -- trade_month_last_year_walmart_key
            , -1                    -- trade_month_last_year_12m_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -2                      -- trade_month_key
            , -2                    -- trade_year_num
            , -2                    -- trade_month_num
            , 'Invalid'             -- trade_month_nm
            , 'INV'                 -- trade_month_abbr
            , -2                    -- trade_quarter_num
            , 'INV'                 -- trade_quarter_nm
            , 'Invalid'             -- trade_quarter_full_nm
            , '1900-01-02'::date    -- trade_quarter_start_dt
            , -2                    -- trade_quarter_start_key
            , '1900-01-02'::date    -- trade_quarter_end_dt
            , -2                    -- trade_quarter_end_key
            , '1900-01-02'::date    -- trade_month_start_dt
            , '1900-01-02'::date    -- trade_month_end_dt
            , -2                    -- trade_month_start_key
            , -2                    -- trade_month_end_key
            , -2                    -- trade_month_in_quarter_num
            , -2                    -- first_week_of_month_num
            , -2                    -- last_week_of_month_num
            , -2                    -- weeks_in_month_num
            , -2                    -- days_in_month_num
            , 0                     -- contains_leap_week_flg
            , -2                    -- weeks_in_trade_year_num
            , -2                    -- days_in_trade_year_num
            , -2                    -- weeks_in_trade_quarter_num
            , -2                    -- days_in_trade_quarter_num
            , -2                    -- trade_weeks_in_month_445_num
            , 0                     -- is_5_week_month_445_flg
            , -2                    -- trade_weeks_in_month_454_num
            , 0                     -- is_5_week_month_454_flg
            , -2                    -- trade_weeks_in_month_544_num
            , 0                     -- is_5_week_month_544_flg
            , 'Invalid'             -- trade_month_year_nm
            , 'Invalid'             -- trade_year_month_txt
            , 'INV'                 -- trade_month_year_abbr
            , '1900-01-02'::date    -- trade_year_start_dt
            , -2                    -- trade_year_start_key
            , '1900-01-02'::date    -- trade_year_end_dt
            , -2                    -- trade_year_end_key
            , -2                    -- trade_month_overall_num
            , null                  -- prior_trade_month_key
            , null                  -- next_trade_month_key
            , -2                    -- trade_month_last_year_nrf_key
            , -2                    -- trade_month_last_year_walmart_key
            , -2                    -- trade_month_last_year_12m_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -3                      -- trade_month_key
            , -3                    -- trade_year_num
            , -3                    -- trade_month_num
            , 'Not Applicable'      -- trade_month_nm
            , 'N/A'                 -- trade_month_abbr
            , -3                    -- trade_quarter_num
            , 'N/A'                 -- trade_quarter_nm
            , 'Not Applicable'      -- trade_quarter_full_nm
            , '1900-01-03'::date    -- trade_quarter_start_dt
            , -3                    -- trade_quarter_start_key
            , '1900-01-03'::date    -- trade_quarter_end_dt
            , -3                    -- trade_quarter_end_key
            , '1900-01-03'::date    -- trade_month_start_dt
            , '1900-01-03'::date    -- trade_month_end_dt
            , -3                    -- trade_month_start_key
            , -3                    -- trade_month_end_key
            , -3                    -- trade_month_in_quarter_num
            , -3                    -- first_week_of_month_num
            , -3                    -- last_week_of_month_num
            , -3                    -- weeks_in_month_num
            , -3                    -- days_in_month_num
            , 0                     -- contains_leap_week_flg
            , -3                    -- weeks_in_trade_year_num
            , -3                    -- days_in_trade_year_num
            , -3                    -- weeks_in_trade_quarter_num
            , -3                    -- days_in_trade_quarter_num
            , -3                    -- trade_weeks_in_month_445_num
            , 0                     -- is_5_week_month_445_flg
            , -3                    -- trade_weeks_in_month_454_num
            , 0                     -- is_5_week_month_454_flg
            , -3                    -- trade_weeks_in_month_544_num
            , 0                     -- is_5_week_month_544_flg
            , 'Not Applicable'      -- trade_month_year_nm
            , 'Not Applicable'      -- trade_year_month_txt
            , 'N/A'                 -- trade_month_year_abbr
            , '1900-01-03'::date    -- trade_year_start_dt
            , -3                    -- trade_year_start_key
            , '1900-01-03'::date    -- trade_year_end_dt
            , -3                    -- trade_year_end_key
            , -3                    -- trade_month_overall_num
            , null                  -- prior_trade_month_key
            , null                  -- next_trade_month_key
            , -3                    -- trade_month_last_year_nrf_key
            , -3                    -- trade_month_last_year_walmart_key
            , -3                    -- trade_month_last_year_12m_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
    ) as t (
        trade_month_key
        , trade_year_num
        , trade_month_num
        , trade_month_nm
        , trade_month_abbr
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        , trade_quarter_start_dt
        , trade_quarter_start_key
        , trade_quarter_end_dt
        , trade_quarter_end_key
        , trade_month_start_dt
        , trade_month_end_dt
        , trade_month_start_key
        , trade_month_end_key
        , trade_month_in_quarter_num
        , first_week_of_month_num
        , last_week_of_month_num
        , weeks_in_month_num
        , days_in_month_num
        , contains_leap_week_flg
        , weeks_in_trade_year_num
        , days_in_trade_year_num
        , weeks_in_trade_quarter_num
        , days_in_trade_quarter_num
        , trade_weeks_in_month_445_num
        , is_5_week_month_445_flg
        , trade_weeks_in_month_454_num
        , is_5_week_month_454_flg
        , trade_weeks_in_month_544_num
        , is_5_week_month_544_flg
        , trade_month_year_nm
        , trade_year_month_txt
        , trade_month_year_abbr
        , trade_year_start_dt
        , trade_year_start_key
        , trade_year_end_dt
        , trade_year_end_key
        , trade_month_overall_num
        , prior_trade_month_key
        , next_trade_month_key
        , trade_month_last_year_nrf_key
        , trade_month_last_year_walmart_key
        , trade_month_last_year_12m_key
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
