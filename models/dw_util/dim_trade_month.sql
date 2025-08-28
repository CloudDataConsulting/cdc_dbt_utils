{{ config(materialized='table') }}

{#
dim_trade_month - Trade Calendar Month-level dimension
One row per trade month per pattern, derived from dim_trade_date for consistency
Includes all three patterns (445, 454, 544) for retail/trade calendar months
Each pattern has different month boundaries due to different week allocations
#}

with trade_date as ( select * from  {{ ref('dim_trade_date') }} )
, trade_date_data as (
    -- Pull from dim_trade_date to ensure consistency
    select
        date_key
        , full_dt,
        trade_year_num
        , trade_week_num,
        trade_week_start_dt
        , trade_week_end_dt,

        -- Month and quarter attributes for all patterns
        trade_month_445_num
        , trade_month_454_num,
        trade_month_544_num
        , trade_month_445_nm,
        trade_month_454_nm
        , trade_month_544_nm,

        trade_quarter_445_num
        , trade_quarter_454_num,
        trade_quarter_544_num
        , trade_quarter_445_nm,
        trade_quarter_454_nm
        , trade_quarter_544_nm,

        -- Week of period attributes for all patterns
        trade_week_of_month_445_num
        , trade_week_of_month_454_num,
        trade_week_of_month_544_num,

        -- Common attributes
        is_leap_week_flg
        , weeks_in_trade_year_num

    from trade_date)
,

-- Aggregate for 445 Pattern
month_445_aggregated as (
    select
        -- Primary key for 445 pattern
        trade_year_num * 100 + trade_month_445_num as trade_month_445_key,

        -- Month boundaries for 445 pattern
        min(full_dt) as trade_month_445_start_dt
        , max(full_dt) as trade_month_445_end_dt,
        min(date_key) as trade_month_445_start_key
        , max(date_key) as trade_month_445_end_key,

        -- Trade calendar attributes (same for all days in month)
        max(trade_year_num) as trade_year_num
        , max(trade_month_445_num) as trade_month_445_num,
        max(trade_month_445_nm) as trade_month_445_nm
        , max(trade_quarter_445_num) as trade_quarter_445_num,
        max(trade_quarter_445_nm) as trade_quarter_445_nm,

        -- Month metrics for 445 pattern
        count(distinct full_dt) as days_in_month_445_num
        , count(distinct trade_week_num) as weeks_in_month_445_num,
        min(trade_week_num) as first_week_of_month_445_num
        , max(trade_week_num) as last_week_of_month_445_num,

        -- Calculate month in quarter
        case trade_month_445_num
            when 1 then 1 when 2 then 2 when 3 then 3
            when 4 then 1 when 5 then 2 when 6 then 3
            when 7 then 1 when 8 then 2 when 9 then 3
            when 10 then 1 when 11 then 2 when 12 then 3
        end as month_in_quarter_445_num

    from trade_date_data
    group by
        trade_year_num
        , trade_month_445_num),

-- Aggregate for 454 Pattern
month_454_aggregated as (
    select
        -- Primary key for 454 pattern
        trade_year_num * 100 + trade_month_454_num as trade_month_454_key,

        -- Month boundaries for 454 pattern
        min(full_dt) as trade_month_454_start_dt
        , max(full_dt) as trade_month_454_end_dt,
        min(date_key) as trade_month_454_start_key
        , max(date_key) as trade_month_454_end_key,

        -- Trade calendar attributes
        max(trade_year_num) as trade_year_num
        , max(trade_month_454_num) as trade_month_454_num,
        max(trade_month_454_nm) as trade_month_454_nm
        , max(trade_quarter_454_num) as trade_quarter_454_num,
        max(trade_quarter_454_nm) as trade_quarter_454_nm,

        -- Month metrics for 454 pattern
        count(distinct full_dt) as days_in_month_454_num
        , count(distinct trade_week_num) as weeks_in_month_454_num,
        min(trade_week_num) as first_week_of_month_454_num
        , max(trade_week_num) as last_week_of_month_454_num,

        -- Calculate month in quarter
        case trade_month_454_num
            when 1 then 1 when 2 then 2 when 3 then 3
            when 4 then 1 when 5 then 2 when 6 then 3
            when 7 then 1 when 8 then 2 when 9 then 3
            when 10 then 1 when 11 then 2 when 12 then 3
        end as month_in_quarter_454_num

    from trade_date_data
    group by
        trade_year_num
        , trade_month_454_num),

-- Aggregate for 544 Pattern
month_544_aggregated as (
    select
        -- Primary key for 544 pattern
        trade_year_num * 100 + trade_month_544_num as trade_month_544_key,

        -- Month boundaries for 544 pattern
        min(full_dt) as trade_month_544_start_dt
        , max(full_dt) as trade_month_544_end_dt,
        min(date_key) as trade_month_544_start_key
        , max(date_key) as trade_month_544_end_key,

        -- Trade calendar attributes
        max(trade_year_num) as trade_year_num
        , max(trade_month_544_num) as trade_month_544_num,
        max(trade_month_544_nm) as trade_month_544_nm
        , max(trade_quarter_544_num) as trade_quarter_544_num,
        max(trade_quarter_544_nm) as trade_quarter_544_nm,

        -- Month metrics for 544 pattern
        count(distinct full_dt) as days_in_month_544_num
        , count(distinct trade_week_num) as weeks_in_month_544_num,
        min(trade_week_num) as first_week_of_month_544_num
        , max(trade_week_num) as last_week_of_month_544_num,

        -- Calculate month in quarter
        case trade_month_544_num
            when 1 then 1 when 2 then 2 when 3 then 3
            when 4 then 1 when 5 then 2 when 6 then 3
            when 7 then 1 when 8 then 2 when 9 then 3
            when 10 then 1 when 11 then 2 when 12 then 3
        end as month_in_quarter_544_num

    from trade_date_data
    group by
        trade_year_num
        , trade_month_544_num),

-- Combine all patterns into a unified structure
unified_months as (
    -- 445 Pattern records
    select
        '445' as trade_pattern_txt,

        -- Primary keys
        trade_month_445_key as trade_month_key
        , trade_year_num * 10000 + trade_month_445_num * 100 + 445 as trade_month_pattern_key,

        -- Core attributes
        trade_year_num
        , trade_month_445_num as trade_month_num,
        trade_month_445_nm as trade_month_nm
        , trade_quarter_445_num as trade_quarter_num,
        trade_quarter_445_nm as trade_quarter_nm
        , month_in_quarter_445_num as month_in_quarter_num,

        -- Month boundaries
        trade_month_445_start_dt as trade_month_start_dt
        , trade_month_445_end_dt as trade_month_end_dt,
        trade_month_445_start_key as trade_month_start_key
        , trade_month_445_end_key as trade_month_end_key,

        -- Metrics
        days_in_month_445_num as days_in_month_num
        , weeks_in_month_445_num as weeks_in_month_num,
        first_week_of_month_445_num as first_week_of_month_num
        , last_week_of_month_445_num as last_week_of_month_num

    from month_445_aggregated

    union all

    -- 454 Pattern records
    select
        '454' as trade_pattern_txt,

        -- Primary keys
        trade_month_454_key as trade_month_key
        , trade_year_num * 10000 + trade_month_454_num * 100 + 454 as trade_month_pattern_key,

        -- Core attributes
        trade_year_num
        , trade_month_454_num as trade_month_num,
        trade_month_454_nm as trade_month_nm
        , trade_quarter_454_num as trade_quarter_num,
        trade_quarter_454_nm as trade_quarter_nm
        , month_in_quarter_454_num as month_in_quarter_num,

        -- Month boundaries
        trade_month_454_start_dt as trade_month_start_dt
        , trade_month_454_end_dt as trade_month_end_dt,
        trade_month_454_start_key as trade_month_start_key
        , trade_month_454_end_key as trade_month_end_key,

        -- Metrics
        days_in_month_454_num as days_in_month_num
        , weeks_in_month_454_num as weeks_in_month_num,
        first_week_of_month_454_num as first_week_of_month_num
        , last_week_of_month_454_num as last_week_of_month_num

    from month_454_aggregated

    union all

    -- 544 Pattern records
    select
        '544' as trade_pattern_txt,

        -- Primary keys
        trade_month_544_key as trade_month_key
        , trade_year_num * 10000 + trade_month_544_num * 100 + 544 as trade_month_pattern_key,

        -- Core attributes
        trade_year_num
        , trade_month_544_num as trade_month_num,
        trade_month_544_nm as trade_month_nm
        , trade_quarter_544_num as trade_quarter_num,
        trade_quarter_544_nm as trade_quarter_nm
        , month_in_quarter_544_num as month_in_quarter_num,

        -- Month boundaries
        trade_month_544_start_dt as trade_month_start_dt
        , trade_month_544_end_dt as trade_month_end_dt,
        trade_month_544_start_key as trade_month_start_key
        , trade_month_544_end_key as trade_month_end_key,

        -- Metrics
        days_in_month_544_num as days_in_month_num
        , weeks_in_month_544_num as weeks_in_month_num,
        first_week_of_month_544_num as first_week_of_month_num
        , last_week_of_month_544_num as last_week_of_month_num

    from month_544_aggregated),

final as ( select
        -- Primary keys
        trade_month_pattern_key -- Unique across all patterns
        , trade_month_key,          -- Pattern-specific key (YYYYMM format)

        -- Pattern identifier
        trade_pattern_txt,

        -- Core trade calendar
        trade_year_num
        , trade_month_num,
        trade_month_nm
        , trade_quarter_num,
        trade_quarter_nm
        , month_in_quarter_num,

        -- Month boundaries
        trade_month_start_dt
        , trade_month_end_dt,
        trade_month_start_key
        , trade_month_end_key,

        -- Quarter information
        case trade_quarter_num
            when 1 then 'Q1'
            when 2 then 'Q2'
            when 3 then 'Q3'
            when 4 then 'Q4'
        end as trade_quarter_txt,

        -- Month descriptions
        trade_month_nm || ' ' || trade_year_num::varchar as trade_month_year_nm
        , left(trade_month_nm, 3) || ' ' || trade_year_num::varchar as trade_month_year_abbr,
        trade_year_num::varchar || '-' || lpad(trade_month_num::varchar, 2, '0') as trade_year_month_txt,

        -- Month abbreviation
        case trade_month_nm
            when 'January' then 'Jan' when 'February' then 'Feb' when 'March' then 'Mar'
            when 'April' then 'Apr' when 'May' then 'May' when 'June' then 'Jun'
            when 'July' then 'Jul' when 'August' then 'Aug' when 'September' then 'Sep'
            when 'October' then 'Oct' when 'November' then 'Nov' when 'December' then 'Dec'
        end as trade_month_abbr,

        -- Pattern-specific descriptions
        trade_pattern_txt || ' Pattern: ' || trade_month_nm || ' ' || trade_year_num::varchar as trade_month_pattern_desc_txt
        , 'TM' || lpad(trade_month_num::varchar, 2, '0') || ' ' || trade_year_num::varchar || ' (' || trade_pattern_txt || ')' as trade_month_pattern_code_txt,

        -- Month metrics
        days_in_month_num
        , weeks_in_month_num,
        first_week_of_month_num
        , last_week_of_month_num,

        -- Expected weeks based on pattern
        case
            when trade_pattern_txt = '445' and month_in_quarter_num in (1, 2) then 4
            when trade_pattern_txt = '445' and month_in_quarter_num = 3 then 5
            when trade_pattern_txt = '454' and month_in_quarter_num in (1, 3) then 4
            when trade_pattern_txt = '454' and month_in_quarter_num = 2 then 5
            when trade_pattern_txt = '544' and month_in_quarter_num = 1 then 5
            when trade_pattern_txt = '544' and month_in_quarter_num in (2, 3) then 4
        end as expected_weeks_in_month_num,

        -- Week span description
        case
            when first_week_of_month_num = last_week_of_month_num then 'Week ' || first_week_of_month_num::varchar
            else 'Weeks ' || first_week_of_month_num::varchar || '-' || last_week_of_month_num::varchar
        end as trade_week_span_txt,

        -- Flags
        case
            when trade_month_start_dt <= current_date()
                and trade_month_end_dt >= current_date()
            then 1 else 0
        end as is_current_trade_month_flg,

        case
            when trade_month_start_dt <= dateadd(month, -1, current_date())
                and trade_month_end_dt >= dateadd(month, -1, current_date())
            then 1 else 0
        end as is_prior_trade_month_flg,

        case
            when trade_year_num = year(current_date())
            then 1 else 0
        end as is_current_trade_year_flg,

        case
            when trade_month_end_dt < current_date()
            then 1 else 0
        end as is_past_trade_month_flg,

        -- Relative month calculations
        datediff(month, trade_month_start_dt, current_date()) as trade_months_ago_num
        , datediff(month, current_date(), trade_month_start_dt) as trade_months_from_now_num,

        -- Position in year
        trade_month_num as trade_month_of_year_num
        , round(trade_month_num / 12.0 * 100, 1) as trade_month_pct_of_year_num,

        -- Calculate overall month number (months since earliest trade month)
        dense_rank() over (partition by trade_pattern_txt order by trade_month_start_dt) as trade_month_overall_num,

        -- Sorting helpers
        trade_year_num * 12 + trade_month_num - 1 as trade_month_sort_num,

        -- ETL metadata
        false as dw_deleted_flg
        , current_timestamp as dw_synced_ts,
        'dim_trade_month' as dw_source_nm
        , current_user as create_user_id,
        current_timestamp as create_timestamp

    from unified_months)

select * from final
order by trade_pattern_txt, trade_month_key
