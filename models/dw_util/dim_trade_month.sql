{{ config(materialized='table') }}

with trade_date as ( select * from {{ ref('dim_trade_date') }} )
, trade_date_data as (
    select
        date_key
        , calendar_full_dt
        , trade_year_num
        , trade_week_num
        , trade_week_start_dt
        , trade_week_end_dt
        , trade_month_445_num
        , trade_month_454_num
        , trade_month_544_num
        , trade_month_445_nm
        , trade_month_454_nm
        , trade_month_544_nm
        , trade_quarter_445_num
        , trade_quarter_454_num
        , trade_quarter_544_num
        , trade_quarter_nm
        , trade_week_of_month_445_num
        , trade_week_of_month_454_num
        , trade_week_of_month_544_num
        , is_trade_leap_week_flg
        , weeks_in_trade_year_num
    from trade_date)
, month_445_aggregated as (
    select
        trade_year_num::varchar || lpad(trade_month_445_num::varchar, 2, '0') || '445' as trade_month_key
        , '445' as pattern_txt
        , trade_year_num
        , trade_month_445_num as trade_month_num
        , trade_month_445_nm as trade_month_nm
        , trade_quarter_445_num as trade_quarter_num
        , trade_quarter_nm
        , min(calendar_full_dt) as first_day_of_month_dt
        , max(calendar_full_dt) as last_day_of_month_dt
        , min(date_key) as first_day_of_month_key
        , max(date_key) as last_day_of_month_key
        , min(trade_week_num) as first_week_of_month_num
        , max(trade_week_num) as last_week_of_month_num
        , count(distinct trade_week_num) as weeks_in_month_num
        , count(*) as days_in_month_num
        , max(case when trade_week_of_month_445_num = 5 then 1 else 0 end) as is_5_week_month_flg
        , max(is_trade_leap_week_flg) as contains_leap_week_flg
    from trade_date_data
    group by trade_year_num, trade_month_445_num, trade_month_445_nm, trade_quarter_445_num, trade_quarter_nm)
, month_454_aggregated as (
    select
        trade_year_num::varchar || lpad(trade_month_454_num::varchar, 2, '0') || '454' as trade_month_key
        , '454' as pattern_txt
        , trade_year_num
        , trade_month_454_num as trade_month_num
        , trade_month_454_nm as trade_month_nm
        , trade_quarter_454_num as trade_quarter_num
        , trade_quarter_nm
        , min(calendar_full_dt) as first_day_of_month_dt
        , max(calendar_full_dt) as last_day_of_month_dt
        , min(date_key) as first_day_of_month_key
        , max(date_key) as last_day_of_month_key
        , min(trade_week_num) as first_week_of_month_num
        , max(trade_week_num) as last_week_of_month_num
        , count(distinct trade_week_num) as weeks_in_month_num
        , count(*) as days_in_month_num
        , max(case when trade_week_of_month_454_num = 5 then 1 else 0 end) as is_5_week_month_flg
        , max(is_trade_leap_week_flg) as contains_leap_week_flg
    from trade_date_data
    group by trade_year_num, trade_month_454_num, trade_month_454_nm, trade_quarter_454_num, trade_quarter_nm)
, month_544_aggregated as (
    select
        trade_year_num::varchar || lpad(trade_month_544_num::varchar, 2, '0') || '544' as trade_month_key
        , '544' as pattern_txt
        , trade_year_num
        , trade_month_544_num as trade_month_num
        , trade_month_544_nm as trade_month_nm
        , trade_quarter_544_num as trade_quarter_num
        , trade_quarter_nm
        , min(calendar_full_dt) as first_day_of_month_dt
        , max(calendar_full_dt) as last_day_of_month_dt
        , min(date_key) as first_day_of_month_key
        , max(date_key) as last_day_of_month_key
        , min(trade_week_num) as first_week_of_month_num
        , max(trade_week_num) as last_week_of_month_num
        , count(distinct trade_week_num) as weeks_in_month_num
        , count(*) as days_in_month_num
        , max(case when trade_week_of_month_544_num = 5 then 1 else 0 end) as is_5_week_month_flg
        , max(is_trade_leap_week_flg) as contains_leap_week_flg
    from trade_date_data
    group by trade_year_num, trade_month_544_num, trade_month_544_nm, trade_quarter_544_num, trade_quarter_nm)
, all_patterns_union as (
    select * from month_445_aggregated
    union all
    select * from month_454_aggregated
    union all
    select * from month_544_aggregated)
, final as (
    select
        trade_month_key
        -- Add combined pattern key
        , trade_year_num * 10000 + trade_month_num * 100 + 
            case pattern_txt 
                when '445' then 445 
                when '454' then 454 
                when '544' then 544 
            end as trade_month_pattern_key
        , pattern_txt as trade_pattern_txt
        , trade_year_num
        , trade_month_num
        , trade_month_nm
        , left(trade_month_nm, 3) as trade_month_abbr
        , trade_month_num as trade_month_of_year_num
        , trade_quarter_num
        , trade_quarter_nm
        , first_day_of_month_dt as trade_month_start_dt
        , last_day_of_month_dt as trade_month_end_dt
        , first_day_of_month_key as trade_month_start_key
        , last_day_of_month_key as trade_month_end_key
        , trade_month_nm || ' ' || trade_year_num::varchar as trade_month_year_nm
        , left(trade_month_nm, 3) || ' ' || trade_year_num::varchar as trade_month_year_abbr
        , trade_year_num::varchar || '-' || lpad(trade_month_num::varchar, 2, '0') as year_month_txt
        , 'Q' || trade_quarter_num::varchar as trade_quarter_txt
        , 'TY' || trade_year_num::varchar || '-M' || lpad(trade_month_num::varchar, 2, '0') as trade_year_month_txt
        , case 
            when trade_month_num in (1, 4, 7, 10) then 1
            when trade_month_num in (2, 5, 8, 11) then 2
            else 3
        end as month_in_quarter_num
        , days_in_month_num
        , weeks_in_month_num
        -- Expected weeks based on pattern
        , case 
            when pattern_txt = '445' and month_in_quarter_num in (1, 2) then 4
            when pattern_txt = '445' and month_in_quarter_num = 3 then 5
            when pattern_txt = '454' and month_in_quarter_num in (1, 3) then 4
            when pattern_txt = '454' and month_in_quarter_num = 2 then 5
            when pattern_txt = '544' and month_in_quarter_num = 1 then 5
            when pattern_txt = '544' and month_in_quarter_num in (2, 3) then 4
        end as expected_weeks_in_month_num
        , first_week_of_month_num
        , last_week_of_month_num
        , is_5_week_month_flg
        , contains_leap_week_flg
        , case 
            when trade_year_num = year(current_date()) 
                and first_day_of_month_dt <= current_date() 
                and last_day_of_month_dt >= current_date()
            then 1 else 0 
        end as is_current_trade_month_flg
        , case 
            when dateadd(month, -1, date_trunc('month', current_date())) = date_trunc('month', first_day_of_month_dt)
            then 1 else 0 
        end as is_prior_trade_month_flg
        , case 
            when trade_year_num = year(current_date()) 
            then 1 else 0 
        end as is_current_trade_year_flg
        , case 
            when last_day_of_month_dt < current_date() 
            then 1 else 0 
        end as is_past_trade_month_flg
        , datediff(month, first_day_of_month_dt, current_date()) as trade_months_ago_num
        , datediff(month, current_date(), first_day_of_month_dt) as trade_months_from_now_num
        , dense_rank() over (order by trade_year_num, trade_month_num, pattern_txt) as trade_month_overall_num
        , trade_year_num * 12 + trade_month_num - 1 as trade_month_sort_num
        -- Pattern descriptions
        , pattern_txt as trade_month_pattern_code_txt
        , case pattern_txt
            when '445' then 'Four-Four-Five Week Pattern'
            when '454' then 'Four-Five-Four Week Pattern'  
            when '544' then 'Five-Four-Four Week Pattern'
        end as trade_month_pattern_desc_txt
        -- Week span text
        , 'W' || first_week_of_month_num || '-W' || last_week_of_month_num as trade_week_span_txt
        -- Percentage of year
        , round(days_in_month_num / 365.0 * 100, 2) as trade_month_pct_of_year_num
        -- DW metadata
        , current_timestamp as dw_synced_ts
        , 'dim_trade_month' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from all_patterns_union)
select * from final