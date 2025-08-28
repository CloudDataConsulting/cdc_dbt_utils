{{ config(materialized='table') }}

with trade_date as (
    select 
        date_key
        , full_dt
        , trade_year_num
        , trade_week_num
        , trade_week_start_dt
        , trade_week_end_dt
        , trade_quarter_445_num
        , trade_quarter_454_num
        , trade_quarter_544_num
        , trade_quarter_445_nm
        , trade_quarter_454_nm
        , trade_quarter_544_nm
        , trade_month_445_num
        , trade_month_454_num
        , trade_month_544_num
        , trade_month_445_nm
        , trade_month_454_nm
        , trade_month_544_nm
        , trade_week_of_quarter_445_num
        , trade_week_of_quarter_454_num
        , trade_week_of_quarter_544_num
        , is_leap_week_flg
        , weeks_in_trade_year_num
    from {{ ref('dim_trade_date') }}),
quarter_445_aggregated as (
    select
        trade_year_num * 10 + trade_quarter_445_num as trade_quarter_445_key
        , min(full_dt) as trade_quarter_445_start_dt
        , max(full_dt) as trade_quarter_445_end_dt
        , min(date_key) as trade_quarter_445_start_key
        , max(date_key) as trade_quarter_445_end_key
        , max(trade_year_num) as trade_year_num
        , max(trade_quarter_445_num) as trade_quarter_445_num
        , max(trade_quarter_445_nm) as trade_quarter_445_nm
        , min(case when trade_week_of_quarter_445_num <= 4 then trade_month_445_num end) as first_month_445_num
        , min(case when trade_week_of_quarter_445_num <= 4 then trade_month_445_nm end) as first_month_445_nm
        , min(case when trade_week_of_quarter_445_num > 4 and trade_week_of_quarter_445_num <= 8 then trade_month_445_num end) as second_month_445_num
        , min(case when trade_week_of_quarter_445_num > 4 and trade_week_of_quarter_445_num <= 8 then trade_month_445_nm end) as second_month_445_nm
        , max(case when trade_week_of_quarter_445_num > 8 then trade_month_445_num end) as third_month_445_num
        , max(case when trade_week_of_quarter_445_num > 8 then trade_month_445_nm end) as third_month_445_nm
        , count(distinct full_dt) as days_in_quarter_445_num
        , count(distinct trade_week_num) as weeks_in_quarter_445_num
        , count(distinct trade_month_445_num) as months_in_quarter_445_num
        , min(trade_week_num) as first_week_of_quarter_445_num
        , max(trade_week_num) as last_week_of_quarter_445_num
    from trade_date
    group by 
        trade_year_num
        , trade_quarter_445_num),
quarter_454_aggregated as (
    select
        trade_year_num * 10 + trade_quarter_454_num as trade_quarter_454_key
        , min(full_dt) as trade_quarter_454_start_dt
        , max(full_dt) as trade_quarter_454_end_dt
        , min(date_key) as trade_quarter_454_start_key
        , max(date_key) as trade_quarter_454_end_key
        , max(trade_year_num) as trade_year_num
        , max(trade_quarter_454_num) as trade_quarter_454_num
        , max(trade_quarter_454_nm) as trade_quarter_454_nm
        , min(case when trade_week_of_quarter_454_num <= 4 then trade_month_454_num end) as first_month_454_num
        , min(case when trade_week_of_quarter_454_num <= 4 then trade_month_454_nm end) as first_month_454_nm
        , min(case when trade_week_of_quarter_454_num > 4 and trade_week_of_quarter_454_num <= 9 then trade_month_454_num end) as second_month_454_num
        , min(case when trade_week_of_quarter_454_num > 4 and trade_week_of_quarter_454_num <= 9 then trade_month_454_nm end) as second_month_454_nm
        , max(case when trade_week_of_quarter_454_num > 9 then trade_month_454_num end) as third_month_454_num
        , max(case when trade_week_of_quarter_454_num > 9 then trade_month_454_nm end) as third_month_454_nm
        , count(distinct full_dt) as days_in_quarter_454_num
        , count(distinct trade_week_num) as weeks_in_quarter_454_num
        , count(distinct trade_month_454_num) as months_in_quarter_454_num
        , min(trade_week_num) as first_week_of_quarter_454_num
        , max(trade_week_num) as last_week_of_quarter_454_num
    from trade_date
    group by 
        trade_year_num
        , trade_quarter_454_num),
quarter_544_aggregated as (
    select
        trade_year_num * 10 + trade_quarter_544_num as trade_quarter_544_key
        , min(full_dt) as trade_quarter_544_start_dt
        , max(full_dt) as trade_quarter_544_end_dt
        , min(date_key) as trade_quarter_544_start_key
        , max(date_key) as trade_quarter_544_end_key
        , max(trade_year_num) as trade_year_num
        , max(trade_quarter_544_num) as trade_quarter_544_num
        , max(trade_quarter_544_nm) as trade_quarter_544_nm
        , min(case when trade_week_of_quarter_544_num <= 5 then trade_month_544_num end) as first_month_544_num
        , min(case when trade_week_of_quarter_544_num <= 5 then trade_month_544_nm end) as first_month_544_nm
        , min(case when trade_week_of_quarter_544_num > 5 and trade_week_of_quarter_544_num <= 9 then trade_month_544_num end) as second_month_544_num
        , min(case when trade_week_of_quarter_544_num > 5 and trade_week_of_quarter_544_num <= 9 then trade_month_544_nm end) as second_month_544_nm
        , max(case when trade_week_of_quarter_544_num > 9 then trade_month_544_num end) as third_month_544_num
        , max(case when trade_week_of_quarter_544_num > 9 then trade_month_544_nm end) as third_month_544_nm
        , count(distinct full_dt) as days_in_quarter_544_num
        , count(distinct trade_week_num) as weeks_in_quarter_544_num
        , count(distinct trade_month_544_num) as months_in_quarter_544_num
        , min(trade_week_num) as first_week_of_quarter_544_num
        , max(trade_week_num) as last_week_of_quarter_544_num
    from trade_date
    group by 
        trade_year_num
        , trade_quarter_544_num),
unified_quarters as (
    select
        '445' as trade_pattern_txt
        , trade_quarter_445_key as trade_quarter_key
        , trade_year_num * 10000 + trade_quarter_445_num * 1000 + 445 as trade_quarter_pattern_key
        , trade_year_num
        , trade_quarter_445_num as trade_quarter_num
        , trade_quarter_445_nm as trade_quarter_nm
        , trade_quarter_445_start_dt as trade_quarter_start_dt
        , trade_quarter_445_end_dt as trade_quarter_end_dt
        , trade_quarter_445_start_key as trade_quarter_start_key
        , trade_quarter_445_end_key as trade_quarter_end_key
        , first_month_445_num as first_month_num
        , second_month_445_num as second_month_num
        , third_month_445_num as third_month_num
        , first_month_445_nm as first_month_nm
        , second_month_445_nm as second_month_nm
        , third_month_445_nm as third_month_nm
        , days_in_quarter_445_num as days_in_quarter_num
        , weeks_in_quarter_445_num as weeks_in_quarter_num
        , months_in_quarter_445_num as months_in_quarter_num
        , first_week_of_quarter_445_num as first_week_of_quarter_num
        , last_week_of_quarter_445_num as last_week_of_quarter_num
        , '4-4-5 weeks per month' as pattern_desc_txt
    from quarter_445_aggregated
    union all
    select
        '454' as trade_pattern_txt
        , trade_quarter_454_key as trade_quarter_key
        , trade_year_num * 10000 + trade_quarter_454_num * 1000 + 454 as trade_quarter_pattern_key
        , trade_year_num
        , trade_quarter_454_num as trade_quarter_num
        , trade_quarter_454_nm as trade_quarter_nm
        , trade_quarter_454_start_dt as trade_quarter_start_dt
        , trade_quarter_454_end_dt as trade_quarter_end_dt
        , trade_quarter_454_start_key as trade_quarter_start_key
        , trade_quarter_454_end_key as trade_quarter_end_key
        , first_month_454_num as first_month_num
        , second_month_454_num as second_month_num
        , third_month_454_num as third_month_num
        , first_month_454_nm as first_month_nm
        , second_month_454_nm as second_month_nm
        , third_month_454_nm as third_month_nm
        , days_in_quarter_454_num as days_in_quarter_num
        , weeks_in_quarter_454_num as weeks_in_quarter_num
        , months_in_quarter_454_num as months_in_quarter_num
        , first_week_of_quarter_454_num as first_week_of_quarter_num
        , last_week_of_quarter_454_num as last_week_of_quarter_num
        , '4-5-4 weeks per month' as pattern_desc_txt
    from quarter_454_aggregated
    union all
    select
        '544' as trade_pattern_txt
        , trade_quarter_544_key as trade_quarter_key
        , trade_year_num * 10000 + trade_quarter_544_num * 1000 + 544 as trade_quarter_pattern_key
        , trade_year_num
        , trade_quarter_544_num as trade_quarter_num
        , trade_quarter_544_nm as trade_quarter_nm
        , trade_quarter_544_start_dt as trade_quarter_start_dt
        , trade_quarter_544_end_dt as trade_quarter_end_dt
        , trade_quarter_544_start_key as trade_quarter_start_key
        , trade_quarter_544_end_key as trade_quarter_end_key
        , first_month_544_num as first_month_num
        , second_month_544_num as second_month_num
        , third_month_544_num as third_month_num
        , first_month_544_nm as first_month_nm
        , second_month_544_nm as second_month_nm
        , third_month_544_nm as third_month_nm
        , days_in_quarter_544_num as days_in_quarter_num
        , weeks_in_quarter_544_num as weeks_in_quarter_num
        , months_in_quarter_544_num as months_in_quarter_num
        , first_week_of_quarter_544_num as first_week_of_quarter_num
        , last_week_of_quarter_544_num as last_week_of_quarter_num
        , '5-4-4 weeks per month' as pattern_desc_txt
    from quarter_544_aggregated),
final as (
    select
        trade_quarter_pattern_key
        , trade_quarter_key
        , trade_pattern_txt
        , pattern_desc_txt
        , trade_year_num
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_start_dt
        , trade_quarter_end_dt
        , trade_quarter_start_key
        , trade_quarter_end_key
        , 'Q' || trade_quarter_num::varchar as trade_quarter_txt
        , 'Q' || trade_quarter_num::varchar || ' ' || trade_year_num::varchar as trade_quarter_year_txt
        , trade_year_num::varchar || '-Q' || trade_quarter_num::varchar as trade_year_quarter_txt
        , trade_pattern_txt || ' Pattern: Q' || trade_quarter_num::varchar || ' ' || trade_year_num::varchar as trade_quarter_pattern_desc_txt
        , 'TQ' || trade_quarter_num::varchar || ' ' || trade_year_num::varchar || ' (' || trade_pattern_txt || ')' as trade_quarter_pattern_code_txt
        , first_month_num
        , second_month_num
        , third_month_num
        , first_month_nm
        , second_month_nm
        , third_month_nm
        , case first_month_nm
            when 'January' then 'Jan' when 'February' then 'Feb' when 'March' then 'Mar'
            when 'April' then 'Apr' when 'May' then 'May' when 'June' then 'Jun'
            when 'July' then 'Jul' when 'August' then 'Aug' when 'September' then 'Sep'
            when 'October' then 'Oct' when 'November' then 'Nov' when 'December' then 'Dec'
        end as first_month_abbr
        , case second_month_nm
            when 'January' then 'Jan' when 'February' then 'Feb' when 'March' then 'Mar'
            when 'April' then 'Apr' when 'May' then 'May' when 'June' then 'Jun'
            when 'July' then 'Jul' when 'August' then 'Aug' when 'September' then 'Sep'
            when 'October' then 'Oct' when 'November' then 'Nov' when 'December' then 'Dec'
        end as second_month_abbr
        , case third_month_nm
            when 'January' then 'Jan' when 'February' then 'Feb' when 'March' then 'Mar'
            when 'April' then 'Apr' when 'May' then 'May' when 'June' then 'Jun'
            when 'July' then 'Jul' when 'August' then 'Aug' when 'September' then 'Sep'
            when 'October' then 'Oct' when 'November' then 'Nov' when 'December' then 'Dec'
        end as third_month_abbr
        , coalesce(
            case first_month_nm
                when 'January' then 'Jan' when 'February' then 'Feb' when 'March' then 'Mar'
                when 'April' then 'Apr' when 'May' then 'May' when 'June' then 'Jun'
                when 'July' then 'Jul' when 'August' then 'Aug' when 'September' then 'Sep'
                when 'October' then 'Oct' when 'November' then 'Nov' when 'December' then 'Dec'
            end, '') || '-' ||
        coalesce(
            case second_month_nm
                when 'January' then 'Jan' when 'February' then 'Feb' when 'March' then 'Mar'
                when 'April' then 'Apr' when 'May' then 'May' when 'June' then 'Jun'
                when 'July' then 'Jul' when 'August' then 'Aug' when 'September' then 'Sep'
                when 'October' then 'Oct' when 'November' then 'Nov' when 'December' then 'Dec'
            end, '') || '-' ||
        coalesce(
            case third_month_nm
                when 'January' then 'Jan' when 'February' then 'Feb' when 'March' then 'Mar'
                when 'April' then 'Apr' when 'May' then 'May' when 'June' then 'Jun'
                when 'July' then 'Jul' when 'August' then 'Aug' when 'September' then 'Sep'
                when 'October' then 'Oct' when 'November' then 'Nov' when 'December' then 'Dec'
            end, '') as trade_quarter_months_abbr
        , days_in_quarter_num
        , weeks_in_quarter_num
        , months_in_quarter_num
        , first_week_of_quarter_num
        , last_week_of_quarter_num
        , case 
            when weeks_in_quarter_num = 13 then 13
            when weeks_in_quarter_num = 14 then 14
            else weeks_in_quarter_num
        end as expected_weeks_in_quarter_num
        , case 
            when first_week_of_quarter_num = last_week_of_quarter_num then 'Week ' || first_week_of_quarter_num::varchar
            else 'Weeks ' || first_week_of_quarter_num::varchar || '-' || last_week_of_quarter_num::varchar
        end as trade_week_span_txt
        , case 
            when trade_quarter_start_dt <= current_date() 
                and trade_quarter_end_dt >= current_date() 
            then 1 else 0 
        end as is_current_trade_quarter_flg
        , case 
            when trade_quarter_start_dt <= dateadd(quarter, -1, current_date()) 
                and trade_quarter_end_dt >= dateadd(quarter, -1, current_date()) 
            then 1 else 0 
        end as is_prior_trade_quarter_flg
        , case 
            when trade_year_num = year(current_date()) 
            then 1 else 0 
        end as is_current_trade_year_flg
        , case 
            when trade_quarter_end_dt < current_date() 
            then 1 else 0 
        end as is_past_trade_quarter_flg
        , case 
            when trade_quarter_num = 4 and weeks_in_quarter_num = 14 
            then 1 else 0 
        end as is_leap_quarter_flg
        , datediff(quarter, trade_quarter_start_dt, current_date()) as trade_quarters_ago_num
        , datediff(quarter, current_date(), trade_quarter_start_dt) as trade_quarters_from_now_num
        , trade_quarter_num as trade_quarter_of_year_num
        , round(trade_quarter_num / 4.0 * 100, 1) as trade_quarter_pct_of_year_num
        , dense_rank() over (partition by trade_pattern_txt order by trade_quarter_start_dt) as trade_quarter_overall_num
        , trade_year_num * 4 + trade_quarter_num - 1 as trade_quarter_sort_num
        , false as dw_deleted_flg
        , current_timestamp as dw_synced_ts
        , 'dim_trade_quarter' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from unified_quarters)
select * from final
order by trade_pattern_txt, trade_quarter_key