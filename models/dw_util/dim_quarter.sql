{{ config(materialized='table') }}

with date_dimension as (
    select * from {{ ref('dim_date') }}),
quarter_level_aggregation as (
    select
        year_num * 10 + quarter_num as quarter_key
        , min(full_date) as first_day_of_quarter_dt
        , max(full_date) as last_day_of_quarter_dt
        , min(date_key) as first_day_of_quarter_key
        , max(date_key) as last_day_of_quarter_key
        , max(year_num) as year_num
        , max(quarter_num) as quarter_num
        , max(quarter_nm) as quarter_nm
        , min(case when day_of_quarter_num <= 31 then month_num end) as first_month_num
        , min(case when day_of_quarter_num <= 31 then month_nm end) as first_month_nm
        , min(case when day_of_quarter_num <= 31 then month_abbr end) as first_month_abbr
        , max(case when day_of_quarter_num > 31 and day_of_quarter_num <= 61 then month_num end) as second_month_num
        , max(case when day_of_quarter_num > 31 and day_of_quarter_num <= 61 then month_nm end) as second_month_nm
        , max(case when day_of_quarter_num > 31 and day_of_quarter_num <= 61 then month_abbr end) as second_month_abbr
        , max(case when day_of_quarter_num > 61 then month_num end) as third_month_num
        , max(case when day_of_quarter_num > 61 then month_nm end) as third_month_nm
        , max(case when day_of_quarter_num > 61 then month_abbr end) as third_month_abbr
        , count(*) as days_in_quarter_num
        , count(distinct week_of_year_num) as weeks_in_quarter_num
        , count(distinct month_num) as months_in_quarter_num
    from date_dimension
    group by year_num, quarter_num),
quarter_with_fiscal as (
    select 
        q.*
        , q.year_num as fiscal_year_num
        , q.quarter_num as fiscal_quarter_num
    from quarter_level_aggregation q),
final_quarter_dimension as ( 
    select
        quarter_key
        , first_day_of_quarter_dt
        , last_day_of_quarter_dt
        , first_day_of_quarter_key
        , last_day_of_quarter_key
        , year_num
        , quarter_num
        , quarter_nm
        , 'Q' || quarter_num::varchar as quarter_txt
        , year_num::varchar || '-Q' || quarter_num::varchar as year_quarter_txt
        , 'CY' || year_num::varchar || '-Q' || quarter_num::varchar as calendar_quarter_txt
        , first_month_num
        , first_month_nm
        , first_month_abbr
        , second_month_num
        , second_month_nm
        , second_month_abbr
        , third_month_num
        , third_month_nm
        , third_month_abbr
        , first_month_abbr || ', ' || second_month_abbr || ', ' || third_month_abbr as months_in_quarter_txt
        , days_in_quarter_num
        , weeks_in_quarter_num
        , months_in_quarter_num
        , fiscal_year_num
        , fiscal_quarter_num
        , 'FY' || fiscal_year_num::varchar || '-Q' || fiscal_quarter_num::varchar as fiscal_quarter_txt
        , case 
            when year_num = year(current_date()) 
                and quarter_num = quarter(current_date()) 
            then 1 else 0 
        end as is_current_quarter_flg
        , case 
            when year_num = year(dateadd(quarter, -1, current_date()))
                and quarter_num = quarter(dateadd(quarter, -1, current_date()))
            then 1 else 0 
        end as is_prior_quarter_flg
        , case 
            when year_num = year(current_date()) 
            then 1 else 0 
        end as is_current_year_flg
        , case 
            when last_day_of_quarter_dt < current_date() 
            then 1 else 0 
        end as is_past_quarter_flg
        , datediff(quarter, first_day_of_quarter_dt, current_date()) as quarters_ago_num
        , datediff(quarter, current_date(), first_day_of_quarter_dt) as quarters_from_now_num
        , datediff(quarter, '1970-01-01'::date, first_day_of_quarter_dt) as quarter_overall_num
        , year_num * 4 + quarter_num - 1 as quarter_sort_num
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from quarter_with_fiscal)
select * from final_quarter_dimension