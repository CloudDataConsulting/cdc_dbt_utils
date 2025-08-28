{{ config(materialized='table') }}

with date_dimension as ( select * from {{ ref('dim_date') }} ),
monthly_aggregated_data as (
    select
        yearmonth_num as month_key
        , min(full_date) as first_day_of_month_dt
        , max(full_date) as last_day_of_month_dt
        , min(date_key) as first_day_of_month_key
        , max(date_key) as last_day_of_month_key
        , max(year_num) as year_num
        , max(quarter_num) as quarter_num
        , max(month_num) as month_num
        , max(month_nm) as month_nm
        , max(month_abbr) as month_abbr
        , max(month_in_quarter_num) as month_in_quarter_num
        , max(month_overall_num) as month_overall_num
        , count(*) as days_in_month_num
        , count(distinct week_of_year_num) as weeks_in_month_num
        , min(week_of_year_num) as first_week_of_month_num
        , max(week_of_year_num) as last_week_of_month_num
    from date_dimension
    group by yearmonth_num),
final as ( 
    select
        month_key
        , first_day_of_month_dt
        , last_day_of_month_dt
        , first_day_of_month_key
        , last_day_of_month_key
        , year_num
        , quarter_num
        , month_num
        , month_nm
        , month_abbr
        , month_in_quarter_num
        , case 
            when quarter_num = 1 then 'Q1'
            when quarter_num = 2 then 'Q2'
            when quarter_num = 3 then 'Q3'
            when quarter_num = 4 then 'Q4'
        end as quarter_txt
        , case 
            when quarter_num = 1 then 'First'
            when quarter_num = 2 then 'Second'
            when quarter_num = 3 then 'Third'
            when quarter_num = 4 then 'Fourth'
        end as quarter_nm
        , month_nm || ' ' || year_num::varchar as month_year_nm
        , month_abbr || ' ' || year_num::varchar as month_year_abbr
        , year_num::varchar || '-' || lpad(month_num::varchar, 2, '0') as year_month_txt
        , days_in_month_num
        , weeks_in_month_num
        , month_num as month_of_year_num
        , month_num as month_of_year_fiscal_num
        , case 
            when year_num = year(current_date()) 
                and month_num = month(current_date()) 
            then 1 else 0 
        end as is_current_month_flg
        , case 
            when year_num = year(dateadd(month, -1, current_date()))
                and month_num = month(dateadd(month, -1, current_date()))
            then 1 else 0 
        end as is_prior_month_flg
        , case 
            when year_num = year(current_date()) 
            then 1 else 0 
        end as is_current_year_flg
        , case 
            when last_day_of_month_dt < current_date() 
            then 1 else 0 
        end as is_past_month_flg
        , datediff(month, first_day_of_month_dt, current_date()) as months_ago_num
        , datediff(month, current_date(), first_day_of_month_dt) as months_from_now_num
        , month_overall_num
        , year_num * 12 + month_num - 1 as month_sort_num
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from monthly_aggregated_data)
select * from final