{{ config(materialized='table') }}

with date_dimension as ( select * from {{ ref('dim_date') }} ),
week_level_aggregated as (
    select
        week_begin_key as week_key
        , min(week_begin_dt) as week_start_dt
        , max(week_end_dt) as week_end_dt
        , min(week_end_key) as week_end_key
        , max(case when dayofweek(full_date) = 5 then year_num end) as year_num
        , max(case when dayofweek(full_date) = 5 then quarter_num end) as quarter_num
        , max(case when dayofweek(full_date) = 5 then month_num end) as month_num
        , max(case when dayofweek(full_date) = 5 then month_nm end) as month_nm
        , max(week_of_year_num) as week_of_year_num
        , max(week_of_month_num) as week_of_month_num
        , max(iso_week_of_year_txt) as iso_week_txt
        , max(iso_year_num) as iso_year_num
        , max(week_overall_num) as week_overall_num
        , count(*) as days_in_week_num
    from date_dimension
    group by week_begin_key),
final as ( 
    select
        week_key
        , week_start_dt
        , week_end_dt
        , week_end_key
        , year_num
        , quarter_num
        , month_num
        , week_of_year_num
        , week_of_month_num
        , split_part(iso_week_txt, '-W', 2)::int as iso_week_num
        , iso_year_num
        , iso_week_txt
        , month_nm || ' ' || year_num::varchar as month_year_nm
        , 'Week ' || week_of_month_num::varchar || ' of ' || month_nm as week_of_month_nm
        , 'W' || lpad(week_of_year_num::varchar, 2, '0') || ' ' || year_num::varchar as week_year_txt
        , case 
            when week_start_dt <= current_date() 
                and week_end_dt >= current_date() 
            then 1 else 0 
        end as is_current_week_flg
        , case 
            when week_start_dt <= dateadd(week, -1, current_date()) 
                and week_end_dt >= dateadd(week, -1, current_date()) 
            then 1 else 0 
        end as is_prior_week_flg
        , case 
            when year_num = year(current_date()) 
            then 1 else 0 
        end as is_current_year_flg
        , datediff(week, week_start_dt, current_date()) as weeks_ago_num
        , datediff(week, date_trunc(year, week_start_dt), week_start_dt) + 1 as week_of_year_fiscal_num
        , days_in_week_num
        , week_overall_num
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from week_level_aggregated)
select * from final