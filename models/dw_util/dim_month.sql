{{
  config(
    materialized='table',
    post_hook="alter table {{ this }} add primary key (month_key)"
  )
}}
with date_dimension as (
    select * from {{ ref('dim_date') }}
)
, trade_date_dimension as (
    select * from {{ ref('dim_trade_date') }}
)
, filtered_date_data as (
    select
        date_key
        , full_dt
        , year_num
        , quarter_num
        , month_num
        , month_nm
        , month_abbr
        , month_in_quarter_num
        , day_of_month_num
        , month_begin_dt
        , first_day_of_month_flg
        , month_end_dt
        , end_of_month_flg
        , week_of_year_num
        , month_overall_num
        , yearmonth_num
    from date_dimension
    where date_key > 0  -- Exclude the -1 "Not Set" record
)
, monthly_aggregated_data as (
    -- Aggregate to month level
    select
        yearmonth_num as month_key
        -- Month boundaries
        , min(full_dt) as month_begin_dt
        , max(full_dt) as month_end_dt
        , min(date_key) as month_begin_key
        , max(date_key) as month_end_key
        
        -- Calendar attributes (same for all days in month)
        , max(year_num) as year_num
        , max(quarter_num) as quarter_num
        , max(month_num) as month_num
        , max(month_nm) as month_nm
        , max(month_abbr) as month_abbr
        , max(month_in_quarter_num) as month_in_quarter_num
        , max(month_overall_num) as month_overall_num
        
        -- Month metrics
        , count(*) as days_in_month_num
        , count(distinct week_of_year_num) as weeks_in_month_num
        , min(week_of_year_num) as first_week_of_month_num
        , max(week_of_year_num) as last_week_of_month_num
        
    from filtered_date_data
    group by yearmonth_num
    having count(*) >= 28  -- Only include complete months (minimum 28 days for February)
)
, monthly_data_with_trade_calendar as (
    -- Add retail calendar from dim_trade_date if it exists
    select 
        m.*
        
        -- Pull retail/trade calendar attributes from dim_trade_date
        -- Using the 15th of the month as the determinant
        , coalesce(
            (select max(trade_year_num) 
             from trade_date_dimension as dr
             where dr.calendar_year_num = m.year_num
               and dr.calendar_month_num = m.month_num
               and dr.day_of_month_num = 15)
            , m.year_num
        ) as trade_year_num
        
        , coalesce(
            (select max(trade_month_445_num)
             from trade_date_dimension as dr
             where dr.calendar_year_num = m.year_num
               and dr.calendar_month_num = m.month_num
               and dr.day_of_month_num = 15)
            , m.month_num
        ) as trade_month_num
        
        , coalesce(
            (select max(calendar_quarter_num)
             from trade_date_dimension as dr
             where dr.calendar_year_num = m.year_num
               and dr.calendar_month_num = m.month_num
               and dr.day_of_month_num = 15)
            , m.quarter_num
        ) as trade_quarter_num
        
    from monthly_aggregated_data as m
)
, final as (
    select
        -- Primary key
        month_key
        
        -- Month dates
        , month_begin_dt
        , month_end_dt
        , month_begin_key
        , month_end_key
        
        -- Standard calendar
        , year_num
        , quarter_num
        , month_num
        , month_nm
        , month_abbr
        , month_in_quarter_num
        
        -- Quarter information
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
        
        -- Month descriptions
        , month_nm || ' ' || year_num::varchar as month_year_nm
        , month_abbr || ' ' || year_num::varchar as month_year_abbr
        , year_num::varchar || '-' || lpad(month_num::varchar, 2, '0') as year_month_txt
        
        -- Month metrics
        , days_in_month_num
        , weeks_in_month_num
        
        -- Retail calendar
        , trade_year_num
        , trade_month_num
        , trade_quarter_num
        
        -- Position in year
        , month_num as month_of_year_num
        , month_num as month_of_year_fiscal_num  -- Can be overridden for fiscal calendars
        
        -- Flags
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
            when month_end_dt < current_date()
            then 1 else 0
        end as is_past_month_flg
        -- Relative month numbers
        , datediff(month, month_begin_dt, current_date()) as months_ago_num
        , datediff(month, current_date(), month_begin_dt) as months_from_now_num
        
        -- Overall month number
        , month_overall_num
        
        -- Sorting helpers
        , year_num * 12 + month_num - 1 as month_sort_num
        
        -- ETL metadata
        , current_user as create_user_id
        , current_timestamp as create_timestamp

    from monthly_data_with_trade_calendar
)
select * from final
