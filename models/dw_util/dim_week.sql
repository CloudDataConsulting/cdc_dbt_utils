{{ config(materialized='table') }}

with dim_date as (select * from {{ ref('dim_date') }})
, weeks as (
    select
        -- Use the Sunday of each week as the natural key
        week_start_key as week_key
        
        -- Core week identifiers
        , min(full_dt) as week_start_dt
        , max(full_dt) as week_end_dt
        , min(date_key) as week_start_key
        , max(date_key) as week_end_key
        
        -- Week attributes (constant within week, so just use MAX)
        , max(year_num) as year_num
        , max(week_num) as week_num
        , max(week_of_year_num) as week_of_year_num
        , max(week_of_quarter_num) as week_of_quarter_num
        , max(week_overall_num) as week_overall_num
        
        -- Month attributes (most common month in the week)
        , max(month_num) as month_num
        , max(month_nm) as month_nm
        , max(month_abbr) as month_abbr
        , max(week_of_month_num) as week_of_month_num
        
        -- Quarter attributes
        , max(quarter_num) as quarter_num
        , max(quarter_nm) as quarter_nm
        
        -- Week metrics
        , count(*) as days_in_week_num
        
        -- ISO week attributes
        , max(iso_year_num) as iso_year_num
        , max(iso_week_of_year_txt) as iso_week_of_year_txt
        , max(iso_week_overall_num) as iso_week_overall_num
        
    from dim_date
    group by week_start_key
)
, final as (
    select
        *
        
        -- Derived display columns
        , 'W' || lpad(week_num::varchar, 2, '0') || ' ' || year_num::varchar as week_year_txt
        , year_num::varchar || '-W' || lpad(week_num::varchar, 2, '0') as year_week_txt
        , month_nm || ' ' || year_num::varchar as month_year_nm
        , 'Week ' || week_of_month_num::varchar || ' of ' || month_nm as week_of_month_nm
        
        -- Current period flags
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
        
        , case 
            when week_end_dt < current_date()
            then 1 else 0 
        end as is_past_week_flg
        
        -- Relative date calculations
        , datediff(week, week_start_dt, current_date()) as weeks_ago_num
        
        -- Metadata
        , current_timestamp as dw_synced_ts
        , 'dim_week' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from weeks
)
select * from final