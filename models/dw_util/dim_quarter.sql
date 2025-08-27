{{ config(materialized='table') }}

{#
dim_quarter - Quarter-level date dimension
One row per quarter, derived from dim_date for consistency
Includes both standard fiscal and retail calendar attributes
#}

with date_data as (
    -- Pull from dim_date to ensure consistency
    select 
        date_key,
        full_date,
        year_num,
        quarter_num,
        quarter_nm,
        month_num,
        month_nm,
        month_abbr,
        first_day_of_quarter,
        last_day_of_quarter,
        day_of_quarter_num,
        week_of_year_num
    from {{ ref('dim_date') }}
    where date_key > 0  -- Exclude the -1 "Not Set" record
),

quarter_aggregated as (
    -- Aggregate to quarter level
    select
        year_num * 10 + quarter_num as quarter_key,
        
        -- Quarter boundaries
        min(full_date) as first_day_of_quarter_dt,
        max(full_date) as last_day_of_quarter_dt,
        min(date_key) as first_day_of_quarter_key,
        max(date_key) as last_day_of_quarter_key,
        
        -- Calendar attributes
        max(year_num) as year_num,
        max(quarter_num) as quarter_num,
        max(quarter_nm) as quarter_nm,
        
        -- Months in quarter
        min(case when day_of_quarter_num <= 31 then month_num end) as first_month_num,
        min(case when day_of_quarter_num <= 31 then month_nm end) as first_month_nm,
        min(case when day_of_quarter_num <= 31 then month_abbr end) as first_month_abbr,
        
        max(case when day_of_quarter_num > 31 and day_of_quarter_num <= 61 then month_num end) as second_month_num,
        max(case when day_of_quarter_num > 31 and day_of_quarter_num <= 61 then month_nm end) as second_month_nm,
        max(case when day_of_quarter_num > 31 and day_of_quarter_num <= 61 then month_abbr end) as second_month_abbr,
        
        max(case when day_of_quarter_num > 61 then month_num end) as third_month_num,
        max(case when day_of_quarter_num > 61 then month_nm end) as third_month_nm,
        max(case when day_of_quarter_num > 61 then month_abbr end) as third_month_abbr,
        
        -- Quarter metrics
        count(*) as days_in_quarter_num,
        count(distinct week_of_year_num) as weeks_in_quarter_num,
        count(distinct month_num) as months_in_quarter_num
        
    from date_data
    group by year_num, quarter_num
),

quarter_with_retail as (
    -- Add retail calendar from dim_date_trade if it exists
    select 
        q.*,
        
        -- For fiscal year, default to calendar year (organizations can customize)
        q.year_num as fiscal_year_num,
        q.quarter_num as fiscal_quarter_num,
        
        -- Pull retail/trade calendar attributes from dim_date_trade
        -- Using the middle of the quarter (day 46) as the determinant
        coalesce(
            (select max(trade_year_num) 
             from {{ ref('dim_date_trade') }} dr
             where dr.full_dt = dateadd(day, 45, q.first_day_of_quarter_dt)),
            q.year_num
        ) as trade_year_num,
        
        -- Retail quarters often don't align with calendar quarters
        -- This is simplified - actual implementation would need proper retail quarter logic
        case 
            when q.first_month_num in (2,3,4) then 1
            when q.first_month_num in (5,6,7) then 2
            when q.first_month_num in (8,9,10) then 3
            else 4
        end as trade_quarter_num
        
    from quarter_aggregated q
),

final as (
    select
        -- Primary key
        quarter_key,
        
        -- Quarter dates
        first_day_of_quarter_dt,
        last_day_of_quarter_dt,
        first_day_of_quarter_key,
        last_day_of_quarter_key,
        
        -- Standard calendar
        year_num,
        quarter_num,
        
        -- Quarter naming
        'Q' || quarter_num::varchar as quarter_txt,
        quarter_nm,
        
        'Q' || quarter_num::varchar || ' ' || year_num::varchar as quarter_year_txt,
        year_num::varchar || '-Q' || quarter_num::varchar as year_quarter_txt,
        
        -- Months in quarter
        first_month_num,
        second_month_num,
        third_month_num,
        
        first_month_nm,
        second_month_nm,
        third_month_nm,
        
        coalesce(first_month_abbr, '') || '-' ||
        coalesce(second_month_abbr, '') || '-' ||
        coalesce(third_month_abbr, '') as quarter_months_abbr,
        
        -- Quarter metrics
        days_in_quarter_num,
        weeks_in_quarter_num,
        months_in_quarter_num,
        
        -- Fiscal calendar
        fiscal_year_num,
        fiscal_quarter_num,
        'FY' || fiscal_year_num::varchar || '-Q' || fiscal_quarter_num::varchar as fiscal_quarter_txt,
        
        -- Retail calendar
        trade_year_num,
        trade_quarter_num,
        'RY' || trade_year_num::varchar || '-Q' || trade_quarter_num::varchar as trade_quarter_txt,
        
        -- Flags
        case 
            when year_num = year(current_date()) 
                and quarter_num = quarter(current_date()) 
            then 1 else 0 
        end as is_current_quarter_flg,
        
        case 
            when year_num = year(dateadd(quarter, -1, current_date()))
                and quarter_num = quarter(dateadd(quarter, -1, current_date()))
            then 1 else 0 
        end as is_prior_quarter_flg,
        
        case 
            when year_num = year(current_date()) 
            then 1 else 0 
        end as is_current_year_flg,
        
        case 
            when last_day_of_quarter_dt < current_date() 
            then 1 else 0 
        end as is_past_quarter_flg,
        
        -- Relative quarter numbers
        datediff(quarter, first_day_of_quarter_dt, current_date()) as quarters_ago_num,
        datediff(quarter, current_date(), first_day_of_quarter_dt) as quarters_from_now_num,
        
        -- Overall quarter number since 1970
        datediff(quarter, '1970-01-01'::date, first_day_of_quarter_dt) as quarter_overall_num,
        
        -- Sorting helpers
        year_num * 4 + quarter_num - 1 as quarter_sort_num,
        
        -- ETL metadata
        current_user as create_user_id,
        current_timestamp as create_timestamp
        
    from quarter_with_retail
)

select * from final