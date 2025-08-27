{{ config(materialized='table') }}

{# 
Retail Calendar Dimension with All Patterns
Provides trade_week_of_month calculations for all three common retail patterns (445, 454, 544)
in a single table. Column names follow CDC standards with abbreviations (dt, num, ts, flg).
#}

-- Generate base sequence of dates
with date_sequence as (
    select
        dateadd(day, seq4(), '1990-01-01'::date) as calendar_date
    from table(generator(rowcount => 20000))  -- ~55 years of dates
)

-- Calculate retail year boundaries (Sunday nearest to Jan 1)
, retail_years as (
    select distinct
        year(calendar_date) as calendar_year,
        case 
            when dayofweek(date_from_parts(year(calendar_date), 1, 1)) <= 3 then
                -- If Jan 1 is Sun(0), Mon(1), Tue(2), Wed(3) - go to previous Sunday
                dateadd(day, -dayofweek(date_from_parts(year(calendar_date), 1, 1)), 
                       date_from_parts(year(calendar_date), 1, 1))
            else
                -- If Jan 1 is Thu(4), Fri(5), Sat(6) - go to next Sunday
                dateadd(day, 7 - dayofweek(date_from_parts(year(calendar_date), 1, 1)), 
                       date_from_parts(year(calendar_date), 1, 1))
        end as retail_year_start
    from date_sequence
    where calendar_date between '1990-01-01' and '2045-12-31'
)

-- Calculate retail year end dates and week counts
, retail_year_boundaries as (
    select
        calendar_year,
        retail_year_start,
        lead(retail_year_start, 1) over (order by calendar_year) - 1 as retail_year_end,
        datediff(week, retail_year_start, 
                lead(retail_year_start, 1) over (order by calendar_year)) as weeks_in_year
    from retail_years
)

-- Generate retail weeks for each year
, retail_weeks as (
    select
        ryb.calendar_year as retail_year,
        ryb.retail_year_start,
        ryb.retail_year_end,
        ryb.weeks_in_year,
        row_number() over (partition by ryb.calendar_year order by week_num.seq) as retail_week_num,
        dateadd(week, week_num.seq - 1, ryb.retail_year_start) as retail_week_start,
        dateadd(day, 6, dateadd(week, week_num.seq - 1, ryb.retail_year_start)) as retail_week_end
    from retail_year_boundaries ryb
    cross join (
        select seq4() + 1 as seq 
        from table(generator(rowcount => 53))
    ) week_num
    where week_num.seq <= ryb.weeks_in_year
)

-- Assign months for each pattern
, retail_periods as (
    select
        rw.*,
        
        -- 445 Pattern: 4-4-5 weeks per month in each quarter
        case 
            when retail_week_num <= 4 then 1
            when retail_week_num <= 8 then 2
            when retail_week_num <= 13 then 3
            when retail_week_num <= 17 then 4
            when retail_week_num <= 21 then 5
            when retail_week_num <= 26 then 6
            when retail_week_num <= 30 then 7
            when retail_week_num <= 34 then 8
            when retail_week_num <= 39 then 9
            when retail_week_num <= 43 then 10
            when retail_week_num <= 47 then 11
            else 12
        end as retail_month_445,
        
        -- 454 Pattern: 4-5-4 weeks per month in each quarter
        case 
            when retail_week_num <= 4 then 1
            when retail_week_num <= 9 then 2
            when retail_week_num <= 13 then 3
            when retail_week_num <= 17 then 4
            when retail_week_num <= 22 then 5
            when retail_week_num <= 26 then 6
            when retail_week_num <= 30 then 7
            when retail_week_num <= 35 then 8
            when retail_week_num <= 39 then 9
            when retail_week_num <= 43 then 10
            when retail_week_num <= 48 then 11
            else 12
        end as retail_month_454,
        
        -- 544 Pattern: 5-4-4 weeks per month in each quarter
        case 
            when retail_week_num <= 5 then 1
            when retail_week_num <= 9 then 2
            when retail_week_num <= 13 then 3
            when retail_week_num <= 18 then 4
            when retail_week_num <= 22 then 5
            when retail_week_num <= 26 then 6
            when retail_week_num <= 31 then 7
            when retail_week_num <= 35 then 8
            when retail_week_num <= 39 then 9
            when retail_week_num <= 44 then 10
            when retail_week_num <= 48 then 11
            else 12
        end as retail_month_544
        
    from retail_weeks rw
)

-- Generate individual dates with retail attributes
, retail_dates as (
    select
        ds.calendar_date,
        to_char(ds.calendar_date, 'yyyymmdd')::int as date_key,
        
        -- Standard calendar attributes
        year(ds.calendar_date) as calendar_year,
        quarter(ds.calendar_date) as calendar_quarter,
        month(ds.calendar_date) as calendar_month,
        week(ds.calendar_date) as calendar_week,
        dayofweek(ds.calendar_date) as day_of_week,
        dayname(ds.calendar_date) as day_name,
        
        -- Core retail calendar attributes (same for all patterns)
        rp.retail_year,
        rp.retail_week_num,
        rp.retail_week_start,
        rp.retail_week_end,
        rp.weeks_in_year,
        
        -- Month assignments for each pattern
        rp.retail_month_445,
        rp.retail_month_454,
        rp.retail_month_544,
        
        -- Week of month for each pattern
        dense_rank() over (
            partition by rp.retail_year, rp.retail_month_445 
            order by rp.retail_week_num
        ) as trade_week_of_month_445,
        
        dense_rank() over (
            partition by rp.retail_year, rp.retail_month_454 
            order by rp.retail_week_num
        ) as trade_week_of_month_454,
        
        dense_rank() over (
            partition by rp.retail_year, rp.retail_month_544 
            order by rp.retail_week_num
        ) as trade_week_of_month_544,
        
        -- Common flags
        case when rp.retail_week_num = 53 then true else false end as is_leap_week,
        datediff(day, rp.retail_year_start, ds.calendar_date) + 1 as retail_day_of_year
        
    from date_sequence ds
    inner join retail_periods rp
        on ds.calendar_date between rp.retail_week_start and rp.retail_week_end
)

, final as (
    select
        -- Keys
        date_key,
        calendar_date as full_dt,
        
        -- Standard calendar (using CDC abbreviations)
        calendar_year as calendar_year_num,
        calendar_quarter as calendar_quarter_num,
        calendar_month as calendar_month_num,
        calendar_week as calendar_week_num,
        day_of_week as day_of_week_num,
        day_name as day_nm,
        
        -- Core retail calendar (same for all patterns, using CDC abbreviations)
        retail_year as trade_year_num,
        retail_week_num as trade_week_num,
        retail_week_start as trade_week_start_dt,
        retail_week_end as trade_week_end_dt,
        
        -- Trade months for each pattern
        retail_month_445 as trade_month_445_num,
        retail_month_454 as trade_month_454_num,
        retail_month_544 as trade_month_544_num,
        
        -- Trade week of month for each pattern (already has _num suffix implicitly)
        trade_week_of_month_445 as trade_week_of_month_445_num,
        trade_week_of_month_454 as trade_week_of_month_454_num,
        trade_week_of_month_544 as trade_week_of_month_544_num,
        
        -- Common attributes (using CDC abbreviations)
        is_leap_week as is_leap_week_flg,
        weeks_in_year as weeks_in_trade_year_num,
        retail_day_of_year as trade_day_of_year_num,
        
        -- ETL metadata (using CDC abbreviations)
        false as dw_deleted_flg,
        current_timestamp as dw_synced_ts,
        'dim_date_retail' as dw_source_nm
        
    from retail_dates
    where calendar_date between '1995-01-01' and '2040-12-31'
)

select * from final