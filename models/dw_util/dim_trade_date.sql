{{ config(materialized='table') }}

{# 
Trade Date Dimension with All Patterns
Provides trade_week_of_month calculations for all three common trade/retail patterns (445, 454, 544)
in a single table. Column names follow CDC standards with abbreviations (dt, num, ts, flg).
#}

-- Generate base sequence of dates
with date_sequence as (
    select dateadd(day, seq4(), '1990-01-01'::date) as calendar_date
    from table(generator(rowcount => 20000))  -- ~55 years of dates
)
, retail_years as (
    select distinct
        year(calendar_date) as calendar_year
        , case
            when dayofweek(date_from_parts(year(calendar_date), 1, 1)) <= 3 then
                -- If Jan 1 is Sun(0), Mon(1), Tue(2), Wed(3) - go to previous Sunday
                dateadd(day, -dayofweek(date_from_parts(year(calendar_date), 1, 1))
                       , date_from_parts(year(calendar_date), 1, 1))
            else
                -- If Jan 1 is Thu(4), Fri(5), Sat(6) - go to next Sunday
                dateadd(day, 7 - dayofweek(date_from_parts(year(calendar_date), 1, 1))
                       , date_from_parts(year(calendar_date), 1, 1))
        end as retail_year_start
    from date_sequence
    where calendar_date between '1990-01-01' and '2045-12-31'
)
, retail_year_boundaries as (
    select
        calendar_year
        , retail_year_start
        , lead(retail_year_start, 1) over (order by calendar_year) - 1 as retail_year_end
        , datediff(week, retail_year_start
                , lead(retail_year_start, 1) over (order by calendar_year)) as weeks_in_year
    from retail_years
)
, week_num as (
        select seq4() + 1 as seq
        from table(generator(rowcount => 53))
    )
, retail_weeks as (
    select
        ryb.calendar_year as retail_year
        , ryb.retail_year_start
        , ryb.retail_year_end
        , ryb.weeks_in_year
        , row_number() over (partition by ryb.calendar_year order by week_num.seq) as retail_week_num
        , dateadd(week, week_num.seq - 1, ryb.retail_year_start) as retail_week_start
        , dateadd(day, 6, dateadd(week, week_num.seq - 1, ryb.retail_year_start)) as retail_week_end
    from retail_year_boundaries as ryb
    cross join week_num
    where week_num.seq <= ryb.weeks_in_year
)
, retail_periods as (
    select
        rw.*

        -- 445 Pattern: 4-4-5 weeks per month in each quarter
        , case
            when rw.retail_week_num <= 4 then 1
            when rw.retail_week_num <= 8 then 2
            when rw.retail_week_num <= 13 then 3
            when rw.retail_week_num <= 17 then 4
            when rw.retail_week_num <= 21 then 5
            when rw.retail_week_num <= 26 then 6
            when rw.retail_week_num <= 30 then 7
            when rw.retail_week_num <= 34 then 8
            when rw.retail_week_num <= 39 then 9
            when rw.retail_week_num <= 43 then 10
            when rw.retail_week_num <= 47 then 11
            else 12
        end as retail_month_445

        -- 454 Pattern: 4-5-4 weeks per month in each quarter
        , case
            when rw.retail_week_num <= 4 then 1
            when rw.retail_week_num <= 9 then 2
            when rw.retail_week_num <= 13 then 3
            when rw.retail_week_num <= 17 then 4
            when rw.retail_week_num <= 22 then 5
            when rw.retail_week_num <= 26 then 6
            when rw.retail_week_num <= 30 then 7
            when rw.retail_week_num <= 35 then 8
            when rw.retail_week_num <= 39 then 9
            when rw.retail_week_num <= 43 then 10
            when rw.retail_week_num <= 48 then 11
            else 12
        end as retail_month_454

        -- 544 Pattern: 5-4-4 weeks per month in each quarter
        , case
            when rw.retail_week_num <= 5 then 1
            when rw.retail_week_num <= 9 then 2
            when rw.retail_week_num <= 13 then 3
            when rw.retail_week_num <= 18 then 4
            when rw.retail_week_num <= 22 then 5
            when rw.retail_week_num <= 26 then 6
            when rw.retail_week_num <= 31 then 7
            when rw.retail_week_num <= 35 then 8
            when rw.retail_week_num <= 39 then 9
            when rw.retail_week_num <= 44 then 10
            when rw.retail_week_num <= 48 then 11
            else 12
        end as retail_month_544

    from retail_weeks as rw
)
, retail_dates as (
    select
        ds.calendar_date
        , to_char(ds.calendar_date, 'yyyymmdd')::int as date_key

        -- Standard calendar attributes
        , year(ds.calendar_date) as calendar_year
        , quarter(ds.calendar_date) as calendar_quarter
        , month(ds.calendar_date) as calendar_month
        , week(ds.calendar_date) as calendar_week
        , dayofweek(ds.calendar_date) as day_of_week
        , dayname(ds.calendar_date) as day_name

        -- Core retail calendar attributes (same for all patterns)
        , rp.retail_year
        , rp.retail_week_num
        , rp.retail_week_start
        , rp.retail_week_end
        , rp.weeks_in_year

        -- Month assignments for each pattern
        , rp.retail_month_445
        , rp.retail_month_454
        , rp.retail_month_544

        -- Week of month for each pattern
        , dense_rank() over (
            partition by rp.retail_year, rp.retail_month_445
            order by rp.retail_week_num
        ) as trade_week_of_month_445

        , dense_rank() over (
            partition by rp.retail_year, rp.retail_month_454
            order by rp.retail_week_num
        ) as trade_week_of_month_454

        , dense_rank() over (
            partition by rp.retail_year, rp.retail_month_544
            order by rp.retail_week_num
        ) as trade_week_of_month_544

        -- Common flags
        , coalesce(rp.retail_week_num = 53, false) as is_leap_week
        , datediff(day, rp.retail_year_start, ds.calendar_date) + 1 as retail_day_of_year

    from date_sequence as ds
    inner join retail_periods as rp
        on ds.calendar_date between rp.retail_week_start and rp.retail_week_end
)
, final as (
    select
        -- Keys
        date_key
        , calendar_date as full_dt
        , calendar_date - interval '1 year' as same_dt_last_year

        -- Standard calendar (using CDC abbreviations)
        , calendar_year as calendar_year_num
        , calendar_quarter as calendar_quarter_num
        , calendar_month as calendar_month_num
        , calendar_week as calendar_week_num
        , day_of_week as day_of_week_num
        , dayofweekiso(calendar_date) as iso_day_of_week_num
        , day_name as day_nm
        , left(day_name, 3) as day_abbr
        , case
            when day_of_week in (1, 7) then 'Weekend'
            else 'Weekday'
        end as weekday_flg
        , case
            when calendar_date = dateadd(day, 6, date_trunc('week', calendar_date)) then 1
            else 0
        end as end_of_week_flg

        -- Day suffixes and counters
        , case
            when mod(day(calendar_date), 10) = 1 and day(calendar_date) not in (11)
                then day(calendar_date)::varchar || 'st'
            when mod(day(calendar_date), 10) = 2 and day(calendar_date) not in (12)
                then day(calendar_date)::varchar || 'nd'
            when mod(day(calendar_date), 10) = 3 and day(calendar_date) not in (13)
                then day(calendar_date)::varchar || 'rd'
            else day(calendar_date)::varchar || 'th'
        end as day_suffix_txt
        , datediff('d', date('1970-01-01'), calendar_date) as day_overall_num

        -- Month details
        , monthname(calendar_date) as month_nm
        , left(monthname(calendar_date), 3) as month_abbr
        , month(calendar_date) as month_num
        , day(calendar_date) as day_of_month_num
        , datediff('month', date('1970-01-01'), calendar_date) as month_overall_num
        , mod(month(calendar_date) - 1, 3) + 1 as month_in_quarter_num
        , date_trunc('month', calendar_date) as first_day_of_month
        , last_day(calendar_date, 'month') as last_day_of_month
        , case when date_trunc('month', calendar_date) = calendar_date then 1 else 0 end as first_day_of_month_flg
        , case when last_day(calendar_date, 'month') = calendar_date then 1 else 0 end as end_of_month_flg

        -- Quarter details
        , case
            when quarter(calendar_date) = 1 then 'First'
            when quarter(calendar_date) = 2 then 'Second'
            when quarter(calendar_date) = 3 then 'Third'
            when quarter(calendar_date) = 4 then 'Fourth'
        end as quarter_nm
        , datediff(day, date_trunc('quarter', calendar_date), calendar_date) + 1 as day_of_quarter_num
        , date_trunc('quarter', calendar_date) as first_day_of_quarter
        , last_day(calendar_date, 'quarter') as last_day_of_quarter

        -- Year details
        , yearofweekiso(calendar_date) as iso_year_num
        , to_char(calendar_date, 'yyyymm')::int as yearmonth_num
        , date_trunc('year', calendar_date) as first_day_of_year_dt
        , dateadd(day, -1, dateadd(year, 1, date_trunc('year', calendar_date))) as last_day_of_year_dt
        , dayofyear(calendar_date) as day_of_year_num
        , case when month(calendar_date) = 12 and day(calendar_date) = 31 then 1 else 0 end as end_of_year_flg

        -- Week details  
        , weekofyear(calendar_date) as week_of_year_num
        , ceil(day(calendar_date) / 7.0) as week_of_month_num
        , yearofweekiso(calendar_date)::varchar || '-W'
            || lpad(weekiso(calendar_date)::varchar, 2, '0') || '-'
            || dayofweekiso(calendar_date)::varchar as iso_week_of_year_txt
        , datediff('w', date('1970-01-01'), calendar_date) as week_overall_num
        , dateadd(day, 1 - dayofweekiso(calendar_date), calendar_date) as week_begin_dt
        , to_char(dateadd(day, 1 - dayofweekiso(calendar_date), calendar_date), 'yyyymmdd')::int as week_begin_key
        , dateadd(day, 7 - dayofweekiso(calendar_date), calendar_date) as week_end_dt
        , to_char(dateadd(day, 7 - dayofweekiso(calendar_date), calendar_date), 'yyyymmdd')::int as week_end_key

        -- Other date formats
        , date_part(epoch_second, calendar_date) as epoch
        , to_char(calendar_date, 'yyyymmdd')::int as yyyymmdd

        -- Core retail calendar (same for all patterns, using CDC abbreviations)
        , retail_year as trade_year_num
        , retail_week_num as trade_week_num
        , retail_week_start as trade_week_start_dt
        , retail_week_end as trade_week_end_dt

        -- Trade months for each pattern
        , retail_month_445 as trade_month_445_num
        , retail_month_454 as trade_month_454_num
        , retail_month_544 as trade_month_544_num

        -- Trade quarters for each pattern
        , ceil(retail_month_445 / 3.0) as trade_quarter_445_num
        , ceil(retail_month_454 / 3.0) as trade_quarter_454_num
        , ceil(retail_month_544 / 3.0) as trade_quarter_544_num

        -- Trade quarter names for each pattern
        , case ceil(retail_month_445 / 3.0)
            when 1 then 'First'
            when 2 then 'Second'
            when 3 then 'Third'
            when 4 then 'Fourth'
        end as trade_quarter_445_nm
        , case ceil(retail_month_454 / 3.0)
            when 1 then 'First'
            when 2 then 'Second'
            when 3 then 'Third'
            when 4 then 'Fourth'
        end as trade_quarter_454_nm
        , case ceil(retail_month_544 / 3.0)
            when 1 then 'First'
            when 2 then 'Second'
            when 3 then 'Third'
            when 4 then 'Fourth'
        end as trade_quarter_544_nm

        -- Trade week of month for each pattern (already has _num suffix implicitly)
        , trade_week_of_month_445 as trade_week_of_month_445_num
        , trade_week_of_month_454 as trade_week_of_month_454_num
        , trade_week_of_month_544 as trade_week_of_month_544_num

        -- Trade week of quarter for each pattern
        , dense_rank() over (
            partition by retail_year, ceil(retail_month_445 / 3.0)
            order by retail_week_num
        ) as trade_week_of_quarter_445_num
        , dense_rank() over (
            partition by retail_year, ceil(retail_month_454 / 3.0)
            order by retail_week_num
        ) as trade_week_of_quarter_454_num
        , dense_rank() over (
            partition by retail_year, ceil(retail_month_544 / 3.0)
            order by retail_week_num
        ) as trade_week_of_quarter_544_num

        -- Trade week of year (same for all patterns)
        , retail_week_num as trade_week_of_year_num

        -- Trade month names for each pattern
        , case retail_month_445
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as trade_month_445_nm
        , case retail_month_454
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as trade_month_454_nm
        , case retail_month_544
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as trade_month_544_nm

        -- Common attributes (using CDC abbreviations)
        , is_leap_week as is_leap_week_flg
        , weeks_in_year as weeks_in_trade_year_num
        , retail_day_of_year as trade_day_of_year_num

        -- ETL metadata (using CDC abbreviations)
        , false as dw_deleted_flg
        , current_timestamp as dw_synced_ts
        , 'dim_trade_date' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp

    from retail_dates
    where calendar_date between '1995-01-01' and '2040-12-31'
)
select * from final
