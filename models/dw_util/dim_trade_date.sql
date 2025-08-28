{{ config(materialized='table') }}

with date_sequence as ( select
        dateadd(day, seq4(), '1990-01-01'::date) as calendar_date
    from table(generator(rowcount => 20000))
)
, retail_years as (
    select distinct
        year(calendar_date) as calendar_year
        , case
            when dayofweek(date_from_parts(year(calendar_date), 1, 1)) <= 3 then
                dateadd(day, -dayofweek(date_from_parts(year(calendar_date), 1, 1))
                       , date_from_parts(year(calendar_date), 1, 1))
            else
                dateadd(day, 7 - dayofweek(date_from_parts(year(calendar_date), 1, 1))
                       , date_from_parts(year(calendar_date), 1, 1))
        end as retail_year_start
    from date_sequence
    where calendar_date between '1990-01-01' and '2045-12-31')
, retail_year_boundaries as (
    select
        calendar_year
        , retail_year_start
        , lead(retail_year_start, 1) over (order by calendar_year) - 1 as retail_year_end
        , datediff(week, retail_year_start,
                lead(retail_year_start, 1) over (order by calendar_year)) as weeks_in_year
    from retail_years)
, retail_weeks as (
    select
        ryb.calendar_year as retail_year
        , ryb.retail_year_start
        , ryb.retail_year_end
        , ryb.weeks_in_year
        , row_number() over (partition by ryb.calendar_year order by week_num.seq) as retail_week_num
        , dateadd(week, week_num.seq - 1, ryb.retail_year_start) as retail_week_start
        , dateadd(day, 6, dateadd(week, week_num.seq - 1, ryb.retail_year_start)) as retail_week_end
    from retail_year_boundaries ryb
    cross join (
        select seq4() + 1 as seq 
        from table(generator(rowcount => 53))) week_num
    where week_num.seq <= ryb.weeks_in_year)
, retail_periods as (
    select
        rw.*
        , case 
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
        end as retail_month_445
        , case 
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
        end as retail_month_454
        , case 
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
    from retail_weeks rw)
, retail_dates as (
    select
        ds.calendar_date
        , to_char(ds.calendar_date, 'yyyymmdd')::int as date_key
        , year(ds.calendar_date) as calendar_year
        , quarter(ds.calendar_date) as calendar_quarter
        , month(ds.calendar_date) as calendar_month
        , week(ds.calendar_date) as calendar_week
        , dayofweek(ds.calendar_date) as day_of_week
        , dayname(ds.calendar_date) as day_name
        , rp.retail_year
        , rp.retail_week_num
        , rp.retail_week_start
        , rp.retail_week_end
        , rp.weeks_in_year
        , rp.retail_month_445
        , rp.retail_month_454
        , rp.retail_month_544
        , dense_rank() over (
            partition by rp.retail_year, rp.retail_month_445 
            order by rp.retail_week_num) as trade_week_of_month_445
        , dense_rank() over (
            partition by rp.retail_year, rp.retail_month_454 
            order by rp.retail_week_num) as trade_week_of_month_454
        , dense_rank() over (
            partition by rp.retail_year, rp.retail_month_544 
            order by rp.retail_week_num) as trade_week_of_month_544
        , case when rp.retail_week_num = 53 then true else false end as is_leap_week
        , datediff(day, rp.retail_year_start, ds.calendar_date) + 1 as retail_day_of_year
    from date_sequence ds
    inner join retail_periods rp
        on ds.calendar_date between rp.retail_week_start and rp.retail_week_end)
, final as (
    select
        date_key
        , calendar_date as full_dt
        , calendar_date - interval '1 year' as same_dt_last_year
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
        , case
            when quarter(calendar_date) = 1 then 'First'
            when quarter(calendar_date) = 2 then 'Second'
            when quarter(calendar_date) = 3 then 'Third'
            when quarter(calendar_date) = 4 then 'Fourth'
        end as quarter_nm
        , datediff(day, date_trunc('quarter', calendar_date), calendar_date) + 1 as day_of_quarter_num
        , date_trunc('quarter', calendar_date) as first_day_of_quarter
        , last_day(calendar_date, 'quarter') as last_day_of_quarter
        , yearofweekiso(calendar_date) as iso_year_num
        , to_char(calendar_date, 'yyyymm')::int as yearmonth_num
        , date_trunc('year', calendar_date) as first_day_of_year_dt
        , dateadd(day, -1, dateadd(year, 1, date_trunc('year', calendar_date))) as last_day_of_year_dt
        , dayofyear(calendar_date) as day_of_year_num
        , case when month(calendar_date) = 12 and day(calendar_date) = 31 then 1 else 0 end as end_of_year_flg
        , weekofyear(calendar_date) as week_of_year_num
        , ceil(day(calendar_date) / 7.0) as week_of_month_num
        , yearofweekiso(calendar_date)::varchar || '-W' || 
            lpad(weekiso(calendar_date)::varchar, 2, '0') || '-' || 
            dayofweekiso(calendar_date)::varchar as iso_week_of_year_txt
        , datediff('w', date('1970-01-01'), calendar_date) as week_overall_num
        , dateadd(day, 1 - dayofweekiso(calendar_date), calendar_date) as week_begin_dt
        , to_char(dateadd(day, 1 - dayofweekiso(calendar_date), calendar_date), 'yyyymmdd')::int as week_begin_key
        , dateadd(day, 7 - dayofweekiso(calendar_date), calendar_date) as week_end_dt
        , to_char(dateadd(day, 7 - dayofweekiso(calendar_date), calendar_date), 'yyyymmdd')::int as week_end_key
        , date_part(epoch_second, calendar_date) as epoch
        , to_char(calendar_date, 'yyyymmdd')::int as yyyymmdd
        , retail_year as trade_year_num
        , retail_week_num as trade_week_num
        , retail_week_start as trade_week_start_dt
        , retail_week_end as trade_week_end_dt
        , retail_month_445 as trade_month_445_num
        , retail_month_454 as trade_month_454_num
        , retail_month_544 as trade_month_544_num
        , ceil(retail_month_445 / 3.0) as trade_quarter_445_num
        , ceil(retail_month_454 / 3.0) as trade_quarter_454_num
        , ceil(retail_month_544 / 3.0) as trade_quarter_544_num
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
        , trade_week_of_month_445 as trade_week_of_month_445_num
        , trade_week_of_month_454 as trade_week_of_month_454_num
        , trade_week_of_month_544 as trade_week_of_month_544_num
        , dense_rank() over (
            partition by retail_year, ceil(retail_month_445 / 3.0)
            order by retail_week_num) as trade_week_of_quarter_445_num
        , dense_rank() over (
            partition by retail_year, ceil(retail_month_454 / 3.0)
            order by retail_week_num) as trade_week_of_quarter_454_num
        , dense_rank() over (
            partition by retail_year, ceil(retail_month_544 / 3.0)
            order by retail_week_num) as trade_week_of_quarter_544_num
        , retail_week_num as trade_week_of_year_num
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
        , is_leap_week as is_leap_week_flg
        , weeks_in_year as weeks_in_trade_year_num
        , retail_day_of_year as trade_day_of_year_num
        , false as dw_deleted_flg
        , current_timestamp as dw_synced_ts
        , 'dim_trade_date' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from retail_dates
    where calendar_date between '1995-01-01' and '2040-12-31')
select * from final