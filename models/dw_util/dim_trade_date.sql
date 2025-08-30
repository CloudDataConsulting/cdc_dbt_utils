{{ config(materialized='table') }}

with date_sequence as ( select
        dateadd(day, seq4(), '1990-01-01'::date) as calendar_date
    from table(generator(rowcount => 20000))
)
, trade_years as (
    select distinct
        year(calendar_date) as calendar_year
        , case
            when dayofweek(date_from_parts(year(calendar_date), 1, 1)) <= 3 then
                dateadd(day, -dayofweek(date_from_parts(year(calendar_date), 1, 1))
                       , date_from_parts(year(calendar_date), 1, 1))
            else
                dateadd(day, 7 - dayofweek(date_from_parts(year(calendar_date), 1, 1))
                       , date_from_parts(year(calendar_date), 1, 1))
        end as trade_year_start
    from date_sequence
    where calendar_date between '1990-01-01' and '2045-12-31')
, trade_year_boundaries as (
    select
        calendar_year
        , trade_year_start
        , lead(trade_year_start, 1) over (order by calendar_year) - 1 as trade_year_end
        , datediff(week, trade_year_start,
                lead(trade_year_start, 1) over (order by calendar_year)) as weeks_in_year
    from trade_years)
, trade_weeks as (
    select
        ryb.calendar_year as trade_year_num
        , ryb.trade_year_start
        , ryb.trade_year_end
        , ryb.weeks_in_year
        , row_number() over (partition by ryb.calendar_year order by week_num.seq) as trade_week_num
        , dateadd(week, week_num.seq - 1, ryb.trade_year_start) as trade_week_start_dt
        , dateadd(day, 6, dateadd(week, week_num.seq - 1, ryb.trade_year_start)) as trade_week_end_dt
    from trade_year_boundaries ryb
    cross join (
        select seq4() + 1 as seq 
        from table(generator(rowcount => 53))) week_num
    where week_num.seq <= ryb.weeks_in_year)
, trade_periods as (
    select
        rw.*
        , case 
            when trade_week_num <= 4 then 1
            when trade_week_num <= 8 then 2
            when trade_week_num <= 13 then 3
            when trade_week_num <= 17 then 4
            when trade_week_num <= 21 then 5
            when trade_week_num <= 26 then 6
            when trade_week_num <= 30 then 7
            when trade_week_num <= 34 then 8
            when trade_week_num <= 39 then 9
            when trade_week_num <= 43 then 10
            when trade_week_num <= 47 then 11
            else 12
        end as trade_month_445_num
        , case 
            when trade_week_num <= 4 then 1
            when trade_week_num <= 9 then 2
            when trade_week_num <= 13 then 3
            when trade_week_num <= 17 then 4
            when trade_week_num <= 22 then 5
            when trade_week_num <= 26 then 6
            when trade_week_num <= 30 then 7
            when trade_week_num <= 35 then 8
            when trade_week_num <= 39 then 9
            when trade_week_num <= 43 then 10
            when trade_week_num <= 48 then 11
            else 12
        end as trade_month_454_num
        , case 
            when trade_week_num <= 5 then 1
            when trade_week_num <= 9 then 2
            when trade_week_num <= 13 then 3
            when trade_week_num <= 18 then 4
            when trade_week_num <= 22 then 5
            when trade_week_num <= 26 then 6
            when trade_week_num <= 31 then 7
            when trade_week_num <= 35 then 8
            when trade_week_num <= 39 then 9
            when trade_week_num <= 44 then 10
            when trade_week_num <= 48 then 11
            else 12
        end as trade_month_544_num
    from trade_weeks rw)
, trade_month_boundaries as (
    select distinct
        trade_year_num
        , trade_month_445_num
        , trade_month_454_num
        , trade_month_544_num
        , min(trade_week_start_dt) over (partition by trade_year_num, trade_month_445_num) as trade_month_445_start_dt
        , max(trade_week_end_dt) over (partition by trade_year_num, trade_month_445_num) as trade_month_445_end_dt
        , min(trade_week_start_dt) over (partition by trade_year_num, trade_month_454_num) as trade_month_454_start_dt
        , max(trade_week_end_dt) over (partition by trade_year_num, trade_month_454_num) as trade_month_454_end_dt
        , min(trade_week_start_dt) over (partition by trade_year_num, trade_month_544_num) as trade_month_544_start_dt
        , max(trade_week_end_dt) over (partition by trade_year_num, trade_month_544_num) as trade_month_544_end_dt
    from trade_periods
)
, trade_quarter_boundaries as (
    select distinct
        trade_year_num
        , ceil(trade_month_445_num / 3.0) as trade_quarter_num
        , min(trade_week_start_dt) over (partition by trade_year_num, ceil(trade_month_445_num / 3.0)) as trade_quarter_start_dt
        , max(trade_week_end_dt) over (partition by trade_year_num, ceil(trade_month_445_num / 3.0)) as trade_quarter_end_dt
    from trade_periods
)
, trade_dates as (
    select
        ds.calendar_date
        , to_char(ds.calendar_date, 'yyyymmdd')::int as date_key
        , year(ds.calendar_date) as calendar_year
        , quarter(ds.calendar_date) as calendar_quarter
        , month(ds.calendar_date) as calendar_month
        -- Calculate week number within year (Sunday-Saturday weeks)
        -- Week 1 starts with the first Sunday of the year or Jan 1 if it's a Sunday
        , floor(datediff(day, 
            date_trunc('year', ds.calendar_date) - dayofweek(date_trunc('year', ds.calendar_date)), 
            ds.calendar_date - dayofweek(ds.calendar_date)) / 7) + 1 as calendar_week
        , dayofweek(ds.calendar_date) as day_of_week
        , dayname(ds.calendar_date) as day_name
        , rp.trade_year_num
        , rp.trade_week_num
        , rp.trade_week_start_dt
        , rp.trade_week_end_dt
        , rp.trade_year_start
        , rp.trade_year_end
        , rp.weeks_in_year as weeks_in_trade_year_num
        , rp.trade_month_445_num
        , rp.trade_month_454_num
        , rp.trade_month_544_num
        , dense_rank() over (
            partition by rp.trade_year_num, rp.trade_month_445_num 
            order by rp.trade_week_num) as trade_week_of_month_445_num
        , dense_rank() over (
            partition by rp.trade_year_num, rp.trade_month_454_num 
            order by rp.trade_week_num) as trade_week_of_month_454_num
        , dense_rank() over (
            partition by rp.trade_year_num, rp.trade_month_544_num 
            order by rp.trade_week_num) as trade_week_of_month_544_num
        , case when rp.trade_week_num = 53 then true else false end as is_trade_leap_week_flg
        , datediff(day, rp.trade_year_start, ds.calendar_date) + 1 as trade_day_of_year_num
        , mb.trade_month_445_start_dt
        , mb.trade_month_445_end_dt
        , mb.trade_month_454_start_dt
        , mb.trade_month_454_end_dt
        , mb.trade_month_544_start_dt
        , mb.trade_month_544_end_dt
        , qb.trade_quarter_start_dt
        , qb.trade_quarter_end_dt
    from date_sequence ds
    inner join trade_periods rp
        on ds.calendar_date between rp.trade_week_start_dt and rp.trade_week_end_dt
    left join trade_month_boundaries mb
        on rp.trade_year_num = mb.trade_year_num
        and rp.trade_month_445_num = mb.trade_month_445_num
        and rp.trade_month_454_num = mb.trade_month_454_num
        and rp.trade_month_544_num = mb.trade_month_544_num
    left join trade_quarter_boundaries qb
        on rp.trade_year_num = qb.trade_year_num
        and ceil(rp.trade_month_445_num / 3.0) = qb.trade_quarter_num)
, trade_year_comparison as (
    select
        td.calendar_date
        , td.date_key
        , td.trade_year_num
        , td.trade_week_num
        , td.day_of_week
        -- Find the same trade week/day from the previous trade year
        , ly.date_key as trade_date_last_year_key
    from trade_dates td
    left join trade_dates ly
        on ly.trade_year_num = td.trade_year_num - 1
        and ly.trade_week_num = td.trade_week_num
        and ly.day_of_week = td.day_of_week
)
, final as (
    select
        -- Primary Key
        trade_dates.date_key
        
        -- DAY Level (Most Granular)
        -- Core day identifiers
        , trade_dates.calendar_date as calendar_full_dt
        , trade_dates.calendar_date as trade_full_dt  -- same as calendar_full_dt
        , to_char(trade_dates.calendar_date - interval '1 year', 'yyyymmdd')::int as calendar_date_last_year_key
        , tyc.trade_date_last_year_key
        
        -- Day position metrics
        , trade_dates.day_of_week as calendar_day_of_week_num
        , dayofweekiso(trade_dates.calendar_date) as iso_day_of_week_num
        , day(trade_dates.calendar_date) as calendar_day_of_month_num
        , datediff(day, date_trunc('quarter', trade_dates.calendar_date), trade_dates.calendar_date) + 1 as calendar_day_of_quarter_num
        , dayofyear(trade_dates.calendar_date) as calendar_day_of_year_num
        , trade_dates.trade_day_of_year_num
        , datediff('d', date('1970-01-01'), trade_dates.calendar_date) as calendar_day_overall_num
        
        -- Day descriptors
        , case dayofweek(trade_dates.calendar_date)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as calendar_day_nm
        , trade_dates.day_name as calendar_day_abbr
        , case
            when mod(day(trade_dates.calendar_date), 10) = 1 and day(trade_dates.calendar_date) not in (11)
                then day(trade_dates.calendar_date)::varchar || 'st'
            when mod(day(trade_dates.calendar_date), 10) = 2 and day(trade_dates.calendar_date) not in (12)
                then day(trade_dates.calendar_date)::varchar || 'nd'
            when mod(day(trade_dates.calendar_date), 10) = 3 and day(trade_dates.calendar_date) not in (13)
                then day(trade_dates.calendar_date)::varchar || 'rd'
            else day(trade_dates.calendar_date)::varchar || 'th'
        end as calendar_day_suffix_txt
        , date_part(epoch_second, trade_dates.calendar_date) as calendar_epoch_num
        
        -- Day flags
        , case 
            when trade_dates.day_of_week in (0, 6) then 'Weekend'
            else 'Weekday'
        end as calendar_weekday_flg
        , case
            when trade_dates.calendar_date = dateadd(day, 6, date_trunc('week', trade_dates.calendar_date)) then 1
            else 0
        end as calendar_last_day_of_week_flg
        , case when date_trunc('month', trade_dates.calendar_date) = trade_dates.calendar_date then 1 else 0 end as calendar_first_day_of_month_flg
        , case when last_day(trade_dates.calendar_date, 'month') = trade_dates.calendar_date then 1 else 0 end as calendar_last_day_of_month_flg
        , case when last_day(trade_dates.calendar_date, 'quarter') = trade_dates.calendar_date then 1 else 0 end as calendar_last_day_of_quarter_flg
        , case when month(trade_dates.calendar_date) = 12 and day(trade_dates.calendar_date) = 31 then 1 else 0 end as calendar_last_day_of_year_flg
        
        -- WEEK Level
        -- Week numbers
        , trade_dates.calendar_week as calendar_week_num
        , trade_dates.trade_week_num
        , trade_dates.calendar_week as calendar_week_of_year_num
        , trade_dates.trade_week_num as trade_week_of_year_num
        , ceil(day(trade_dates.calendar_date) / 7.0) as calendar_week_of_month_num
        , trade_dates.trade_week_of_month_445_num
        , trade_dates.trade_week_of_month_454_num
        , trade_dates.trade_week_of_month_544_num
        , dense_rank() over (
            partition by year(trade_dates.calendar_date), quarter(trade_dates.calendar_date)
            order by trade_dates.calendar_week) as calendar_week_of_quarter_num
        , dense_rank() over (
            partition by trade_dates.trade_year_num, ceil(trade_dates.trade_month_445_num / 3.0)
            order by trade_dates.trade_week_num) as trade_week_of_quarter_num
        , datediff('w', date('1970-01-01'), dateadd(day, -dayofweek(trade_dates.calendar_date), trade_dates.calendar_date)) as calendar_week_overall_num
        , datediff('w', date('1970-01-01'), trade_dates.trade_week_start_dt) as trade_week_overall_num
        
        -- Week boundaries
        , dateadd(day, -dayofweek(trade_dates.calendar_date), trade_dates.calendar_date) as calendar_week_start_dt
        , trade_dates.trade_week_start_dt
        , to_char(dateadd(day, -dayofweek(trade_dates.calendar_date), trade_dates.calendar_date), 'yyyymmdd')::int as calendar_week_start_key
        , to_char(trade_dates.trade_week_start_dt, 'yyyymmdd')::int as trade_week_start_key
        , dateadd(day, 6 - dayofweek(trade_dates.calendar_date), trade_dates.calendar_date) as calendar_week_end_dt
        , trade_dates.trade_week_end_dt
        , to_char(dateadd(day, 6 - dayofweek(trade_dates.calendar_date), trade_dates.calendar_date), 'yyyymmdd')::int as calendar_week_end_key
        , to_char(trade_dates.trade_week_end_dt, 'yyyymmdd')::int as trade_week_end_key
        
        -- MONTH Level
        -- Month identifiers
        , trade_dates.calendar_month as calendar_month_num
        , trade_dates.trade_month_445_num
        , trade_dates.trade_month_454_num
        , trade_dates.trade_month_544_num
        , case month(trade_dates.calendar_date)
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as calendar_month_nm
        , case trade_dates.trade_month_445_num
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as trade_month_445_nm
        , case trade_dates.trade_month_454_num
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as trade_month_454_nm
        , case trade_dates.trade_month_544_num
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as trade_month_544_nm
        , monthname(trade_dates.calendar_date) as calendar_month_abbr
        , case trade_dates.trade_month_445_num
            when 1 then 'Jan' when 2 then 'Feb' when 3 then 'Mar'
            when 4 then 'Apr' when 5 then 'May' when 6 then 'Jun'
            when 7 then 'Jul' when 8 then 'Aug' when 9 then 'Sep'
            when 10 then 'Oct' when 11 then 'Nov' when 12 then 'Dec'
        end as trade_month_abbr
        
        -- Month metrics
        , mod(month(trade_dates.calendar_date) - 1, 3) + 1 as calendar_month_in_quarter_num
        , datediff('month', date('1970-01-01'), trade_dates.calendar_date) as calendar_month_overall_num
        , (trade_dates.trade_year_num - 1990) * 12 + trade_dates.trade_month_445_num as trade_month_overall_num
        , to_char(trade_dates.calendar_date, 'yyyymm')::int as calendar_yearmonth_num
        , (trade_dates.trade_year_num * 100) + trade_dates.trade_month_445_num as trade_yearmonth_num
        
        -- Month boundaries
        , date_trunc('month', trade_dates.calendar_date) as calendar_month_start_dt
        , trade_dates.trade_month_445_start_dt
        , trade_dates.trade_month_454_start_dt
        , trade_dates.trade_month_544_start_dt
        , to_char(date_trunc('month', trade_dates.calendar_date), 'yyyymmdd')::int as calendar_month_start_key
        , to_char(trade_dates.trade_month_445_start_dt, 'yyyymmdd')::int as trade_month_445_start_key
        , to_char(trade_dates.trade_month_454_start_dt, 'yyyymmdd')::int as trade_month_454_start_key
        , to_char(trade_dates.trade_month_544_start_dt, 'yyyymmdd')::int as trade_month_544_start_key
        , last_day(trade_dates.calendar_date, 'month') as calendar_month_end_dt
        , trade_dates.trade_month_445_end_dt
        , trade_dates.trade_month_454_end_dt
        , trade_dates.trade_month_544_end_dt
        , to_char(last_day(trade_dates.calendar_date, 'month'), 'yyyymmdd')::int as calendar_month_end_key
        , to_char(trade_dates.trade_month_445_end_dt, 'yyyymmdd')::int as trade_month_445_end_key
        , to_char(trade_dates.trade_month_454_end_dt, 'yyyymmdd')::int as trade_month_454_end_key
        , to_char(trade_dates.trade_month_544_end_dt, 'yyyymmdd')::int as trade_month_544_end_key
        
        -- QUARTER Level
        -- Quarter identifiers
        , trade_dates.calendar_quarter as calendar_quarter_num
        , ceil(trade_dates.trade_month_445_num / 3.0) as trade_quarter_num
        , case
            when quarter(trade_dates.calendar_date) = 1 then 'First'
            when quarter(trade_dates.calendar_date) = 2 then 'Second'
            when quarter(trade_dates.calendar_date) = 3 then 'Third'
            when quarter(trade_dates.calendar_date) = 4 then 'Fourth'
        end as calendar_quarter_nm
        , case ceil(trade_dates.trade_month_445_num / 3.0)
            when 1 then 'First'
            when 2 then 'Second'
            when 3 then 'Third'
            when 4 then 'Fourth'
        end as trade_quarter_nm
        
        -- Quarter boundaries
        , date_trunc('quarter', trade_dates.calendar_date) as calendar_quarter_start_dt
        , trade_dates.trade_quarter_start_dt
        , to_char(date_trunc('quarter', trade_dates.calendar_date), 'yyyymmdd')::int as calendar_quarter_start_key
        , to_char(trade_dates.trade_quarter_start_dt, 'yyyymmdd')::int as trade_quarter_start_key
        , last_day(trade_dates.calendar_date, 'quarter') as calendar_quarter_end_dt
        , trade_dates.trade_quarter_end_dt
        , to_char(last_day(trade_dates.calendar_date, 'quarter'), 'yyyymmdd')::int as calendar_quarter_end_key
        , to_char(trade_dates.trade_quarter_end_dt, 'yyyymmdd')::int as trade_quarter_end_key
        
        -- YEAR Level
        -- Year identifiers
        , trade_dates.calendar_year as calendar_year_num
        , trade_dates.trade_year_num
        
        -- Year boundaries
        , date_trunc('year', trade_dates.calendar_date) as calendar_year_start_dt
        , trade_dates.trade_year_start as trade_year_start_dt
        , to_char(date_trunc('year', trade_dates.calendar_date), 'yyyymmdd')::int as calendar_year_start_key
        , to_char(trade_dates.trade_year_start, 'yyyymmdd')::int as trade_year_start_key
        , dateadd(day, -1, dateadd(year, 1, date_trunc('year', trade_dates.calendar_date))) as calendar_year_end_dt
        , trade_dates.trade_year_end as trade_year_end_dt
        , to_char(dateadd(day, -1, dateadd(year, 1, date_trunc('year', trade_dates.calendar_date))), 'yyyymmdd')::int as calendar_year_end_key
        , to_char(trade_dates.trade_year_end, 'yyyymmdd')::int as trade_year_end_key
        
        -- Year flags and metrics
        , case 
            when (year(trade_dates.calendar_date) % 4 = 0 and year(trade_dates.calendar_date) % 100 != 0) 
                or (year(trade_dates.calendar_date) % 400 = 0) 
            then 1 else 0 
        end as calendar_is_leap_year_flg
        , trade_dates.is_trade_leap_week_flg
        , trade_dates.weeks_in_trade_year_num
        
        -- ISO Columns (Special Group)
        , yearofweekiso(trade_dates.calendar_date) as iso_year_num
        , yearofweekiso(trade_dates.calendar_date)::varchar || '-W' || 
            lpad(weekiso(trade_dates.calendar_date)::varchar, 2, '0') || '-' || 
            dayofweekiso(trade_dates.calendar_date)::varchar as iso_week_of_year_txt
        , datediff('w', date('1970-01-01'), dateadd(day, 1 - dayofweekiso(trade_dates.calendar_date), trade_dates.calendar_date)) as iso_week_overall_num
        , dateadd(day, 1 - dayofweekiso(trade_dates.calendar_date), trade_dates.calendar_date) as iso_week_start_dt
        , to_char(dateadd(day, 1 - dayofweekiso(trade_dates.calendar_date), trade_dates.calendar_date), 'yyyymmdd')::int as iso_week_start_key
        , dateadd(day, 7 - dayofweekiso(trade_dates.calendar_date), trade_dates.calendar_date) as iso_week_end_dt
        , to_char(dateadd(day, 7 - dayofweekiso(trade_dates.calendar_date), trade_dates.calendar_date), 'yyyymmdd')::int as iso_week_end_key
        
        -- Metadata Columns
        , current_timestamp as dw_synced_ts
        , 'dim_trade_date' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from trade_dates
    left join trade_year_comparison tyc
        on trade_dates.date_key = tyc.date_key
    where trade_dates.calendar_date between '1995-01-01' and '2040-12-31')
select * from final