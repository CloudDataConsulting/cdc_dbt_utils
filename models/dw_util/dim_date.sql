{{ config(materialized='table') }}
with date_spine as (
    select dateadd(day, seq4(), '2000-01-01'::date) as full_dt
    from table(generator(rowcount => 11323))  -- 31 years of dates
    where dateadd(day, seq4(), '2000-01-01'::date) <= '2030-12-31'::date)
, date_with_attributes as (
    select
        full_dt
        , to_char(full_dt, 'YYYYMMDD')::int as date_key
        , year(full_dt) as year_num
        , month(full_dt) as month_num
        , day(full_dt) as day_of_month_num
        , dayofweek(full_dt) as day_of_week_num_raw  -- 0=Sunday in Snowflake
        , dayofweek(full_dt) + 1 as day_of_week_num  -- Convert to 1=Sunday, 7=Saturday
        , dayofyear(full_dt) as day_of_year_num
        , quarter(full_dt) as quarter_num
        -- Find the Sunday on or before Jan 1 (start of week 1)
        , date_from_parts(year(full_dt), 1, 1) - dayofweek(date_from_parts(year(full_dt), 1, 1)) as week1_start_dt
    from date_spine)
, date_with_weeks as (
    select
        *
        -- Calculate week number: Week 1 contains Jan 1
        , floor(datediff('day', week1_start_dt, full_dt - mod(day_of_week_num_raw, 7)) / 7) + 1 as week_num
        , full_dt - mod(day_of_week_num_raw, 7) as week_start_dt  -- Sunday of current week
        , full_dt - mod(day_of_week_num_raw, 7) + 6 as week_end_dt  -- Saturday of current week
    from date_with_attributes)
, enriched_dates as (
    select
        date_key
        , full_dt
        -- Previous year date
        , dateadd(year, -1, full_dt) as date_last_year_dt
        , to_char(dateadd(year, -1, full_dt), 'YYYYMMDD')::int as date_last_year_key
        -- Day attributes
        , day_of_week_num
        , day_of_month_num
        , datediff('day', date_trunc('quarter', full_dt), full_dt) + 1 as day_of_quarter_num
        , day_of_year_num
        , datediff('day', '2000-01-01'::date, full_dt) + 1 as day_overall_num
        -- Day names and text
        , case day_of_week_num
            when 1 then 'Sunday'
            when 2 then 'Monday'
            when 3 then 'Tuesday'
            when 4 then 'Wednesday'
            when 5 then 'Thursday'
            when 6 then 'Friday'
            when 7 then 'Saturday'
        end as day_nm
        , case day_of_week_num
            when 1 then 'Sun'
            when 2 then 'Mon'
            when 3 then 'Tue'
            when 4 then 'Wed'
            when 5 then 'Thu'
            when 6 then 'Fri'
            when 7 then 'Sat'
        end as day_abbr
        , case
            when day_of_month_num in (1, 21, 31) then 'st'
            when day_of_month_num in (2, 22) then 'nd'
            when day_of_month_num in (3, 23) then 'rd'
            else 'th'
        end as day_suffix_txt
        -- Unix epoch
        , date_part(epoch_second, full_dt)::int as epoch_num
        -- Day flags
        , case when day_of_week_num between 2 and 6 then 1 else 0 end as weekday_flg
        , case when day_of_week_num = 7 then 1 else 0 end as last_day_of_week_flg
        , case when day_of_month_num = 1 then 1 else 0 end as first_day_of_month_flg
        , case when full_dt = last_day(full_dt, 'month') then 1 else 0 end as last_day_of_month_flg
        , case when full_dt = last_day(full_dt, 'quarter') then 1 else 0 end as last_day_of_quarter_flg
        , case when month_num = 12 and day_of_month_num = 31 then 1 else 0 end as last_day_of_year_flg
        -- Week attributes
        , week_num
        , week_num as week_of_year_num
        , ceil(day_of_month_num / 7.0)::int as week_of_month_num
        , floor((day_of_quarter_num - 1) / 7) + 1 as week_of_quarter_num
        , datediff('week', '2000-01-01'::date - dayofweek('2000-01-01'::date), week_start_dt) + 1 as week_overall_num
        , week_start_dt
        , to_char(week_start_dt, 'YYYYMMDD')::int as week_start_key
        , week_end_dt
        , to_char(week_end_dt, 'YYYYMMDD')::int as week_end_key
        -- Month attributes
        , month_num
        , case month_num
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
        end as month_nm
        , case month_num
            when 1 then 'Jan'
            when 2 then 'Feb'
            when 3 then 'Mar'
            when 4 then 'Apr'
            when 5 then 'May'
            when 6 then 'Jun'
            when 7 then 'Jul'
            when 8 then 'Aug'
            when 9 then 'Sep'
            when 10 then 'Oct'
            when 11 then 'Nov'
            when 12 then 'Dec'
        end as month_abbr
        , mod(month_num - 1, 3) + 1 as month_in_quarter_num
        , (year_num - 2000) * 12 + month_num as month_overall_num
        , year_num * 100 + month_num as yearmonth_num
        , date_trunc('month', full_dt)::date as month_start_dt
        , to_char(date_trunc('month', full_dt), 'YYYYMMDD')::int as month_start_key
        , last_day(full_dt, 'month')::date as month_end_dt
        , to_char(last_day(full_dt, 'month'), 'YYYYMMDD')::int as month_end_key
        -- Quarter attributes
        , quarter_num
        , 'Q' || quarter_num::varchar as quarter_nm
        , date_trunc('quarter', full_dt)::date as quarter_start_dt
        , to_char(date_trunc('quarter', full_dt), 'YYYYMMDD')::int as quarter_start_key
        , last_day(full_dt, 'quarter')::date as quarter_end_dt
        , to_char(last_day(full_dt, 'quarter'), 'YYYYMMDD')::int as quarter_end_key
        -- Year attributes
        , year_num
        , date_from_parts(year_num, 1, 1) as year_start_dt
        , to_char(date_from_parts(year_num, 1, 1), 'YYYYMMDD')::int as year_start_key
        , date_from_parts(year_num, 12, 31) as year_end_dt
        , to_char(date_from_parts(year_num, 12, 31), 'YYYYMMDD')::int as year_end_key
        , case
            when mod(year_num, 400) = 0 then 1
            when mod(year_num, 100) = 0 then 0
            when mod(year_num, 4) = 0 then 1
            else 0
        end as leap_year_flg
        -- ISO week attributes
        , dayofweekiso(full_dt) as iso_day_of_week_num
        , yearofweekiso(full_dt) as iso_year_num
        , 'W' || lpad(weekiso(full_dt)::varchar, 2, '0') as iso_week_of_year_txt
        , (yearofweekiso(full_dt) - 2000) * 53 + weekiso(full_dt) as iso_week_overall_num
        , dateadd('day', 1 - dayofweekiso(full_dt), full_dt)::date as iso_week_start_dt
        , to_char(dateadd('day', 1 - dayofweekiso(full_dt), full_dt), 'YYYYMMDD')::int as iso_week_start_key
        , dateadd('day', 7 - dayofweekiso(full_dt), full_dt)::date as iso_week_end_dt
        , to_char(dateadd('day', 7 - dayofweekiso(full_dt), full_dt), 'YYYYMMDD')::int as iso_week_end_key
    from date_with_weeks)
, regular_dates as (
    select
        date_key
        , full_dt
        , date_last_year_dt
        , date_last_year_key
        , day_of_week_num
        , day_of_month_num
        , day_of_quarter_num
        , day_of_year_num
        , day_overall_num
        , day_nm
        , day_abbr
        , day_suffix_txt
        , epoch_num
        , weekday_flg
        , last_day_of_week_flg
        , first_day_of_month_flg
        , last_day_of_month_flg
        , last_day_of_quarter_flg
        , last_day_of_year_flg
        , week_num
        , week_of_year_num
        , week_of_month_num
        , week_of_quarter_num
        , week_overall_num
        , week_start_dt
        , week_start_key
        , week_end_dt
        , week_end_key
        , month_num
        , month_nm
        , month_abbr
        , month_in_quarter_num
        , month_overall_num
        , yearmonth_num
        , month_start_dt
        , month_start_key
        , month_end_dt
        , month_end_key
        , quarter_num
        , quarter_nm
        , quarter_start_dt
        , quarter_start_key
        , quarter_end_dt
        , quarter_end_key
        , year_num
        , year_start_dt
        , year_start_key
        , year_end_dt
        , year_end_key
        , leap_year_flg
        , iso_day_of_week_num
        , iso_year_num
        , iso_week_of_year_txt
        , iso_week_overall_num
        , iso_week_start_dt
        , iso_week_start_key
        , iso_week_end_dt
        , iso_week_end_key
        , current_timestamp() as dw_synced_ts
        , 'CALENDAR' as dw_source_nm
        , 'ETL_PROCESS' as create_user_id
        , current_timestamp() as create_timestamp
    from enriched_dates)
, special_records as (
    select * from (values
        (
            -1
            , '1900-01-01'::date
            , '1900-01-01'::date
            , -1
            , -1
            , -1
            , -1
            , -1
            , -1
            , 'Not Available'
            , 'N/A'
            , 'N/A'
            , -1
            , 0
            , 0
            , 0
            , 0
            , 0
            , 0
            , -1
            , -1
            , -1
            , -1
            , -1
            , '1900-01-01'::date
            , -1
            , '1900-01-01'::date
            , -1
            , -1
            , 'Not Available'
            , 'N/A'
            , -1
            , -1
            , -1
            , '1900-01-01'::date
            , -1
            , '1900-01-01'::date
            , -1
            , -1
            , 'N/A'
            , '1900-01-01'::date
            , -1
            , '1900-01-01'::date
            , -1
            , -1
            , '1900-01-01'::date
            , -1
            , '1900-01-01'::date
            , -1
            , 0
            , -1
            , -1
            , 'N/A'
            , -1
            , '1900-01-01'::date
            , -1
            , '1900-01-01'::date
            , -1
            , current_timestamp()
            , 'SPECIAL'
            , 'SYSTEM'
            , current_timestamp()
        )
        , (
            -2
            , '1900-01-02'::date
            , '1900-01-02'::date
            , -2
            , -2
            , -2
            , -2
            , -2
            , -2
            , 'Invalid'
            , 'INV'
            , 'INV'
            , -2
            , 0
            , 0
            , 0
            , 0
            , 0
            , 0
            , -2
            , -2
            , -2
            , -2
            , -2
            , '1900-01-02'::date
            , -2
            , '1900-01-02'::date
            , -2
            , -2
            , 'Invalid'
            , 'INV'
            , -2
            , -2
            , -2
            , '1900-01-02'::date
            , -2
            , '1900-01-02'::date
            , -2
            , -2
            , 'Invalid'
            , '1900-01-02'::date
            , -2
            , '1900-01-02'::date
            , -2
            , -2
            , '1900-01-02'::date
            , -2
            , '1900-01-02'::date
            , -2
            , 0
            , -2
            , -2
            , 'INV'
            , -2
            , '1900-01-02'::date
            , -2
            , '1900-01-02'::date
            , -2
            , current_timestamp()
            , 'SPECIAL'
            , 'SYSTEM'
            , current_timestamp()
        )
        , (
            -3
            , '1900-01-03'::date
            , '1900-01-03'::date
            , -3
            , -3
            , -3
            , -3
            , -3
            , -3
            , 'Not Applicable'
            , 'N/A'
            , 'N/A'
            , -3
            , 0
            , 0
            , 0
            , 0
            , 0
            , 0
            , -3
            , -3
            , -3
            , -3
            , -3
            , '1900-01-03'::date
            , -3
            , '1900-01-03'::date
            , -3
            , -3
            , 'Not Applicable'
            , 'N/A'
            , -3
            , -3
            , -3
            , '1900-01-03'::date
            , -3
            , '1900-01-03'::date
            , -3
            , -3
            , 'N/A'
            , '1900-01-03'::date
            , -3
            , '1900-01-03'::date
            , -3
            , -3
            , '1900-01-03'::date
            , -3
            , '1900-01-03'::date
            , -3
            , 0
            , -3
            , -3
            , 'N/A'
            , -3
            , '1900-01-03'::date
            , -3
            , '1900-01-03'::date
            , -3
            , current_timestamp()
            , 'SPECIAL'
            , 'SYSTEM'
            , current_timestamp()
        )
        , (
            -4
            , '1900-01-04'::date
            , '1900-01-04'::date
            , -4
            , -4
            , -4
            , -4
            , -4
            , -4
            , 'Unknown'
            , 'UNK'
            , 'UNK'
            , -4
            , 0
            , 0
            , 0
            , 0
            , 0
            , 0
            , -4
            , -4
            , -4
            , -4
            , -4
            , '1900-01-04'::date
            , -4
            , '1900-01-04'::date
            , -4
            , -4
            , 'Unknown'
            , 'UNK'
            , -4
            , -4
            , -4
            , '1900-01-04'::date
            , -4
            , '1900-01-04'::date
            , -4
            , -4
            , 'Unknown'
            , '1900-01-04'::date
            , -4
            , '1900-01-04'::date
            , -4
            , -4
            , '1900-01-04'::date
            , -4
            , '1900-01-04'::date
            , -4
            , 0
            , -4
            , -4
            , 'UNK'
            , -4
            , '1900-01-04'::date
            , -4
            , '1900-01-04'::date
            , -4
            , current_timestamp()
            , 'SPECIAL'
            , 'SYSTEM'
            , current_timestamp()
        )
    )
        as t (
            date_key
            , full_dt
            , date_last_year_dt
            , date_last_year_key
            , day_of_week_num
            , day_of_month_num
            , day_of_quarter_num
            , day_of_year_num
            , day_overall_num
            , day_nm
            , day_abbr
            , day_suffix_txt
            , epoch_num
            , weekday_flg
            , last_day_of_week_flg
            , first_day_of_month_flg
            , last_day_of_month_flg
            , last_day_of_quarter_flg
            , last_day_of_year_flg
            , week_num
            , week_of_year_num
            , week_of_month_num
            , week_of_quarter_num
            , week_overall_num
            , week_start_dt
            , week_start_key
            , week_end_dt
            , week_end_key
            , month_num
            , month_nm
            , month_abbr
            , month_in_quarter_num
            , month_overall_num
            , yearmonth_num
            , month_start_dt
            , month_start_key
            , month_end_dt
            , month_end_key
            , quarter_num
            , quarter_nm
            , quarter_start_dt
            , quarter_start_key
            , quarter_end_dt
            , quarter_end_key
            , year_num
            , year_start_dt
            , year_start_key
            , year_end_dt
            , year_end_key
            , leap_year_flg
            , iso_day_of_week_num
            , iso_year_num
            , iso_week_of_year_txt
            , iso_week_overall_num
            , iso_week_start_dt
            , iso_week_start_key
            , iso_week_end_dt
            , iso_week_end_key
            , dw_synced_ts
            , dw_source_nm
            , create_user_id
            , create_timestamp
        )
    )
, final as (
    select * from regular_dates
    union all
    select * from special_records)
select * from final
