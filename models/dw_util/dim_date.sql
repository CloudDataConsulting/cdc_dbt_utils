{{ config(materialized='table') }}
{{ config( post_hook="alter table {{ this }} add primary key (date_key)", ) }}
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
        , case when day_of_week_num between 2 and 6 then 'Weekday' else 'Weekend' end as weekday_flg
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
        , case quarter_num
            when 1 then 'First'
            when 2 then 'Second'
            when 3 then 'Third'
            when 4 then 'Fourth'
        end as quarter_full_nm
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
        , quarter_full_nm
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
        , current_timestamp() as create_ts
    from enriched_dates)
, special_records as (
    select * from (values
        (
            -1                      -- date_key
            , '1900-01-01'::date    -- full_dt
            , '1900-01-01'::date    -- date_last_year_dt
            , -1                    -- date_last_year_key
            , -1                    -- day_of_week_num
            , -1                    -- day_of_month_num
            , -1                    -- day_of_quarter_num
            , -1                    -- day_of_year_num
            , -1                    -- day_overall_num
            , 'Unknown'             -- day_nm
            , 'UNK'                 -- day_abbr
            , 'UNK'                 -- day_suffix_txt
            , -1                    -- epoch_num
            , 'UNK'                 -- weekday_flg
            , 0                     -- last_day_of_week_flg
            , 0                     -- first_day_of_month_flg
            , 0                     -- last_day_of_month_flg
            , 0                     -- last_day_of_quarter_flg
            , 0                     -- last_day_of_year_flg
            , -1                    -- week_num
            , -1                    -- week_of_year_num
            , -1                    -- week_of_month_num
            , -1                    -- week_of_quarter_num
            , -1                    -- week_overall_num
            , '1900-01-01'::date    -- week_start_dt
            , -1                    -- week_start_key
            , '1900-01-01'::date    -- week_end_dt
            , -1                    -- week_end_key
            , -1                    -- month_num
            , 'Unknown'             -- month_nm
            , 'UNK'                 -- month_abbr
            , -1                    -- month_in_quarter_num
            , -1                    -- month_overall_num
            , -1                    -- yearmonth_num
            , '1900-01-01'::date    -- month_start_dt
            , -1                    -- month_start_key
            , '1900-01-01'::date    -- month_end_dt
            , -1                    -- month_end_key
            , -1                    -- quarter_num
            , 'UNK'                 -- quarter_nm
            , 'Unknown'             -- quarter_full_nm
            , '1900-01-01'::date    -- quarter_start_dt
            , -1                    -- quarter_start_key
            , '1900-01-01'::date    -- quarter_end_dt
            , -1                    -- quarter_end_key
            , -1                    -- year_num
            , '1900-01-01'::date    -- year_start_dt
            , -1                    -- year_start_key
            , '1900-01-01'::date    -- year_end_dt
            , -1                    -- year_end_key
            , 0                     -- leap_year_flg
            , -1                    -- iso_day_of_week_num
            , -1                    -- iso_year_num
            , 'UNK'                 -- iso_week_of_year_txt
            , -1                    -- iso_week_overall_num
            , '1900-01-01'::date    -- iso_week_start_dt
            , -1                    -- iso_week_start_key
            , '1900-01-01'::date    -- iso_week_end_dt
            , -1                    -- iso_week_end_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -2                      -- date_key
            , '1900-01-02'::date    -- full_dt
            , '1900-01-02'::date    -- date_last_year_dt
            , -2                    -- date_last_year_key
            , -2                    -- day_of_week_num
            , -2                    -- day_of_month_num
            , -2                    -- day_of_quarter_num
            , -2                    -- day_of_year_num
            , -2                    -- day_overall_num
            , 'Invalid'             -- day_nm
            , 'INV'                 -- day_abbr
            , 'INV'                 -- day_suffix_txt
            , -2                    -- epoch_num
            , 'INV'                 -- weekday_flg
            , 0                     -- last_day_of_week_flg
            , 0                     -- first_day_of_month_flg
            , 0                     -- last_day_of_month_flg
            , 0                     -- last_day_of_quarter_flg
            , 0                     -- last_day_of_year_flg
            , -2                    -- week_num
            , -2                    -- week_of_year_num
            , -2                    -- week_of_month_num
            , -2                    -- week_of_quarter_num
            , -2                    -- week_overall_num
            , '1900-01-02'::date    -- week_start_dt
            , -2                    -- week_start_key
            , '1900-01-02'::date    -- week_end_dt
            , -2                    -- week_end_key
            , -2                    -- month_num
            , 'Invalid'             -- month_nm
            , 'INV'                 -- month_abbr
            , -2                    -- month_in_quarter_num
            , -2                    -- month_overall_num
            , -2                    -- yearmonth_num
            , '1900-01-02'::date    -- month_start_dt
            , -2                    -- month_start_key
            , '1900-01-02'::date    -- month_end_dt
            , -2                    -- month_end_key
            , -2                    -- quarter_num
            , 'INV'                 -- quarter_nm
            , 'Invalid'             -- quarter_full_nm
            , '1900-01-02'::date    -- quarter_start_dt
            , -2                    -- quarter_start_key
            , '1900-01-02'::date    -- quarter_end_dt
            , -2                    -- quarter_end_key
            , -2                    -- year_num
            , '1900-01-02'::date    -- year_start_dt
            , -2                    -- year_start_key
            , '1900-01-02'::date    -- year_end_dt
            , -2                    -- year_end_key
            , 0                     -- leap_year_flg
            , -2                    -- iso_day_of_week_num
            , -2                    -- iso_year_num
            , 'INV'                 -- iso_week_of_year_txt
            , -2                    -- iso_week_overall_num
            , '1900-01-02'::date    -- iso_week_start_dt
            , -2                    -- iso_week_start_key
            , '1900-01-02'::date    -- iso_week_end_dt
            , -2                    -- iso_week_end_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -3                      -- date_key
            , '1900-01-03'::date    -- full_dt
            , '1900-01-03'::date    -- date_last_year_dt
            , -3                    -- date_last_year_key
            , -3                    -- day_of_week_num
            , -3                    -- day_of_month_num
            , -3                    -- day_of_quarter_num
            , -3                    -- day_of_year_num
            , -3                    -- day_overall_num
            , 'Not Applicable'      -- day_nm
            , 'N/A'                 -- day_abbr
            , 'N/A'                 -- day_suffix_txt
            , -3                    -- epoch_num
            , 'N/A'                 -- weekday_flg
            , 0                     -- last_day_of_week_flg
            , 0                     -- first_day_of_month_flg
            , 0                     -- last_day_of_month_flg
            , 0                     -- last_day_of_quarter_flg
            , 0                     -- last_day_of_year_flg
            , -3                    -- week_num
            , -3                    -- week_of_year_num
            , -3                    -- week_of_month_num
            , -3                    -- week_of_quarter_num
            , -3                    -- week_overall_num
            , '1900-01-03'::date    -- week_start_dt
            , -3                    -- week_start_key
            , '1900-01-03'::date    -- week_end_dt
            , -3                    -- week_end_key
            , -3                    -- month_num
            , 'Not Applicable'      -- month_nm
            , 'N/A'                 -- month_abbr
            , -3                    -- month_in_quarter_num
            , -3                    -- month_overall_num
            , -3                    -- yearmonth_num
            , '1900-01-03'::date    -- month_start_dt
            , -3                    -- month_start_key
            , '1900-01-03'::date    -- month_end_dt
            , -3                    -- month_end_key
            , -3                    -- quarter_num
            , 'N/A'                 -- quarter_nm
            , 'Not Applicable'      -- quarter_full_nm
            , '1900-01-03'::date    -- quarter_start_dt
            , -3                    -- quarter_start_key
            , '1900-01-03'::date    -- quarter_end_dt
            , -3                    -- quarter_end_key
            , -3                    -- year_num
            , '1900-01-03'::date    -- year_start_dt
            , -3                    -- year_start_key
            , '1900-01-03'::date    -- year_end_dt
            , -3                    -- year_end_key
            , 0                     -- leap_year_flg
            , -3                    -- iso_day_of_week_num
            , -3                    -- iso_year_num
            , 'N/A'                 -- iso_week_of_year_txt
            , -3                    -- iso_week_overall_num
            , '1900-01-03'::date    -- iso_week_start_dt
            , -3                    -- iso_week_start_key
            , '1900-01-03'::date    -- iso_week_end_dt
            , -3                    -- iso_week_end_key
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
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
            , quarter_full_nm
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
            , create_ts
        )
    )
, final as (
    select * from regular_dates
    union all
    select * from special_records)
select * from final
