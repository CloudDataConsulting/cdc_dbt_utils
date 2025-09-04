{{ config(materialized='table') }}

with dim_date as (
    select * from {{ ref('dim_date') }}
)
, regular_weeks as (
    select
        -- Use the Sunday of each week as the natural key
        min(week_start_key) as week_key

        -- Core week identifiers
        , min(full_dt) as week_start_dt
        , max(full_dt) as week_end_dt
        , min(week_start_key) as week_start_key
        , max(week_end_key) as week_end_key

        -- Previous year date for same week
        , min(date_last_year_dt) as week_start_last_year_dt
        , max(date_last_year_dt) as week_end_last_year_dt

        -- Week attributes (use MIN/MAX for week that spans years)
        , case
            when count(distinct year_num) > 1 then max(year_num)
            else max(year_num)
        end as year_num
        , max(week_num) as week_num
        , max(week_of_year_num) as week_of_year_num
        , max(week_of_quarter_num) as week_of_quarter_num
        , max(week_overall_num) as week_overall_num

        -- Month attributes (most common month in the week)
        , mode(month_num) as month_num
        , mode(month_nm) as month_nm
        , mode(month_abbr) as month_abbr
        , max(week_of_month_num) as week_of_month_num
        , mode(yearmonth_num) as yearmonth_num

        -- Quarter attributes
        , mode(quarter_num) as quarter_num
        , mode(quarter_nm) as quarter_nm
        , mode(quarter_full_nm) as quarter_full_nm

        -- Week metrics
        , count(*) as days_in_week_num
        , sum(case when weekday_flg = 'Weekday' then 1 else 0 end) as weekdays_in_week_num
        , sum(case when weekday_flg = 'Weekend' then 1 else 0 end) as weekend_days_in_week_num

        -- ISO week attributes
        , mode(iso_year_num) as iso_year_num
        , mode(iso_week_of_year_txt) as iso_week_of_year_txt
        , max(iso_week_overall_num) as iso_week_overall_num
        , min(iso_week_start_dt) as iso_week_start_dt
        , min(iso_week_start_key) as iso_week_start_key
        , max(iso_week_end_dt) as iso_week_end_dt
        , max(iso_week_end_key) as iso_week_end_key

    from dim_date
    where date_key > 0  -- Exclude special records
    group by week_overall_num
)
, special_records as (
    select * from (values
        (
            -1                      -- week_key
            , '1900-01-01'::date    -- week_start_dt
            , '1900-01-07'::date    -- week_end_dt
            , -1                    -- week_start_key
            , -1                    -- week_end_key
            , '1899-01-01'::date    -- week_start_last_year_dt
            , '1899-01-07'::date    -- week_end_last_year_dt
            , -1                    -- year_num
            , -1                    -- week_num
            , -1                    -- week_of_year_num
            , -1                    -- week_of_quarter_num
            , -1                    -- week_overall_num
            , -1                    -- month_num
            , 'Unknown'             -- month_nm
            , 'UNK'                 -- month_abbr
            , -1                    -- week_of_month_num
            , -1                    -- yearmonth_num
            , -1                    -- quarter_num
            , 'UNK'                 -- quarter_nm
            , 'Unknown'             -- quarter_full_nm
            , 7                     -- days_in_week_num
            , 0                     -- weekdays_in_week_num
            , 0                     -- weekend_days_in_week_num
            , -1                    -- iso_year_num
            , 'UNK'                 -- iso_week_of_year_txt
            , -1                    -- iso_week_overall_num
            , '1900-01-01'::date    -- iso_week_start_dt
            , -1                    -- iso_week_start_key
            , '1900-01-07'::date    -- iso_week_end_dt
            , -1                    -- iso_week_end_key
        )
        , (
            -2                      -- week_key
            , '1900-01-02'::date    -- week_start_dt
            , '1900-01-08'::date    -- week_end_dt
            , -2                    -- week_start_key
            , -2                    -- week_end_key
            , '1899-01-02'::date    -- week_start_last_year_dt
            , '1899-01-08'::date    -- week_end_last_year_dt
            , -2                    -- year_num
            , -2                    -- week_num
            , -2                    -- week_of_year_num
            , -2                    -- week_of_quarter_num
            , -2                    -- week_overall_num
            , -2                    -- month_num
            , 'Invalid'             -- month_nm
            , 'INV'                 -- month_abbr
            , -2                    -- week_of_month_num
            , -2                    -- yearmonth_num
            , -2                    -- quarter_num
            , 'INV'                 -- quarter_nm
            , 'Invalid'             -- quarter_full_nm
            , 7                     -- days_in_week_num
            , 0                     -- weekdays_in_week_num
            , 0                     -- weekend_days_in_week_num
            , -2                    -- iso_year_num
            , 'INV'                 -- iso_week_of_year_txt
            , -2                    -- iso_week_overall_num
            , '1900-01-02'::date    -- iso_week_start_dt
            , -2                    -- iso_week_start_key
            , '1900-01-08'::date    -- iso_week_end_dt
            , -2                    -- iso_week_end_key
        )
        , (
            -3                      -- week_key
            , '1900-01-03'::date    -- week_start_dt
            , '1900-01-09'::date    -- week_end_dt
            , -3                    -- week_start_key
            , -3                    -- week_end_key
            , '1899-01-03'::date    -- week_start_last_year_dt
            , '1899-01-09'::date    -- week_end_last_year_dt
            , -3                    -- year_num
            , -3                    -- week_num
            , -3                    -- week_of_year_num
            , -3                    -- week_of_quarter_num
            , -3                    -- week_overall_num
            , -3                    -- month_num
            , 'Not Applicable'      -- month_nm
            , 'N/A'                 -- month_abbr
            , -3                    -- week_of_month_num
            , -3                    -- yearmonth_num
            , -3                    -- quarter_num
            , 'N/A'                 -- quarter_nm
            , 'Not Applicable'      -- quarter_full_nm
            , 7                     -- days_in_week_num
            , 0                     -- weekdays_in_week_num
            , 0                     -- weekend_days_in_week_num
            , -3                    -- iso_year_num
            , 'N/A'                 -- iso_week_of_year_txt
            , -3                    -- iso_week_overall_num
            , '1900-01-03'::date    -- iso_week_start_dt
            , -3                    -- iso_week_start_key
            , '1900-01-09'::date    -- iso_week_end_dt
            , -3                    -- iso_week_end_key
        )
    ) as t (
        week_key
        , week_start_dt
        , week_end_dt
        , week_start_key
        , week_end_key
        , week_start_last_year_dt
        , week_end_last_year_dt
        , year_num
        , week_num
        , week_of_year_num
        , week_of_quarter_num
        , week_overall_num
        , month_num
        , month_nm
        , month_abbr
        , week_of_month_num
        , yearmonth_num
        , quarter_num
        , quarter_nm
        , quarter_full_nm
        , days_in_week_num
        , weekdays_in_week_num
        , weekend_days_in_week_num
        , iso_year_num
        , iso_week_of_year_txt
        , iso_week_overall_num
        , iso_week_start_dt
        , iso_week_start_key
        , iso_week_end_dt
        , iso_week_end_key
    )
)
, combined_weeks as (
    select * from special_records
    union all
    select * from regular_weeks
)
, final as (
    select
        *

        -- Derived display columns
        , case
            when week_key < 0 then month_abbr
            else 'W' || lpad(week_num::varchar, 2, '0') || ' ' || year_num::varchar
        end as week_year_txt
        , case
            when week_key < 0 then month_abbr
            else year_num::varchar || '-W' || lpad(week_num::varchar, 2, '0')
        end as year_week_txt
        , case
            when week_key < 0 then month_nm
            else month_nm || ' ' || year_num::varchar
        end as month_year_nm
        , case
            when week_key < 0 then month_nm
            else 'Week ' || week_of_month_num::varchar || ' of ' || month_nm
        end as week_of_month_nm

        -- Current period flags
        , case
            when week_key < 0 then 0
            when week_start_dt <= current_date()
                and week_end_dt >= current_date()
            then 1 else 0
        end as current_week_flg

        , case
            when week_key < 0 then 0
            when week_start_dt <= dateadd(week, -1, current_date())
                and week_end_dt >= dateadd(week, -1, current_date())
            then 1 else 0
        end as prior_week_flg

        , case
            when week_key < 0 then 0
            when year_num = year(current_date())
            then 1 else 0
        end as current_year_flg

        , case
            when week_key < 0 then 0
            when week_end_dt < current_date()
            then 1 else 0
        end as past_week_flg

        , case
            when week_key < 0 then 0
            when week_start_dt > current_date()
            then 1 else 0
        end as future_week_flg

        -- Relative date calculations
        , case
            when week_key < 0 then -999
            else datediff(week, week_start_dt, current_date())
        end as weeks_ago_num

        -- Navigation keys
        , lag(week_key) over (order by week_key) as prior_week_key
        , lead(week_key) over (order by week_key) as next_week_key
        , lag(week_key, 52) over (order by week_key) as week_last_year_key

        -- Metadata
        , current_timestamp() as dw_synced_ts
        , 'dim_week' as dw_source_nm
        , 'ETL_PROCESS' as create_user_id
        , current_timestamp() as create_ts
    from combined_weeks
)
select * from final
