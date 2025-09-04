{{ config(materialized='table') }}

{{ config( post_hook="alter table {{ this }} add primary key (month_key)", ) }}

with dim_date as (
    select * from {{ ref('dim_date') }}
)
, regular_months as (
    select
        -- Natural key: Year * 100 + Month
        year_num * 100 + month_num as month_key

        -- Core identifiers
        , year_num
        , month_num
        , max(month_nm) as month_nm
        , max(month_abbr) as month_abbr
        , max(quarter_num) as quarter_num
        , max(quarter_nm) as quarter_nm
        , max(quarter_full_nm) as quarter_full_nm

        -- Month boundaries (actual month dates, not week boundaries)
        , date_trunc('month', min(full_dt))::date as month_start_dt
        , last_day(min(full_dt))::date as month_end_dt
        , to_char(date_trunc('month', min(full_dt)), 'YYYYMMDD')::int as month_start_key
        , to_char(last_day(min(full_dt)), 'YYYYMMDD')::int as month_end_key

        -- Previous year dates for same month
        , dateadd('year', -1, date_trunc('month', min(full_dt)))::date as month_start_last_year_dt
        , dateadd('year', -1, last_day(min(full_dt)))::date as month_end_last_year_dt

        -- Month metrics
        , count(distinct week_num) as weeks_in_month_num
        , count(*) as days_in_month_num
        , sum(case when weekday_flg = 'Weekday' then 1 else 0 end) as weekdays_in_month_num
        , sum(case when weekday_flg = 'Weekend' then 1 else 0 end) as weekend_days_in_month_num
        , min(week_num) as first_week_of_month_num
        , max(week_num) as last_week_of_month_num

        -- Position metrics
        , max(month_in_quarter_num) as month_in_quarter_num
        , max(month_overall_num) as month_overall_num

    from dim_date
    where date_key > 0  -- Exclude special records
    group by year_num, month_num
)
, months_with_attributes as (
    select
        *
        , month_key as yearmonth_num  -- Add for compatibility

        -- Display formats
        , month_nm || ' ' || year_num::varchar as month_year_nm
        , month_abbr || ' ' || year_num::varchar as month_year_abbr
        , year_num::varchar || '-' || lpad(month_num::varchar, 2, '0') as year_month_txt
        , 'M' || lpad(month_num::varchar, 2, '0') || ' ' || year_num::varchar as month_year_code_txt

        -- Current period flags
        , case
            when year_num = year(current_date())
                and month_num = month(current_date())
            then 1 else 0
        end as current_month_flg

        , case
            when year_num = year(dateadd(month, -1, current_date()))
                and month_num = month(dateadd(month, -1, current_date()))
            then 1 else 0
        end as prior_month_flg

        , case
            when year_num = year(current_date())
            then 1 else 0
        end as current_year_flg

        , case
            when month_end_dt < current_date()
            then 1 else 0
        end as past_month_flg

        , case
            when month_start_dt > current_date()
            then 1 else 0
        end as future_month_flg

        -- Relative date calculations
        , datediff(month, month_start_dt, current_date()) as months_ago_num
        , datediff(month, current_date(), month_start_dt) as months_from_now_num

        -- Navigation keys
        , lag(month_key) over (order by month_key) as prior_month_key
        , lead(month_key) over (order by month_key) as next_month_key
        , lag(month_key, 12) over (order by month_key) as month_last_year_key

        -- Metadata
        , current_timestamp() as dw_synced_ts
        , 'dim_month' as dw_source_nm
        , 'ETL_PROCESS' as create_user_id
        , current_timestamp() as create_ts
    from regular_months
)
, special_records as (
    select * from (values
        (
            -1                      -- month_key
            , -1                    -- year_num
            , -1                    -- month_num
            , 'Unknown'             -- month_nm
            , 'UNK'                 -- month_abbr
            , -1                    -- quarter_num
            , 'UNK'                 -- quarter_nm
            , 'Unknown'             -- quarter_full_nm
            , '1900-01-01'::date    -- month_start_dt
            , '1900-01-31'::date    -- month_end_dt
            , -1                    -- month_start_key
            , -1                    -- month_end_key
            , '1899-01-01'::date    -- month_start_last_year_dt
            , '1899-01-31'::date    -- month_end_last_year_dt
            , 0                     -- weeks_in_month_num
            , 0                     -- days_in_month_num
            , 0                     -- weekdays_in_month_num
            , 0                     -- weekend_days_in_month_num
            , -1                    -- first_week_of_month_num
            , -1                    -- last_week_of_month_num
            , -1                    -- month_in_quarter_num
            , -1                    -- month_overall_num
            , -1                    -- yearmonth_num
            , 'UNK'                 -- month_year_nm
            , 'UNK'                 -- month_year_abbr
            , 'UNK'                 -- year_month_txt
            , 'UNK'                 -- month_year_code_txt
            , 0                     -- current_month_flg
            , 0                     -- prior_month_flg
            , 0                     -- current_year_flg
            , 0                     -- past_month_flg
            , 0                     -- future_month_flg
            , -999                  -- months_ago_num
            , -999                  -- months_from_now_num
            , null                  -- prior_month_key
            , null                  -- next_month_key
            , null                  -- month_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'dim_month'           -- dw_source_nm
            , 'ETL_PROCESS'         -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -2                      -- month_key
            , -2                    -- year_num
            , -2                    -- month_num
            , 'Invalid'             -- month_nm
            , 'INV'                 -- month_abbr
            , -2                    -- quarter_num
            , 'INV'                 -- quarter_nm
            , 'Invalid'             -- quarter_full_nm
            , '1900-01-02'::date    -- month_start_dt
            , '1900-01-31'::date    -- month_end_dt
            , -2                    -- month_start_key
            , -2                    -- month_end_key
            , '1899-01-02'::date    -- month_start_last_year_dt
            , '1899-01-31'::date    -- month_end_last_year_dt
            , 0                     -- weeks_in_month_num
            , 0                     -- days_in_month_num
            , 0                     -- weekdays_in_month_num
            , 0                     -- weekend_days_in_month_num
            , -2                    -- first_week_of_month_num
            , -2                    -- last_week_of_month_num
            , -2                    -- month_in_quarter_num
            , -2                    -- month_overall_num
            , -2                    -- yearmonth_num
            , 'INV'                 -- month_year_nm
            , 'INV'                 -- month_year_abbr
            , 'INV'                 -- year_month_txt
            , 'INV'                 -- month_year_code_txt
            , 0                     -- current_month_flg
            , 0                     -- prior_month_flg
            , 0                     -- current_year_flg
            , 0                     -- past_month_flg
            , 0                     -- future_month_flg
            , -999                  -- months_ago_num
            , -999                  -- months_from_now_num
            , null                  -- prior_month_key
            , null                  -- next_month_key
            , null                  -- month_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'dim_month'           -- dw_source_nm
            , 'ETL_PROCESS'         -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -3                      -- month_key
            , -3                    -- year_num
            , -3                    -- month_num
            , 'Not Applicable'      -- month_nm
            , 'N/A'                 -- month_abbr
            , -3                    -- quarter_num
            , 'N/A'                 -- quarter_nm
            , 'Not Applicable'      -- quarter_full_nm
            , '1900-01-03'::date    -- month_start_dt
            , '1900-01-31'::date    -- month_end_dt
            , -3                    -- month_start_key
            , -3                    -- month_end_key
            , '1899-01-03'::date    -- month_start_last_year_dt
            , '1899-01-31'::date    -- month_end_last_year_dt
            , 0                     -- weeks_in_month_num
            , 0                     -- days_in_month_num
            , 0                     -- weekdays_in_month_num
            , 0                     -- weekend_days_in_month_num
            , -3                    -- first_week_of_month_num
            , -3                    -- last_week_of_month_num
            , -3                    -- month_in_quarter_num
            , -3                    -- month_overall_num
            , -3                    -- yearmonth_num
            , 'N/A'                 -- month_year_nm
            , 'N/A'                 -- month_year_abbr
            , 'N/A'                 -- year_month_txt
            , 'N/A'                 -- month_year_code_txt
            , 0                     -- current_month_flg
            , 0                     -- prior_month_flg
            , 0                     -- current_year_flg
            , 0                     -- past_month_flg
            , 0                     -- future_month_flg
            , -999                  -- months_ago_num
            , -999                  -- months_from_now_num
            , null                  -- prior_month_key
            , null                  -- next_month_key
            , null                  -- month_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'dim_month'           -- dw_source_nm
            , 'ETL_PROCESS'         -- create_user_id
            , current_timestamp()   -- create_ts
        )
    ) as t (
        month_key
        , year_num
        , month_num
        , month_nm
        , month_abbr
        , quarter_num
        , quarter_nm
        , quarter_full_nm
        , month_start_dt
        , month_end_dt
        , month_start_key
        , month_end_key
        , month_start_last_year_dt
        , month_end_last_year_dt
        , weeks_in_month_num
        , days_in_month_num
        , weekdays_in_month_num
        , weekend_days_in_month_num
        , first_week_of_month_num
        , last_week_of_month_num
        , month_in_quarter_num
        , month_overall_num
        , yearmonth_num
        , month_year_nm
        , month_year_abbr
        , year_month_txt
        , month_year_code_txt
        , current_month_flg
        , prior_month_flg
        , current_year_flg
        , past_month_flg
        , future_month_flg
        , months_ago_num
        , months_from_now_num
        , prior_month_key
        , next_month_key
        , month_last_year_key
        , dw_synced_ts
        , dw_source_nm
        , create_user_id
        , create_ts
    )
)
, final as (
    select * from months_with_attributes
    union all
    select * from special_records
)
select * from final
