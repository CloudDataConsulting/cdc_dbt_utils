{{ config(materialized='table') }}

with dim_date as (select * from {{ ref('dim_date') }} )
, regular_quarters as (
    select
        -- Natural key: Year * 10 + Quarter
        year_num * 10 + quarter_num as quarter_key

        -- Core identifiers
        , year_num
        , quarter_num
        , max(quarter_nm) as quarter_nm
        , max(quarter_full_nm) as quarter_full_nm

        -- Quarter boundaries
        , min(full_dt) as quarter_start_dt
        , max(full_dt) as quarter_end_dt
        , min(date_key) as quarter_start_key
        , max(date_key) as quarter_end_key

        -- Previous year dates for same quarter
        , min(date_last_year_dt) as quarter_start_last_year_dt
        , max(date_last_year_dt) as quarter_end_last_year_dt

        -- Quarter metrics
        , count(distinct month_num) as months_in_quarter_num
        , count(distinct week_num) as weeks_in_quarter_num
        , count(*) as days_in_quarter_num
        , sum(case when weekday_flg = 'Weekday' then 1 else 0 end) as weekdays_in_quarter_num
        , sum(case when weekday_flg = 'Weekend' then 1 else 0 end) as weekend_days_in_quarter_num

        -- Month boundaries within quarter
        , min(month_num) as first_month_of_quarter_num
        , max(month_num) as last_month_of_quarter_num
        , min(week_num) as first_week_of_quarter_num
        , max(week_num) as last_week_of_quarter_num

    from dim_date
    where date_key > 0  -- Exclude special records
    group by year_num, quarter_num
)
, special_records as (
    select * from (values
        (
            -1                      -- quarter_key
            , -1                    -- year_num
            , -1                    -- quarter_num
            , 'UNK'                 -- quarter_nm
            , 'Unknown'             -- quarter_full_nm
            , '1900-01-01'::date    -- quarter_start_dt
            , '1900-03-31'::date    -- quarter_end_dt
            , -1                    -- quarter_start_key
            , -1                    -- quarter_end_key
            , '1899-01-01'::date    -- quarter_start_last_year_dt
            , '1899-03-31'::date    -- quarter_end_last_year_dt
            , 0                     -- months_in_quarter_num
            , 0                     -- weeks_in_quarter_num
            , 0                     -- days_in_quarter_num
            , 0                     -- weekdays_in_quarter_num
            , 0                     -- weekend_days_in_quarter_num
            , -1                    -- first_month_of_quarter_num
            , -1                    -- last_month_of_quarter_num
            , -1                    -- first_week_of_quarter_num
            , -1                    -- last_week_of_quarter_num
        )
        , (
            -2                      -- quarter_key
            , -2                    -- year_num
            , -2                    -- quarter_num
            , 'INV'                 -- quarter_nm
            , 'Invalid'             -- quarter_full_nm
            , '1900-01-02'::date    -- quarter_start_dt
            , '1900-03-31'::date    -- quarter_end_dt
            , -2                    -- quarter_start_key
            , -2                    -- quarter_end_key
            , '1899-01-02'::date    -- quarter_start_last_year_dt
            , '1899-03-31'::date    -- quarter_end_last_year_dt
            , 0                     -- months_in_quarter_num
            , 0                     -- weeks_in_quarter_num
            , 0                     -- days_in_quarter_num
            , 0                     -- weekdays_in_quarter_num
            , 0                     -- weekend_days_in_quarter_num
            , -2                    -- first_month_of_quarter_num
            , -2                    -- last_month_of_quarter_num
            , -2                    -- first_week_of_quarter_num
            , -2                    -- last_week_of_quarter_num
        )
        , (
            -3                      -- quarter_key
            , -3                    -- year_num
            , -3                    -- quarter_num
            , 'N/A'                 -- quarter_nm
            , 'Not Applicable'      -- quarter_full_nm
            , '1900-01-03'::date    -- quarter_start_dt
            , '1900-03-31'::date    -- quarter_end_dt
            , -3                    -- quarter_start_key
            , -3                    -- quarter_end_key
            , '1899-01-03'::date    -- quarter_start_last_year_dt
            , '1899-03-31'::date    -- quarter_end_last_year_dt
            , 0                     -- months_in_quarter_num
            , 0                     -- weeks_in_quarter_num
            , 0                     -- days_in_quarter_num
            , 0                     -- weekdays_in_quarter_num
            , 0                     -- weekend_days_in_quarter_num
            , -3                    -- first_month_of_quarter_num
            , -3                    -- last_month_of_quarter_num
            , -3                    -- first_week_of_quarter_num
            , -3                    -- last_week_of_quarter_num
        )
    ) as t (
        quarter_key
        , year_num
        , quarter_num
        , quarter_nm
        , quarter_full_nm
        , quarter_start_dt
        , quarter_end_dt
        , quarter_start_key
        , quarter_end_key
        , quarter_start_last_year_dt
        , quarter_end_last_year_dt
        , months_in_quarter_num
        , weeks_in_quarter_num
        , days_in_quarter_num
        , weekdays_in_quarter_num
        , weekend_days_in_quarter_num
        , first_month_of_quarter_num
        , last_month_of_quarter_num
        , first_week_of_quarter_num
        , last_week_of_quarter_num
    )
)
, combined_quarters as (
    select * from special_records
    union all
    select * from regular_quarters
)
, final as (
    select
        *

        -- Overall numbering
        , dense_rank() over (order by quarter_key) - 4 as quarter_overall_num  -- Adjust for special records

        -- Display formats
        , case
            when quarter_key < 0 then quarter_nm
            else quarter_nm || ' ' || year_num::varchar
        end as quarter_year_nm
        , case
            when quarter_key < 0 then quarter_nm
            else year_num::varchar || '-' || quarter_nm
        end as year_quarter_txt
        , case
            when quarter_key < 0 then quarter_full_nm
            else quarter_full_nm || ' Quarter ' || year_num::varchar
        end as quarter_full_nm_year

        -- Current period flags
        , case
            when quarter_key < 0 then 0
            when year_num = year(current_date())
                and quarter_num = quarter(current_date())
            then 1 else 0
        end as current_quarter_flg

        , case
            when quarter_key < 0 then 0
            when year_num = year(dateadd(quarter, -1, current_date()))
                and quarter_num = quarter(dateadd(quarter, -1, current_date()))
            then 1 else 0
        end as prior_quarter_flg

        , case
            when quarter_key < 0 then 0
            when year_num = year(current_date())
            then 1 else 0
        end as current_year_flg

        , case
            when quarter_key < 0 then 0
            when quarter_end_dt < current_date()
            then 1 else 0
        end as past_quarter_flg

        , case
            when quarter_key < 0 then 0
            when quarter_start_dt > current_date()
            then 1 else 0
        end as future_quarter_flg

        -- Relative date calculations
        , case
            when quarter_key < 0 then -999
            else datediff(quarter, quarter_start_dt, current_date())
        end as quarters_ago_num

        , case
            when quarter_key < 0 then -999
            else datediff(quarter, current_date(), quarter_start_dt)
        end as quarters_from_now_num

        -- Navigation keys
        , lag(quarter_key) over (order by quarter_key) as prior_quarter_key
        , lead(quarter_key) over (order by quarter_key) as next_quarter_key
        , lag(quarter_key, 4) over (order by quarter_key) as quarter_last_year_key

        -- Metadata
        , current_timestamp() as dw_synced_ts
        , 'dim_quarter' as dw_source_nm
        , 'ETL_PROCESS' as create_user_id
        , current_timestamp() as create_ts
    from combined_quarters
)
select * from final
