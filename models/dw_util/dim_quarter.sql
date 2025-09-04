{{ config(materialized='table') }}

{{ config( post_hook="alter table {{ this }} add primary key (quarter_key)", ) }}

with dim_month as (
    select * from {{ ref('dim_month') }}
)
, regular_quarters as (
    select
        -- Natural key: Year * 10 + Quarter
        year_num * 10 + quarter_num as quarter_key

        -- Core identifiers
        , year_num
        , quarter_num
        , quarter_nm
        , quarter_full_nm

        -- Quarter boundaries
        , min(month_start_dt) as quarter_start_dt
        , max(month_end_dt) as quarter_end_dt
        , min(month_start_key) as quarter_start_key
        , max(month_end_key) as quarter_end_key

        -- Previous year dates for same quarter
        , min(month_start_last_year_dt) as quarter_start_last_year_dt
        , max(month_end_last_year_dt) as quarter_end_last_year_dt

        -- Quarter metrics
        , count(distinct month_key) as months_in_quarter_num
        , sum(weeks_in_month_num) as weeks_in_quarter_num
        , sum(days_in_month_num) as days_in_quarter_num
        , sum(weekdays_in_month_num) as weekdays_in_quarter_num
        , sum(weekend_days_in_month_num) as weekend_days_in_quarter_num

        -- Month boundaries within quarter
        , min(month_num) as first_month_of_quarter_num
        , max(month_num) as last_month_of_quarter_num
        , min(first_week_of_month_num) as first_week_of_quarter_num
        , max(last_week_of_month_num) as last_week_of_quarter_num

    from dim_month
    where month_key > 0  -- Exclude special records
    group by year_num, quarter_num, quarter_nm, quarter_full_nm
)
, quarters_with_attributes as (
    select
        *

        -- Overall numbering
        , row_number() over (order by quarter_key) as quarter_overall_num

        -- Display formats
        , quarter_nm || ' ' || year_num::varchar as quarter_year_nm
        , year_num::varchar || '-' || quarter_nm as year_quarter_txt
        , quarter_full_nm || ' Quarter ' || year_num::varchar as quarter_full_nm_year

        -- Current period flags
        , case
            when year_num = year(current_date())
                and quarter_num = quarter(current_date())
            then 1 else 0
        end as current_quarter_flg

        , case
            when year_num = year(dateadd(quarter, -1, current_date()))
                and quarter_num = quarter(dateadd(quarter, -1, current_date()))
            then 1 else 0
        end as prior_quarter_flg

        , case
            when year_num = year(current_date())
            then 1 else 0
        end as current_year_flg

        , case
            when quarter_end_dt < current_date()
            then 1 else 0
        end as past_quarter_flg

        , case
            when quarter_start_dt > current_date()
            then 1 else 0
        end as future_quarter_flg

        -- Relative date calculations
        , datediff(quarter, quarter_start_dt, current_date()) as quarters_ago_num
        , datediff(quarter, current_date(), quarter_start_dt) as quarters_from_now_num

        -- Navigation keys
        , lag(quarter_key) over (order by quarter_key) as prior_quarter_key
        , lead(quarter_key) over (order by quarter_key) as next_quarter_key
        , lag(quarter_key, 4) over (order by quarter_key) as quarter_last_year_key

        -- Metadata
        , current_timestamp() as dw_synced_ts
        , 'dim_quarter' as dw_source_nm
        , 'ETL_PROCESS' as create_user_id
        , current_timestamp() as create_ts
    from regular_quarters
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
            , -1                    -- quarter_overall_num
            , 'UNK'                 -- quarter_year_nm
            , 'UNK'                 -- year_quarter_txt
            , 'Unknown'             -- quarter_full_nm_year
            , 0                     -- current_quarter_flg
            , 0                     -- prior_quarter_flg
            , 0                     -- current_year_flg
            , 0                     -- past_quarter_flg
            , 0                     -- future_quarter_flg
            , -999                  -- quarters_ago_num
            , -999                  -- quarters_from_now_num
            , null                  -- prior_quarter_key
            , null                  -- next_quarter_key
            , null                  -- quarter_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'dim_quarter'         -- dw_source_nm
            , 'ETL_PROCESS'         -- create_user_id
            , current_timestamp()   -- create_ts
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
            , -2                    -- quarter_overall_num
            , 'INV'                 -- quarter_year_nm
            , 'INV'                 -- year_quarter_txt
            , 'Invalid'             -- quarter_full_nm_year
            , 0                     -- current_quarter_flg
            , 0                     -- prior_quarter_flg
            , 0                     -- current_year_flg
            , 0                     -- past_quarter_flg
            , 0                     -- future_quarter_flg
            , -999                  -- quarters_ago_num
            , -999                  -- quarters_from_now_num
            , null                  -- prior_quarter_key
            , null                  -- next_quarter_key
            , null                  -- quarter_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'dim_quarter'         -- dw_source_nm
            , 'ETL_PROCESS'         -- create_user_id
            , current_timestamp()   -- create_ts
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
            , -3                    -- quarter_overall_num
            , 'N/A'                 -- quarter_year_nm
            , 'N/A'                 -- year_quarter_txt
            , 'Not Applicable'      -- quarter_full_nm_year
            , 0                     -- current_quarter_flg
            , 0                     -- prior_quarter_flg
            , 0                     -- current_year_flg
            , 0                     -- past_quarter_flg
            , 0                     -- future_quarter_flg
            , -999                  -- quarters_ago_num
            , -999                  -- quarters_from_now_num
            , null                  -- prior_quarter_key
            , null                  -- next_quarter_key
            , null                  -- quarter_last_year_key
            , current_timestamp()   -- dw_synced_ts
            , 'dim_quarter'         -- dw_source_nm
            , 'ETL_PROCESS'         -- create_user_id
            , current_timestamp()   -- create_ts
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
        , quarter_overall_num
        , quarter_year_nm
        , year_quarter_txt
        , quarter_full_nm_year
        , current_quarter_flg
        , prior_quarter_flg
        , current_year_flg
        , past_quarter_flg
        , future_quarter_flg
        , quarters_ago_num
        , quarters_from_now_num
        , prior_quarter_key
        , next_quarter_key
        , quarter_last_year_key
        , dw_synced_ts
        , dw_source_nm
        , create_user_id
        , create_ts
    )
)
, final as (
    select * from quarters_with_attributes
    union all
    select * from special_records
)
select * from final
