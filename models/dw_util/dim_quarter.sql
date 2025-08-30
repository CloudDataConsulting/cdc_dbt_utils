{{ config(materialized='table') }}
with dim_month as (select * from {{ ref('dim_month') }})
, quarters as (
    select
        -- Natural key: Year * 10 + Quarter
        year_num * 10 + quarter_num as quarter_key
        -- Core identifiers
        , year_num
        , quarter_num
        , max(quarter_nm) as quarter_nm
        -- Quarter boundaries
        , min(month_start_dt) as quarter_start_dt
        , max(month_end_dt) as quarter_end_dt
        , min(month_start_key) as quarter_start_key
        , max(month_end_key) as quarter_end_key
        -- Month details
        , min(month_num) as first_month_of_quarter_num
        , max(month_num) as last_month_of_quarter_num
        , count(distinct month_num) as months_in_quarter_num
        -- Get month names and abbreviations for each position
        , max(case when month_in_quarter_num = 1 then month_nm end) as first_month_nm
        , max(case when month_in_quarter_num = 1 then month_abbr end) as first_month_abbr
        , max(case when month_in_quarter_num = 2 then month_nm end) as second_month_nm
        , max(case when month_in_quarter_num = 2 then month_abbr end) as second_month_abbr
        , max(case when month_in_quarter_num = 3 then month_nm end) as third_month_nm
        , max(case when month_in_quarter_num = 3 then month_abbr end) as third_month_abbr
        -- Week and day metrics
        , sum(weeks_in_month_num) as weeks_in_quarter_num
        , min(first_week_of_month_num) as first_week_of_quarter_num
        , max(last_week_of_month_num) as last_week_of_quarter_num
        , sum(days_in_month_num) as days_in_quarter_num
    from dim_month
    group by year_num, quarter_num
)
, final as (
    select
        *
        -- Overall numbering
        , dense_rank() over (order by year_num, quarter_num) as quarter_overall_num
        -- Display formats
        , 'Q' || quarter_num::varchar as quarter_txt
        , quarter_nm || ' Quarter' as quarter_position_txt
        , quarter_nm || ' ' || year_num::varchar as quarter_year_nm
        , year_num::varchar || '-Q' || quarter_num::varchar as year_quarter_txt
        , 'Y' || year_num::varchar || '-Q' || quarter_num::varchar as year_quarter_code_txt
        , first_month_abbr || ', ' || second_month_abbr || ', ' || third_month_abbr as months_in_quarter_txt
        -- Current period flags
        , case
            when year_num = year(current_date())
                and quarter_num = quarter(current_date())
            then 1 else 0
        end as is_current_quarter_flg
        , case
            when year_num = year(dateadd(quarter, -1, current_date()))
                and quarter_num = quarter(dateadd(quarter, -1, current_date()))
            then 1 else 0
        end as is_prior_quarter_flg
        , case
            when year_num = year(current_date())
            then 1 else 0
        end as is_current_year_flg
        , case
            when quarter_end_dt < current_date()
            then 1 else 0
        end as is_past_quarter_flg
        -- Relative date calculations
        , datediff(quarter, quarter_start_dt, current_date()) as quarters_ago_num
        , datediff(quarter, current_date(), quarter_start_dt) as quarters_from_now_num
        -- Metadata
        , current_timestamp as dw_synced_ts
        , 'dim_quarter' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from quarters
)
select * from final
