{{ config(materialized='table') }}
with dim_date as (select * from {{ ref('dim_date') }})
, months as (
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
        -- Month boundaries
        , min(full_dt) as month_start_dt
        , max(full_dt) as month_end_dt
        , min(date_key) as month_start_key
        , max(date_key) as month_end_key
        -- Month metrics - count distinct weeks that have any day in this month
        , count(distinct week_num) as weeks_in_month_num
        , count(*) as days_in_month_num
        , min(week_num) as first_week_of_month_num
        , max(week_num) as last_week_of_month_num
        -- Position metrics
        , max(case
            when month_num in (1, 4, 7, 10) then 1
            when month_num in (2, 5, 8, 11) then 2
            else 3
        end) as month_in_quarter_num
    from dim_date
    group by year_num, month_num
)
, final as (
    select
        *
        -- Overall numbering
        , dense_rank() over (order by year_num, month_num) as month_overall_num
        -- Display formats
        , month_nm || ' ' || year_num::varchar as month_year_nm
        , month_abbr || ' ' || year_num::varchar as month_year_abbr
        , year_num::varchar || '-' || lpad(month_num::varchar, 2, '0') as year_month_txt
        , 'M' || lpad(month_num::varchar, 2, '0') || ' ' || year_num::varchar as month_year_code_txt
        -- Quarter display
        , 'Q' || quarter_num::varchar as quarter_txt
        -- Current period flags
        , case
            when year_num = year(current_date())
                and month_num = month(current_date())
            then 1 else 0
        end as is_current_month_flg
        , case
            when year_num = year(dateadd(month, -1, current_date()))
                and month_num = month(dateadd(month, -1, current_date()))
            then 1 else 0
        end as is_prior_month_flg
        , case
            when year_num = year(current_date())
            then 1 else 0
        end as is_current_year_flg
        , case
            when month_end_dt < current_date()
            then 1 else 0
        end as is_past_month_flg
        -- Relative date calculations
        , datediff(month, month_start_dt, current_date()) as months_ago_num
        , datediff(month, current_date(), month_start_dt) as months_from_now_num
        -- Metadata
        , current_timestamp as dw_synced_ts
        , 'dim_month' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from months
)
select * from final
