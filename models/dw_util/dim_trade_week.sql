{{ config(materialized='table') }}

with trade_date as ( select * from {{ ref('dim_trade_date') }} )
, trade_date_filtered as ( 
    select 
        date_key
        , full_dt
        , trade_year_num
        , trade_week_num
        , trade_week_start_dt
        , trade_week_end_dt
        , trade_month_445_num
        , trade_month_454_num
        , trade_month_544_num
        , trade_month_445_nm
        , trade_month_454_nm
        , trade_month_544_nm
        , trade_quarter_445_num
        , trade_quarter_454_num
        , trade_quarter_544_num
        , trade_quarter_445_nm
        , trade_quarter_454_nm
        , trade_quarter_544_nm
        , trade_week_of_month_445_num
        , trade_week_of_month_454_num
        , trade_week_of_month_544_num
        , trade_week_of_quarter_445_num
        , trade_week_of_quarter_454_num
        , trade_week_of_quarter_544_num
        , is_leap_week_flg
        , weeks_in_trade_year_num
        , trade_day_of_year_num
    from trade_date)
, week_aggregated as (
    select
        trade_year_num * 100 + trade_week_num as trade_week_key
        , min(trade_week_start_dt) as trade_week_start_dt
        , max(trade_week_end_dt) as trade_week_end_dt
        , max(trade_year_num) as trade_year_num
        , max(trade_week_num) as trade_week_num
        , max(is_leap_week_flg) as is_leap_week_flg
        , max(weeks_in_trade_year_num) as weeks_in_trade_year_num
        , max(trade_month_445_num) as trade_month_445_num
        , max(trade_month_445_nm) as trade_month_445_nm
        , max(trade_quarter_445_num) as trade_quarter_445_num
        , max(trade_quarter_445_nm) as trade_quarter_445_nm
        , max(trade_week_of_month_445_num) as trade_week_of_month_445_num
        , max(trade_week_of_quarter_445_num) as trade_week_of_quarter_445_num
        , max(trade_month_454_num) as trade_month_454_num
        , max(trade_month_454_nm) as trade_month_454_nm
        , max(trade_quarter_454_num) as trade_quarter_454_num
        , max(trade_quarter_454_nm) as trade_quarter_454_nm
        , max(trade_week_of_month_454_num) as trade_week_of_month_454_num
        , max(trade_week_of_quarter_454_num) as trade_week_of_quarter_454_num
        , max(trade_month_544_num) as trade_month_544_num
        , max(trade_month_544_nm) as trade_month_544_nm
        , max(trade_quarter_544_num) as trade_quarter_544_num
        , max(trade_quarter_544_nm) as trade_quarter_544_nm
        , max(trade_week_of_month_544_num) as trade_week_of_month_544_num
        , max(trade_week_of_quarter_544_num) as trade_week_of_quarter_544_num
        , min(date_key) as first_day_of_week_key
        , max(date_key) as last_day_of_week_key
        , count(*) as days_in_week_num
    from trade_date_filtered
    group by trade_year_num * 100 + trade_week_num)
, final as (
    select
        trade_week_key
        , trade_week_start_dt
        , trade_week_end_dt
        , first_day_of_week_key
        , last_day_of_week_key as trade_week_end_key
        , trade_year_num
        , trade_week_num
        , trade_month_445_num
        , trade_month_445_nm
        , trade_quarter_445_num
        , trade_quarter_445_nm
        , trade_week_of_month_445_num
        , trade_week_of_quarter_445_num
        , trade_month_454_num
        , trade_month_454_nm
        , trade_quarter_454_num
        , trade_quarter_454_nm
        , trade_week_of_month_454_num
        , trade_week_of_quarter_454_num
        , trade_month_544_num
        , trade_month_544_nm
        , trade_quarter_544_num
        , trade_quarter_544_nm
        , trade_week_of_month_544_num
        , trade_week_of_quarter_544_num
        , 'TY' || trade_year_num::varchar || '-W' || lpad(trade_week_num::varchar, 2, '0') as trade_year_week_txt
        , 'W' || lpad(trade_week_num::varchar, 2, '0') || ' ' || trade_year_num::varchar as week_year_txt
        , case 
            when trade_week_start_dt <= current_date() 
                and trade_week_end_dt >= current_date() 
            then 1 else 0 
        end as is_current_trade_week_flg
        , case 
            when trade_week_start_dt <= dateadd(week, -1, current_date()) 
                and trade_week_end_dt >= dateadd(week, -1, current_date()) 
            then 1 else 0 
        end as is_prior_trade_week_flg
        , case 
            when trade_year_num = year(current_date()) 
            then 1 else 0 
        end as is_current_trade_year_flg
        , case 
            when trade_week_end_dt < current_date() 
            then 1 else 0 
        end as is_past_trade_week_flg
        , datediff(week, trade_week_start_dt, current_date()) as trade_weeks_ago_num
        , datediff(week, current_date(), trade_week_start_dt) as weeks_from_now_num
        , days_in_week_num
        , weeks_in_trade_year_num
        , case when trade_week_num = 53 then 1 else 0 end as is_leap_week_flg
        , dense_rank() over (order by trade_week_start_dt) as trade_week_overall_num
        , false as dw_deleted_flg
        , current_timestamp as dw_synced_ts
        , 'dim_trade_week' as dw_source_nm
        , current_user as create_user_id
        , current_timestamp as create_timestamp
    from week_aggregated)
select * from final