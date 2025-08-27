{{ config(materialized='table') }}

with trade_date as (
    select * from {{ ref('dim_trade_date') }}
),

trade_date_filtered as (
    select 
        date_key,
        full_dt,
        trade_year_num,
        trade_week_num,
        trade_week_start_dt,
        trade_week_end_dt,
        
        -- Month and quarter attributes for all patterns
        trade_month_445_num,
        trade_month_454_num,
        trade_month_544_num,
        trade_month_445_nm,
        trade_month_454_nm,
        trade_month_544_nm,
        
        trade_quarter_445_num,
        trade_quarter_454_num,
        trade_quarter_544_num,
        trade_quarter_445_nm,
        trade_quarter_454_nm,
        trade_quarter_544_nm,
        
        -- Week of period attributes for all patterns
        trade_week_of_month_445_num,
        trade_week_of_month_454_num,
        trade_week_of_month_544_num,
        trade_week_of_quarter_445_num,
        trade_week_of_quarter_454_num,
        trade_week_of_quarter_544_num,
        
        -- Common attributes
        is_leap_week_flg,
        weeks_in_trade_year_num,
        trade_day_of_year_num
        
    from trade_date
    where date_key > 0  -- Exclude the -1 "Not Set" record if it exists
),

week_aggregated as (
    -- Aggregate to week level, taking consistent values from trade week start date
    select
        -- Primary key using trade year and week
        trade_year_num * 100 + trade_week_num as trade_week_key,
        
        -- Week dates (same for all days in the week)
        min(trade_week_start_dt) as trade_week_start_dt,
        max(trade_week_end_dt) as trade_week_end_dt,
        
        -- Trade calendar core attributes (same for all days in the week)
        max(trade_year_num) as trade_year_num,
        max(trade_week_num) as trade_week_num,
        max(is_leap_week_flg) as is_leap_week_flg,
        max(weeks_in_trade_year_num) as weeks_in_trade_year_num,
        
        -- 445 Pattern attributes
        max(trade_month_445_num) as trade_month_445_num,
        max(trade_month_445_nm) as trade_month_445_nm,
        max(trade_quarter_445_num) as trade_quarter_445_num,
        max(trade_quarter_445_nm) as trade_quarter_445_nm,
        max(trade_week_of_month_445_num) as trade_week_of_month_445_num,
        max(trade_week_of_quarter_445_num) as trade_week_of_quarter_445_num,
        
        -- 454 Pattern attributes
        max(trade_month_454_num) as trade_month_454_num,
        max(trade_month_454_nm) as trade_month_454_nm,
        max(trade_quarter_454_num) as trade_quarter_454_num,
        max(trade_quarter_454_nm) as trade_quarter_454_nm,
        max(trade_week_of_month_454_num) as trade_week_of_month_454_num,
        max(trade_week_of_quarter_454_num) as trade_week_of_quarter_454_num,
        
        -- 544 Pattern attributes
        max(trade_month_544_num) as trade_month_544_num,
        max(trade_month_544_nm) as trade_month_544_nm,
        max(trade_quarter_544_num) as trade_quarter_544_num,
        max(trade_quarter_544_nm) as trade_quarter_544_nm,
        max(trade_week_of_month_544_num) as trade_week_of_month_544_num,
        max(trade_week_of_quarter_544_num) as trade_week_of_quarter_544_num,
        
        -- Count actual days in week (for validation)
        count(*) as days_in_week_num
        
    from trade_date_filtered
    group by 
        trade_year_num,
        trade_week_num,
        trade_week_start_dt,
        trade_week_end_dt
),

final as (
    select
        -- Primary key
        trade_week_key,
        
        -- Week dates
        trade_week_start_dt,
        trade_week_end_dt,
        to_char(trade_week_end_dt, 'yyyymmdd')::int as trade_week_end_key,
        
        -- Core trade calendar
        trade_year_num,
        trade_week_num,
        
        -- 445 Pattern
        trade_month_445_num,
        trade_month_445_nm,
        trade_quarter_445_num,
        trade_quarter_445_nm,
        trade_week_of_month_445_num,
        trade_week_of_quarter_445_num,
        
        -- 454 Pattern
        trade_month_454_num,
        trade_month_454_nm,
        trade_quarter_454_num,
        trade_quarter_454_nm,
        trade_week_of_month_454_num,
        trade_week_of_quarter_454_num,
        
        -- 544 Pattern
        trade_month_544_num,
        trade_month_544_nm,
        trade_quarter_544_num,
        trade_quarter_544_nm,
        trade_week_of_month_544_num,
        trade_week_of_quarter_544_num,
        
        -- Week descriptions (445 pattern as default)
        trade_month_445_nm || ' ' || trade_year_num::varchar as trade_month_year_445_nm,
        'Week ' || trade_week_of_month_445_num::varchar || ' of ' || trade_month_445_nm as trade_week_of_month_445_txt,
        'TW' || lpad(trade_week_num::varchar, 2, '0') || ' ' || trade_year_num::varchar as trade_week_year_txt,
        
        -- Week descriptions (454 pattern)
        trade_month_454_nm || ' ' || trade_year_num::varchar as trade_month_year_454_nm,
        'Week ' || trade_week_of_month_454_num::varchar || ' of ' || trade_month_454_nm as trade_week_of_month_454_txt,
        
        -- Week descriptions (544 pattern)
        trade_month_544_nm || ' ' || trade_year_num::varchar as trade_month_year_544_nm,
        'Week ' || trade_week_of_month_544_num::varchar || ' of ' || trade_month_544_nm as trade_week_of_month_544_txt,
        
        -- Flags and indicators
        is_leap_week_flg,
        
        case 
            when trade_week_start_dt <= current_date() 
                and trade_week_end_dt >= current_date() 
            then 1 else 0 
        end as is_current_trade_week_flg,
        
        case 
            when trade_week_start_dt <= dateadd(week, -1, current_date()) 
                and trade_week_end_dt >= dateadd(week, -1, current_date()) 
            then 1 else 0 
        end as is_prior_trade_week_flg,
        
        case 
            when trade_year_num = (
                select max(trade_year_num) 
                from trade_date 
                where full_dt <= current_date()
            )
            then 1 else 0 
        end as is_current_trade_year_flg,
        
        -- Relative metrics
        datediff(week, trade_week_start_dt, current_date()) as trade_weeks_ago_num,
        
        -- Week position in year
        round(trade_week_num / weeks_in_trade_year_num * 100, 1) as trade_week_pct_of_year_num,
        
        -- Week metrics
        days_in_week_num,
        weeks_in_trade_year_num,
        
        -- Calculate overall week number (weeks since earliest trade week)
        denserank() over (order by trade_week_start_dt) as trade_week_overall_num,
        
        -- ETL metadata
        false as dw_deleted_flg,
        current_timestamp as dw_synced_ts,
        'dim_trade_week' as dw_source_nm,
        current_user as create_user_id,
        current_timestamp as create_timestamp
        
    from week_aggregated
)

select * from final
order by trade_week_key