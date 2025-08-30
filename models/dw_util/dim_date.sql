{{ config(materialized='table') }}

with dim_trade_date as (select * from {{ ref('dim_trade_date') }})
, final as (
    select
        -- Primary Key
        date_key
        
        -- DAY Level
        -- Core day identifiers
        , calendar_full_dt as full_dt
        , calendar_date_last_year_key as date_last_year_key
        
        -- Day position metrics
        , calendar_day_of_week_num as day_of_week_num
        , iso_day_of_week_num
        , calendar_day_of_month_num as day_of_month_num
        , calendar_day_of_quarter_num as day_of_quarter_num
        , calendar_day_of_year_num as day_of_year_num
        , calendar_day_overall_num as day_overall_num
        
        -- Day descriptors
        , calendar_day_nm as day_nm
        , calendar_day_abbr as day_abbr
        , calendar_day_suffix_txt as day_suffix_txt
        , calendar_epoch_num as epoch_num
        
        -- Day flags
        , calendar_weekday_flg as weekday_flg
        , calendar_last_day_of_week_flg as last_day_of_week_flg
        , calendar_first_day_of_month_flg as first_day_of_month_flg
        , calendar_last_day_of_month_flg as last_day_of_month_flg
        , calendar_last_day_of_quarter_flg as last_day_of_quarter_flg
        , calendar_last_day_of_year_flg as last_day_of_year_flg
        
        -- WEEK Level
        -- Week numbers
        , calendar_week_num as week_num
        , calendar_week_of_year_num as week_of_year_num
        , calendar_week_of_month_num as week_of_month_num
        , calendar_week_of_quarter_num as week_of_quarter_num
        , calendar_week_overall_num as week_overall_num
        
        -- Week boundaries
        , calendar_week_start_dt as week_start_dt
        , calendar_week_start_key as week_start_key
        , calendar_week_end_dt as week_end_dt
        , calendar_week_end_key as week_end_key
        
        -- MONTH Level
        -- Month identifiers
        , calendar_month_num as month_num
        , calendar_month_nm as month_nm
        , calendar_month_abbr as month_abbr
        
        -- Month metrics
        , calendar_month_in_quarter_num as month_in_quarter_num
        , calendar_month_overall_num as month_overall_num
        , calendar_yearmonth_num as yearmonth_num
        
        -- Month boundaries
        , calendar_month_start_dt as month_start_dt
        , calendar_month_start_key as month_start_key
        , calendar_month_end_dt as month_end_dt
        , calendar_month_end_key as month_end_key
        
        -- QUARTER Level
        -- Quarter identifiers
        , calendar_quarter_num as quarter_num
        , calendar_quarter_nm as quarter_nm
        
        -- Quarter boundaries
        , calendar_quarter_start_dt as quarter_start_dt
        , calendar_quarter_start_key as quarter_start_key
        , calendar_quarter_end_dt as quarter_end_dt
        , calendar_quarter_end_key as quarter_end_key
        
        -- YEAR Level
        -- Year identifiers
        , calendar_year_num as year_num
        
        -- Year boundaries
        , calendar_year_start_dt as year_start_dt
        , calendar_year_start_key as year_start_key
        , calendar_year_end_dt as year_end_dt
        , calendar_year_end_key as year_end_key
        
        -- Year flags
        , calendar_is_leap_year_flg as is_leap_year_flg
        
        -- ISO Columns
        , iso_year_num
        , iso_week_of_year_txt
        , iso_week_overall_num
        , iso_week_start_dt
        , iso_week_start_key
        , iso_week_end_dt
        , iso_week_end_key
        
        -- Metadata Columns
        , dw_synced_ts
        , dw_source_nm
        , create_user_id
        , create_timestamp
    from dim_trade_date
)
select * from final