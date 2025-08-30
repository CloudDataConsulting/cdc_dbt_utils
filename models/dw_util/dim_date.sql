

with dim_date as ( select * from {{ ref('dim_trade_date') }} )
, final as (
    select
    dim_date.date_key
    , dim_date.calendar_full_dt as full_dt
    {# , dim_date.trade_full_dt #}
    , dim_date.calendar_date_last_year_key alias date_last_year_key
    {# , dim_date.trade_date_last_year_key #}
    , dim_date.calendar_day_of_week_num
    , dim_date.iso_day_of_week_num
    , dim_date.calendar_day_of_month_num
    , dim_date.calendar_day_of_quarter_num
    , dim_date.calendar_day_of_year_num
    {# , dim_date.trade_day_of_year_num #}
    , dim_date.calendar_day_overall_num
    , dim_date.calendar_day_nm
    , dim_date.calendar_day_abbr
    , dim_date.calendar_day_suffix_txt
    , dim_date.calendar_epoch_num
    , dim_date.calendar_weekday_flg
    , dim_date.calendar_last_day_of_week_flg
    , dim_date.calendar_first_day_of_month_flg
    , dim_date.calendar_last_day_of_month_flg
    , dim_date.calendar_last_day_of_quarter_flg
    , dim_date.calendar_last_day_of_year_flg
    , dim_date.calendar_week_num
    {# , dim_date.trade_week_num #}
    , dim_date.calendar_week_of_year_num
    {# , dim_date.trade_week_of_year_num #}
    , dim_date.calendar_week_of_month_num
    {# , dim_date.trade_week_of_month_445_num #}
    {# , dim_date.trade_week_of_month_454_num #}
    {# , dim_date.trade_week_of_month_544_num #}
    , dim_date.calendar_week_of_quarter_num
    {# , dim_date.trade_week_of_quarter_num #}
    , dim_date.calendar_week_overall_num
    {# , dim_date.trade_week_overall_num #}
    , dim_date.calendar_week_start_dt
    {# , dim_date.trade_week_start_dt #}
    , dim_date.calendar_week_start_key
    {# , dim_date.trade_week_start_key #}
    , dim_date.calendar_week_end_dt
    {# , dim_date.trade_week_end_dt #}
    , dim_date.calendar_week_end_key
    {# , dim_date.trade_week_end_key #}
    , dim_date.calendar_month_num
    {# , dim_date.trade_month_445_num #}
    {# , dim_date.trade_month_454_num #}
    {# , dim_date.trade_month_544_num #}
    , dim_date.calendar_month_nm
    {# , dim_date.trade_month_445_nm #}
    {# , dim_date.trade_month_454_nm #}
    {# , dim_date.trade_month_544_nm #}
    , dim_date.calendar_month_abbr
    {# , dim_date.trade_month_abbr #}
    , dim_date.calendar_month_in_quarter_num
    , dim_date.calendar_month_overall_num
    {# , dim_date.trade_month_overall_num #}
    , dim_date.calendar_yearmonth_num
    {# , dim_date.trade_yearmonth_num #}
    , dim_date.calendar_month_start_dt
    {# , dim_date.trade_month_445_start_dt
    , dim_date.trade_month_454_start_dt
    , dim_date.trade_month_544_start_dt #}
    , dim_date.calendar_month_start_key
    {# , dim_date.trade_month_445_start_key
    , dim_date.trade_month_454_start_key
    , dim_date.trade_month_544_start_key #}
    , dim_date.calendar_month_end_dt
    {# , dim_date.trade_month_445_end_dt
    , dim_date.trade_month_454_end_dt
    , dim_date.trade_month_544_end_dt #}
    , dim_date.calendar_month_end_key
    {# , dim_date.trade_month_445_end_key
    , dim_date.trade_month_454_end_key
    , dim_date.trade_month_544_end_key #}
    , dim_date.calendar_quarter_num
    {# , dim_date.trade_quarter_num #}
    , dim_date.calendar_quarter_nm
    {# , dim_date.trade_quarter_nm #}
    , dim_date.calendar_quarter_start_dt
    {# , dim_date.trade_quarter_start_dt #}
    , dim_date.calendar_quarter_start_key
    {# , dim_date.trade_quarter_start_key #}
    , dim_date.calendar_quarter_end_dt
    {# , dim_date.trade_quarter_end_dt #}
    , dim_date.calendar_quarter_end_key
    {# , dim_date.trade_quarter_end_key #}
    , dim_date.calendar_year_num
    {# , dim_date.trade_year_num #}
    , dim_date.calendar_year_start_dt
    {# , dim_date.trade_year_start_dt #}
    , dim_date.calendar_year_start_key
    {# , dim_date.trade_year_start_key #}
    , dim_date.calendar_year_end_dt
    {# , dim_date.trade_year_end_dt #}
    , dim_date.calendar_year_end_key
    {# , dim_date.trade_year_end_key #}
    , dim_date.calendar_is_leap_year_flg
    {# , dim_date.is_trade_leap_week_flg #}
    , dim_date.weeks_in_trade_year_num
    , dim_date.iso_year_num
    , dim_date.iso_week_of_year_txt
    , dim_date.iso_week_overall_num
    , dim_date.iso_week_start_dt
    , dim_date.iso_week_start_key
    , dim_date.iso_week_end_dt
    , dim_date.iso_week_end_key
    , dim_date.dw_synced_ts
    , dim_date.dw_source_nm
    , dim_date.create_user_id
    , dim_date.create_timestamp
from
    dim_date)
select  * from final
