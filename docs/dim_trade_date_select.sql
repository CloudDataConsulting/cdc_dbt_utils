-- this select statement is for testing and development purposes only
-- it was a way to verfy that claude code, and claude the app did not randomly rename columns.
select
    date.date_key
    , date.full_dt
    , date.trade_date_last_year_key
    , date.trade_day_of_year_num
    , date.trade_week_num
    , date.weeks_in_trade_year_num
    , date.trade_week_of_year_num
    , date.trade_week_of_month_445_num
    , date.trade_week_of_month_454_num
    , date.trade_week_of_month_544_num
    , date.trade_week_of_quarter_num
    , date.trade_week_overall_num
    , date.trade_week_start_dt
    , date.trade_week_start_key
    , date.trade_week_end_dt
    , date.trade_week_end_key
    , date.trade_month_445_num
    , date.trade_month_454_num
    , date.trade_month_544_num
    , date.trade_month_445_nm
    , date.trade_month_454_nm
    , date.trade_month_544_nm
    , date.trade_month_abbr
    , date.trade_month_overall_num
    , date.trade_yearmonth_num
    , date.trade_month_445_start_dt
    , date.trade_month_454_start_dt
    , date.trade_month_544_start_dt
    , date.trade_month_445_start_key
    , date.trade_month_454_start_key
    , date.trade_month_544_start_key
    , date.trade_month_445_end_dt
    , date.trade_month_454_end_dt
    , date.trade_month_544_end_dt
    , date.trade_month_445_end_key
    , date.trade_month_454_end_key
    , date.trade_month_544_end_key
    , date.trade_quarter_num
    , date.trade_quarter_nm
    , date.trade_quarter_full_nm
    , date.trade_quarter_start_dt
    , date.trade_quarter_start_key
    , date.trade_quarter_end_dt
    , date.trade_quarter_end_key
    , date.trade_year_num
    , date.trade_year_start_dt
    , date.trade_year_start_key
    , date.trade_year_end_dt
    , date.trade_year_end_key
    , date.trade_leap_week_flg
    , date.DW_SYNCED_TS
    , date.DW_SOURCE_NM
    , date.CREATE_USER_ID
    , date.CREATE_TS
from bpruss_dw_util.dim_trade_date date;
