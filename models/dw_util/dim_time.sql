{{
  config(
    materialized='table',
    post_hook="alter table {{ this }} add primary key (time_key)"
  )
}}
with gapless_row_numbers as (
  select
    row_number() over (order by seq4()) - 1 as row_number
  from table(generator(rowcount => 60*60*24) ) -- rowcount is 60s x 60m x 24h
)
, time_list as (
select
   to_number(to_char(timeadd('second', row_number, time('00:00')), 'hh24miss')) as time_key
  , timeadd('second', row_number, time('00:00'))                                as full_time -- dimension starts at 00:00
  , extract(hour from full_time)                                                as hour_num
  , extract(minute from full_time)                                              as minute_num
  , extract(second from full_time)                                              as second_num
  , to_varchar(full_time, 'hh12:mi:ss am')                                      as time_12h_txt
  , minute_num = 0 and second_num = 0                                           as hour_flg
  , minute_num%15 = 0 and second_num = 0                                        as quarter_hour_flg
  , hour_num >= 6 and hour_num < 18                                             as day_shift_flg
  , not day_shift_flg                                                           as night_shift_flg
  , iff(hour_num < 12, 'am', 'pm')                                              as time_period_txt
  {{ last_run_fields() }}
from gapless_row_numbers
)
, null_values as (
  select
     -1 as time_key
  , time('00:00:00')                                    as full_time
  , -1                                                  as hour_num
  , -1                                                  as minute_num
  , -1                                                  as second_num
  , 'Not Set'                                           as time_12h_txt
  , false                                               as hour_flg
  , false                                               as quarter_hour_flg
  , false                                               as day_shift_flg
  , false                                               as night_shift_flg
  , 'Not Set'                                           as time_period_txt
  {{ last_run_fields() }}
)
select * from time_list
union all
select * from null_values
