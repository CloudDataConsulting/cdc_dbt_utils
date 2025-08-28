{{ config(materialized='table', ) }}

--@bernie Is this our prioritary time dim? Do we want this included with the codegen package or make another standalone package? This might be an easy way to solve our automotus private package issue.


{#{  config(post_hook="alter table {{ this }} add primary key (time_key)") }#}

with gapless_row_numbers as ( select
    row_number() over (order by seq4()) - 1 as row_number
  from table(generator(rowcount => 60*60*24) ) -- rowcount is 60s x 60m x 24h
),
time_list as ( select
   to_number(to_char(timeadd('second', row_number, time('00:00')), 'hh24miss')) as time_key
  , timeadd('second', row_number, time('00:00'))                                as time -- dimension starts at 00:00
  , extract(hour from time)                                                     as hour
  , extract(minute from time)                                                   as minute
  , extract(second from time)                                                   as second
  , to_varchar(time, 'hh12:mi:ss am')                                           as time_12h
  , minute = 0 and second = 0                                                   as hour_flag
  , minute%15 = 0 and second = 0                                                as quarter_hour_flag
  , hour >= 6 and hour < 18                                                     as day_shift_flag
  , not day_shift_flag                                                          as night_shift_flag
  , iff(hour < 12, 'am', 'pm')                                                  as time_period
  {{ last_run_fields() }}
from gapless_row_numbers)
, null_values as ( select
     -1 as time_key
  , null                              as time -- dimension starts at 00:00
  , -1                                                     as hour
  , -1                                                   as minute
  , -1                                                 as second
  , 'Not Set'                                           as time_12h
  , null                                                 as hour_flag
  , null                                                as quarter_hour_flag
  , null                                                     as day_shift_flag
  , null                                                          as night_shift_flag
  , 'Not Set'                                                 as time_period
  {{ last_run_fields() }})
select * from time_list
union all
select * from null_values