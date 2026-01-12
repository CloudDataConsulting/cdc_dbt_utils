{{
  config(
    materialized='table',
    post_hook="alter table {{ this }} add primary key (date_key)"
  )
}}
-- for snowflake only
-- change the interval on line 8 to reflect the Fiscal offset of the client
with sequence_gen as (
  -- https://docs.snowflake.net/manuals/sql-reference/functions/seq1.html#seq1-seq2-seq4-seq8
    select
      dateadd(day, seq4(), '1970-1-1' :: date)                      as datum
      , dateadd(day, seq4(), '1970-1-1' :: date)                      as fy_datum
    -- https://docs.snowflake.net/manuals/sql-reference/functions/generator.html
    from table(generator(rowcount => 50000))
)
-- https://docs.snowflake.net/manuals/sql-reference/functions-date-time.html#supported-date-and-time-parts
, gen_date as (
select
-- DATE
  --datum,
  --fy_datum,
  to_char(datum, 'yyyymmdd') :: int                              as date_key
  , datum                                                          as full_dt
  , datum - interval '1 year'                                      as prior_year_dt
-- DAY Section
  , case
    when dayname(datum) = 'Mon'
      then 'Monday'
    when dayname(datum) = 'Tue'
      then 'Tuesday'
    when dayname(datum) = 'Wed'
      then 'Wednesday'
    when dayname(datum) = 'Thu'
      then 'Thursday'
    when dayname(datum) = 'Fri'
      then 'Friday'
    when dayname(datum) = 'Sat'
      then 'Saturday'
    when dayname(datum) = 'Sun'
      then 'Sunday'
  end::varchar(30)                                               as day_nm
  , dayname(datum)                                                 as day_abbr
  , extract(dayofweek from datum) + 1                              as day_of_week_num
  , extract(dayofweekiso from datum)                               as iso_day_of_week_num
  , case
  when extract(dayofweekiso from datum) in (6, 7)
    then 'Weekend'
    else 'Weekday'
  end                                                            as weekday_flg
  , case
    when dateadd(day, 7 - extract(dayofweekiso from datum), datum) = datum
      then 1
    else 0
  end                                                            as end_of_week_flg
  , extract(day from datum)                                        as day_of_month_num
  , last_day(datum, 'month')                                       as month_end_dt
  , case
    when day_of_month_num = extract(day from month_end_dt)
      then 1
    else 0
  end                                                            as end_of_month_flg
  , case
    when mod(to_char(datum, 'dd') :: int, 10) = 1
      then to_char(datum, 'dd') :: int || 'st'
    when mod(to_char(datum, 'dd') :: int, 10) = 2
      then to_char(datum, 'dd') :: int || 'nd'
    when mod(to_char(datum, 'dd') :: int, 10) = 3
      then to_char(datum, 'dd') :: int || 'rd'
    else to_char(datum, 'dd') :: int || 'th'
  end::varchar(10)                                               as day_suffix_txt
  -- https://docs.snowflake.net/manuals/sql-reference/functions/last_day.html#last-day
  , date_trunc('month', datum)                                     as month_begin_dt
  , case
    when date_trunc('month', datum) =  datum
      then 1
    else 0
  end                                                            as first_day_of_month_flg
  -- https://docs.snowflake.net/manuals/sql-reference/functions/dayname.html#dayname
  -- https://docs.snowflake.net/manuals/sql-reference/functions/datediff.html#datediff
  , datediff(day, date_trunc('quarter', datum), datum) + 1         as day_of_quarter_num
  , date_trunc('quarter', datum)                                   as quarter_begin_dt
  , last_day(datum, 'quarter')                                     as quarter_end_dt
  , extract(dayofyear from datum)                                  as day_of_year_num
  -- bug found 5/11/2021 Bernie Pruss changed yearofweekiso to year - keep testing.
  , (extract(year from datum) || '-01-01') :: date        as year_begin_dt
  , (extract(year from datum) || '-12-31') :: date        as year_end_dt
  , datediff('d',datefromparts(1970,1,1), datum)                   as day_overall_num 
-- FISCAL DAY Section
  -- https://docs.snowflake.net/manuals/sql-reference/functions/last_day.html#last-day
/*
  date_trunc('month', fy_datum)                                     as fiscal_first_day_of_month,
  last_day(fy_datum, 'month')                                       as fiscal_last_day_of_month,
  case
    when dateadd(day, 7 - extract(dayofweekiso from fy_datum), fy_datum) = fy_datum
      then 1
    else 0
  end                                                            as fiscal_end_of_week_flag, 
  day_of_year_number                                             as fiscal_day_of_the_year_number,  
  day_of_month_number                                            as fiscal_day_of_month_number,  
  case 
    when day_of_month_number = extract(day from last_day_of_month)
      then 1
    else 0
  end                                                            as fiscal_end_of_month_flag,  
  case
    when (extract(year from fy_datum) || '-01-01') :: date = datum   --  Check if fiscal year is the same as the natural year  it had year + 1
      then 1
    else 0
  end                                                            as fiscal_start_of_year_flag, 
  datediff(day, date_trunc('quarter', fy_datum), fy_datum) + 1   as fiscal_day_of_quarter_number,
  extract(dayofyear from fy_datum)                               as fiscal_day_of_year_number,
  date_trunc('quarter', fy_datum)                                as fiscal_first_day_of_quarter,
  last_day(fy_datum, 'quarter')                                  as fiscal_last_day_of_quarter,
  (extract(yearofweekiso from fy_datum) || '-01-01') :: date     as fiscal_first_day_of_year, -- I don't trust this. 
  (extract(yearofweekiso from fy_datum) || '-12-31') :: date     as fiscal_last_day_of_year,  -- I don't trust this.
*/
--  datediff('d',datefromparts(1970,1,1), fy_datum)                 as day_number_overall, 
-- WEEK Section
  , datediff(week, date_trunc('month', datum), datum) + 1          as week_of_month_num
  , extract(week from datum)                                       as week_of_year_num
  -- snowflake weekiso doesn't properly return guaranteed two digit weeks
  , (yearofweekiso(datum)
  || case
     when length(weekiso(datum)) = 1
       then concat('-W0', weekiso(datum))
     else concat('-W', weekiso(datum))
     end
  || concat('-', dayofweekiso(datum)))::varchar(15)               as iso_week_of_year_txt
  , datediff('w',datefromparts(1970,1,1), datum)                   as week_overall_num
  -- https://docs.snowflake.net/manuals/sql-reference/functions/dateadd.html#dateadd
  , dateadd(day, 1 - extract(dayofweekiso from datum), datum)      as week_begin_dt
  , to_number(
    to_char(
      dateadd(day
              , 1 - extract(dayofweekiso from datum)
              , datum), 'yyyymmdd'))                               as week_begin_key
  , dateadd(day, 7 - extract(dayofweekiso from datum), datum)      as week_end_dt
  , to_number(
    to_char(
      dateadd(day
              , 7 - extract(dayofweekiso from datum)
              , datum), 'yyyymmdd'))                               as week_end_key
-- FISCAL WEEK Section
/*
  datediff(week, date_trunc('month', datum), datum) + 1          as fiscal_week_of_month,
  datediff('w',datefromparts(1970,1,1), datum)                   as fiscal_week_num_overall,
  -- https://docs.snowflake.net/manuals/sql-reference/functions/dateadd.html#dateadd 
  dateadd(day, 1 - extract(dayofweekiso from datum), datum)      as fiscal_week_begin_date,
    to_number(
    to_char(
      dateadd(day, 
              1 - extract(dayofweekiso from datum), 
              datum), 'yyyymmdd'))                               as fiscal_week_begin_date_id,
  dateadd(day, 7 - extract(dayofweekiso from datum), datum)      as fiscal_week_end_date,
  to_number(
    to_char(
      dateadd(day, 
              7 - extract(dayofweekiso from datum), 
              datum), 'yyyymmdd'))                               as fiscal_week_end_date_id,
  week_of_year_number                                            as fiscal_week_of_year_number,
  -- snowflake weekiso doesn't properly return guaranteed two digit weeks
  (yearofweekiso(fy_datum) + 1
  || case
     when length(weekiso(fy_datum)) = 1
       then concat('-W0', weekiso(fy_datum))
     else concat('-W', weekiso(fy_datum))
     end
  || concat('-', dayofweekiso(fy_datum)))::varchar(15) as fiscal_week_of_year_iso_number,
*/
-- MONTH Section
  -- https://docs.snowflake.net/manuals/sql-reference/functions/monthname.html#monthname
  , case
    when monthname(datum) = 'Jan'
      then 'January'
    when monthname(datum) = 'Feb'
      then 'February'
    when monthname(datum) = 'Mar'
      then 'March'
    when monthname(datum) = 'Apr'
      then 'April'
    when monthname(datum) = 'May'
      then 'May'
    when monthname(datum) = 'Jun'
      then 'June'
    when monthname(datum) = 'Jul'
      then 'July'
    when monthname(datum) = 'Aug'
      then 'August'
    when monthname(datum) = 'Sep'
      then 'September'
    when monthname(datum) = 'Oct'
      then 'October'
    when monthname(datum) = 'Nov'
      then 'November'
    when monthname(datum) = 'Dec'
      then 'December'
  end::varchar(30)                                               as month_nm
  , monthname(datum)                                               as month_abbr
  , extract(month from datum)                                      as month_num
  , datediff('month',datefromparts(1970,1,1), datum)               as month_overall_num
  , mod(month_num - 1, 3) + 1                                      as month_in_quarter_num
/*
-- FISCAL MONTH Section
  extract(MONTH from fy_datum)                                   as fiscal_month_number,
  month_name::varchar(15)                                                     as fiscal_month_name, 
  monthname(datum)                                               as fiscal_month_abbreviation,
  month_in_quarter_number                                        as fiscal_month_in_quarter_number,  
*/
-- QUARTER Section
  , extract(quarter from datum)                                    as quarter_num
  , case
  when extract(quarter from datum) = 1
    then 'First'
  when extract(quarter from datum) = 2
    then 'Second'
  when extract(quarter from datum) = 3
    then 'Third'
  when extract(quarter from datum) = 4
    then 'Fourth'
  end::varchar(20)                                               as quarter_nm
/*
-- FISCAL QUARTER Section
  extract(quarter from fy_datum)                                 as fiscal_quarter_number,
  case
  when extract(quarter from fy_datum) = 1
    then 'First'
  when extract(quarter from fy_datum) = 2
    then 'Second'
  when extract(quarter from fy_datum) = 3
    then 'Third'
  when extract(quarter from fy_datum) = 4
    then 'Fourth'
  end::varchar(20)                                               as fiscal_quarter_name,
*/
-- YEAR Section
  , extract(year from datum)                                       as year_num
  , extract(yearofweekiso from datum)                              as iso_year_num
  , to_char(datum, 'yyyymm')::number                               as yearmonth_num
  , case
    when (extract(year from fy_datum) || '-12-31') :: date = datum
      then 1
    else 0
  end                                                            as end_of_year_flg 
/*
-- FISCAL YEAR Section
  extract(year from fy_datum)                                    as fiscal_year_number,
  extract(yearofweekiso from fy_datum)                           as fiscal_year_number_iso,
  to_char(datum, 'yyyymm')::varchar(30)                          as fiscal_yearmonth_number,
  extract(yearofweek from fy_datum)                              as fiscal_year_actual,
  extract(yearofweekiso from fy_datum)                           as fiscal_year_actual_iso,
  case
    when (extract(year from fy_datum) || '-12-31') :: date = datum
      then 1
    else 0
  end                                                            as fiscal_end_of_year_flag, 
*/
-- OTHERS Section
  , extract(epoch_second from datum)                               as epoch_num
  , to_char(datum, 'yyyymmdd')::varchar(10)                        as yyyymmdd_txt
  , current_user::varchar(100)                                     as create_user_id
  , current_timestamp                                              as create_timestamp
from sequence_gen
)
, final as 
(
select * from gen_date
union all
select
    -1 as date_key
    , to_date('99991231','yyyymmdd') as full_dt
    , null as prior_year_dt
    , 'Not Set' as day_nm
    , null as day_abbr
    , null as day_of_week_num
    , null as iso_day_of_week_num
    , null as weekday_flg
    , null as end_of_week_flg
    , null as day_of_month_num
    , null as month_end_dt
    , null as end_of_month_flg
    , null as day_suffix_txt
    , null as month_begin_dt
    , null as first_day_of_month_flg
    , null as day_of_quarter_num
    , null as quarter_begin_dt
    , null as quarter_end_dt
    , null as day_of_year_num
    , null as year_begin_dt
    , null as year_end_dt
    , null as day_overall_num
    , null as week_of_month_num
    , null as week_of_year_num
    , null as iso_week_of_year_txt
    , null as week_overall_num
    , null as week_begin_dt
    , null as week_begin_key
    , null as week_end_dt
    , null as week_end_key
    , null as month_nm
    , null as month_abbr
    , null as month_num
    , null as month_overall_num
    , null as month_in_quarter_num
    , null as quarter_num
    , null as quarter_nm
    , null as year_num
    , null as iso_year_num
    , null as yearmonth_num
    , null as end_of_year_flg
    , null as epoch_num
    , null as yyyymmdd_txt
    , null as create_user_id
    , null as create_timestamp
)
select
    date_key
    , full_dt
    , prior_year_dt
    , day_nm
    , day_abbr
    , day_of_week_num
    , iso_day_of_week_num
    , weekday_flg
    , end_of_week_flg
    , month_nm
    , month_abbr
    , month_num
    , month_overall_num
    , month_in_quarter_num
    , day_of_month_num
    , month_end_dt
    , end_of_month_flg
    , day_suffix_txt
    , month_begin_dt
    , first_day_of_month_flg
    , day_of_quarter_num
    , quarter_begin_dt
    , quarter_end_dt
    , day_of_year_num
    , year_begin_dt
    , year_end_dt
    , day_overall_num
    , week_of_month_num
    , week_of_year_num
    , iso_week_of_year_txt
    , week_overall_num
    , week_begin_dt
    , week_begin_key
    , week_end_dt
    , week_end_key
    , quarter_num
    , quarter_nm
    , year_num
    , iso_year_num
    , yearmonth_num
    , end_of_year_flg
    , epoch_num
    , yyyymmdd_txt
    , create_user_id
    , create_timestamp
from final
