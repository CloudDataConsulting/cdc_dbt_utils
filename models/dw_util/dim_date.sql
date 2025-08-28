{{ config(materialized='table') }}

with sequence_gen as (
    select
      dateadd(day, seq4(), '1970-1-1' :: date) as datum
      , dateadd(day, seq4(), '1970-1-1' :: date) as fy_datum
    from table(generator(rowcount => 50000))
)
, gen_date as (
select
  to_char(datum, 'yyyymmdd') :: int as date_key
  , datum as full_date
  , datum - interval '1 year' as same_dt_last_year
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
  end::varchar(30) as day_nm
  , dayname(datum) as day_abbr
  , extract(dayofweek from datum) + 1 as day_of_week_num
  , extract(dayofweekiso from datum) as iso_day_of_week_num
  , case
  when extract(dayofweekiso from datum) IN (6, 7)
    then 'Weekend'
    else 'Weekday'
  end as weekday_flg
  , case
    when dateadd(day, 7 - extract(dayofweekiso from datum), datum) = datum
      then 1
    else 0
  end as end_of_week_flg
  , extract(DAY from datum) as day_of_month_num
  , last_day(datum, 'month') as last_day_of_month_dt
  , case
    when day_of_month_num = extract(day from last_day_of_month_dt)
      then 1
    else 0
  end as end_of_month_flg
  , case
    when mod(to_char(datum, 'dd') :: int, 10) = 1
      then to_char(datum, 'dd') :: int || 'st'
    when mod(to_char(datum, 'dd') :: int, 10) = 2
      then to_char(datum, 'dd') :: int || 'nd'
    when mod(to_char(datum, 'dd') :: int, 10) = 3
      then to_char(datum, 'dd') :: int || 'rd'
    else to_char(datum, 'dd') :: int || 'th'
  end::varchar(10) as day_suffix_txt
  , date_trunc('month', datum) as first_day_of_month_dt
  , case
    when date_trunc('month', datum) =  datum
      then 1
    else 0
  end as first_day_of_month_flg
  , datediff(day, date_trunc('quarter', datum), datum) + 1 as day_of_quarter_num
  , date_trunc('quarter', datum) as first_day_of_quarter_dt
  , last_day(datum, 'quarter') as last_day_of_quarter_dt
  , extract(dayofyear from datum) as day_of_year_num
  , (extract(year from datum) || '-01-01') :: date as first_day_of_year_dt
  , (extract(year from datum) || '-12-31') :: date as last_day_of_year_dt
  , datediff('d',datefromparts(1970,1,1), datum) as day_overall_num
  , datediff(week, date_trunc('month', datum), datum) + 1 as week_of_month_num
  , extract(week from datum) as week_of_year_num
  , (yearofweekiso(datum)
  || case
     when length(weekiso(datum)) = 1
       then concat('-W0', weekiso(datum))
     else concat('-W', weekiso(datum))
     end
  || concat('-', dayofweekiso(datum)))::varchar(15) as iso_week_of_year_txt
  , datediff('w',datefromparts(1970,1,1), datum) as week_overall_num
  , dateadd(day, 1 - extract(dayofweekiso from datum), datum) as week_begin_dt
  , to_number(
    to_char(
      dateadd(day
              , 1 - extract(dayofweekiso from datum),
              datum), 'yyyymmdd')) as week_begin_key
  , dateadd(day, 7 - extract(dayofweekiso from datum), datum) as week_end_dt
  , to_number(
    to_char(
      dateadd(day
              , 7 - extract(dayofweekiso from datum),
              datum), 'yyyymmdd')) as week_end_key
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
  end::varchar(30) as month_nm
  , monthname(datum) as month_abbr
  , extract(MONTH from datum) as month_num
  , datediff('month',datefromparts(1970,1,1), datum) as month_overall_num
  , mod(month_num - 1, 3) + 1 as month_in_quarter_num
  , extract(quarter from datum) as quarter_num
  , case
  when extract(quarter from datum) = 1
    then 'First'
  when extract(quarter from datum) = 2
    then 'Second'
  when extract(quarter from datum) = 3
    then 'Third'
  when extract(quarter from datum) = 4
    then 'Fourth'
  end::varchar(20) as quarter_nm
  , extract(year from datum) as year_num
  , extract(yearofweekiso from datum) as iso_year_num
  , to_char(datum, 'yyyymm')::number as yearmonth_num
  , case
    when (extract(year from fy_datum) || '-12-31') :: date = datum
      then 1
    else 0
  end as end_of_year_flg
  , extract(epoch_second from datum) as epoch
  , to_char(datum, 'yyyymmdd')::varchar(10) as yyyymmdd
  , current_user::varchar(100) as create_user_id
  , current_timestamp as create_timestamp
from sequence_gen
)
, final as
( select * from gen_date
union
select
    -1 as date_key
    , to_date('99991231','yyyymmdd') as full_date
    , null as same_dt_last_year
    , 'Not Set' as day_nm
    , 'N/A' as day_abbr
    , -1 as day_of_week_num
    , -1 as iso_day_of_week_num
    , 'N/A' as weekday_flg
    , 0 as end_of_week_flg
    , -1 as day_of_month_num
    , null as last_day_of_month_dt
    , 0 as end_of_month_flg
    , 'N/A' as day_suffix_txt
    , null as first_day_of_month_dt
    , 0 as first_day_of_month_flg
    , -1 as day_of_quarter_num
    , null as first_day_of_quarter_dt
    , null as last_day_of_quarter_dt
    , -1 as day_of_year_num
    , null as first_day_of_year_dt
    , null as last_day_of_year_dt
    , -1 as day_overall_num
    , -1 as week_of_month_num
    , -1 as week_of_year_num
    , 'N/A' as iso_week_of_year_txt
    , -1 as week_overall_num
    , null as week_begin_dt
    , -1 as week_begin_key
    , null as week_end_dt
    , -1 as week_end_key
    , 'Not Set' as month_nm
    , 'N/A' as month_abbr
    , -1 as month_num
    , -1 as month_overall_num
    , -1 as month_in_quarter_num
    , -1 as quarter_num
    , 'Not Set' as quarter_nm
    , -1 as year_num
    , -1 as iso_year_num
    , -1 as yearmonth_num
    , 0 as end_of_year_flg
    , -1 as epoch
    , 'N/A' as yyyymmdd
    , 'system' as create_user_id
    , current_timestamp as create_timestamp
)
select * from final
