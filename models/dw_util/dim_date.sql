{{ config(materialized='table', ) }}

{# {{ config(
    post_hook="alter table {{ this }} add primary key (date_key)",
) }} #}

-- for snowflake only
-- change the interval on line 8 to reflect the Fiscal offset of the client

with sequence_gen as (
  -- https://docs.snowflake.net/manuals/sql-reference/functions/seq1.html#seq1-seq2-seq4-seq8
    select
      dateadd(day, seq4(), '1970-1-1'::date) as datum
      , dateadd(day, seq4(), '1970-1-1'::date) as fy_datum
    -- https://docs.snowflake.net/manuals/sql-reference/functions/generator.html
    from table(generator(rowcount => 50000))
)
-- https://docs.snowflake.net/manuals/sql-reference/functions-date-time.html#supported-date-and-time-parts
, gen_date as (
select
-- DATE
  --datum,
  --fy_datum,
  to_char(datum, 'yyyymmdd')::int as date_key
  , datum as full_date
  , datum - interval '1 year' as same_date_last_year

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
  end::varchar(30) as day_name
  , dayname(datum) as day_abbreviation
  , extract(dayofweek from datum) + 1 as day_of_week_number
  , extract(dayofweekiso from datum) as day_of_week_number_iso
  , case
  when extract(dayofweekiso from datum) in (6, 7)
    then 'Weekend'
    else 'Weekday'
  end as weekday_flag
  , case
    when dateadd(day, 7 - extract(dayofweekiso from datum), datum) = datum
      then 1
    else 0
  end as end_of_week_flag
  , extract(day from datum) as day_of_month_number
  , last_day(datum, 'month') as last_day_of_month
  , case
    when day_of_month_number = extract(day from last_day_of_month)
      then 1
    else 0
  end as end_of_month_flag
  , case
    when mod(to_char(datum, 'dd')::int, 10) = 1
      then to_char(datum, 'dd')::int || 'st'
    when mod(to_char(datum, 'dd')::int, 10) = 2
      then to_char(datum, 'dd')::int || 'nd'
    when mod(to_char(datum, 'dd')::int, 10) = 3
      then to_char(datum, 'dd')::int || 'rd'
    else to_char(datum, 'dd')::int || 'th'
  end::varchar(10) as day_number_suffix
  -- https://docs.snowflake.net/manuals/sql-reference/functions/last_day.html#last-day
  , date_trunc('month', datum) as first_day_of_month
  , case
    when date_trunc('month', datum) = datum
      then 1
    else 0
  end as first_day_of_month_flag
  -- https://docs.snowflake.net/manuals/sql-reference/functions/dayname.html#dayname
  -- https://docs.snowflake.net/manuals/sql-reference/functions/datediff.html#datediff
  , datediff(day, date_trunc('quarter', datum), datum) + 1 as day_of_quarter_number
  , date_trunc('quarter', datum) as first_day_of_quarter
  , last_day(datum, 'quarter') as last_day_of_quarter
  , extract(dayofyear from datum) as day_of_year_number
  -- bug found 5/11/2021 Bernie Pruss changed yearofweekiso to year - keep testing. 
  , (extract(year from datum) || '-01-01')::date as first_day_of_year
  , (extract(year from datum) || '-12-31')::date as last_day_of_year
  , datediff('d', datefromparts(1970, 1, 1), datum) as day_number_overall

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
  , datediff(week, date_trunc('month', datum), datum) + 1 as week_of_month
  , extract(week from datum) as week_of_year_number
  -- snowflake weekiso doesn't properly return guaranteed two digit weeks
  , (yearofweekiso(datum)
  || case
     when length(weekiso(datum)) = 1
       then concat('-W0', weekiso(datum))
     else concat('-W', weekiso(datum))
     end
  || concat('-', dayofweekiso(datum)))::varchar(15) as week_of_year_number_iso
  , datediff('w', datefromparts(1970, 1, 1), datum) as week_num_overall
  -- https://docs.snowflake.net/manuals/sql-reference/functions/dateadd.html#dateadd 
  , dateadd(day, 1 - extract(dayofweekiso from datum), datum) as week_begin_date
  , to_number(
    to_char(
      dateadd(day
              , 1 - extract(dayofweekiso from datum)
              , datum), 'yyyymmdd')) as week_begin_date_id
  , dateadd(day, 7 - extract(dayofweekiso from datum), datum) as week_end_date
  , to_number(
    to_char(
      dateadd(day
              , 7 - extract(dayofweekiso from datum)
              , datum), 'yyyymmdd')) as week_end_date_id

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
  end::varchar(30) as month_name
  , monthname(datum) as month_abbreviation
  , extract(month from datum) as month_number
  , datediff('month', datefromparts(1970, 1, 1), datum) as month_number_overall
  , mod(month_number - 1, 3) + 1 as month_in_quarter_number
/*
-- FISCAL MONTH Section
  extract(MONTH from fy_datum)                                   as fiscal_month_number,
  month_name::varchar(15)                                                     as fiscal_month_name,
  monthname(datum)                                               as fiscal_month_abbreviation,
  month_in_quarter_number                                        as fiscal_month_in_quarter_number,
*/

-- QUARTER Section
  , extract(quarter from datum) as quarter_number
  , case
  when extract(quarter from datum) = 1
    then 'First'
  when extract(quarter from datum) = 2
    then 'Second'
  when extract(quarter from datum) = 3
    then 'Third'
  when extract(quarter from datum) = 4
    then 'Fourth'
  end::varchar(20) as quarter_name

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
  , extract(year from datum) as year_number
  , extract(yearofweekiso from datum) as year_number_iso
  , to_char(datum, 'yyyymm')::number as yearmonth_number
  , case
    when (extract(year from fy_datum) || '-12-31')::date = datum
      then 1
    else 0
  end as end_of_year_flag

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
  , extract(epoch_second from datum) as epoch
  , to_char(datum, 'yyyymmdd')::varchar(10) as yyyymmdd
  , current_user::varchar(100) as create_user_id
  , current_timestamp as create_timestamp
from sequence_gen
)
, final as (
select * from gen_date
union all
select
    -1 as date_key
    , to_date('99991231', 'yyyymmdd') as full_date
    , null as same_date_last_year
    , 'Not Set' as day_name
    , null as day_abbreviation
    , null as day_of_week_number
    , null as day_of_week_number_iso
    , null as weekday_flag
    , null as end_of_week_flag
    , null as month_name
    , null as month_abbreviation
    , null as month_number
    , null as month_number_overall
    , null as month_in_quarter_number
    , null as day_of_month_number
    , null as last_day_of_month
    , null as end_of_month_flag
    , null as day_number_suffix
    , null as first_day_of_month
    , null as first_day_of_month_flag
    , null as day_of_quarter_number
    , null as first_day_of_quarter
    , null as last_day_of_quarter
    , null as day_of_year_number
    , null as first_day_of_year
    , null as last_day_of_year
    , null as day_number_overall
    , null as week_of_month
    , null as week_of_year_number
    , null as week_of_year_number_iso
    , null as week_num_overall
    , null as week_begin_date
    , null as week_begin_date_id
    , null as week_end_date
    , null as week_end_date_id
    , null as quarter_number
    , null as quarter_name
    , null as year_number
    , null as year_number_iso
    , null as yearmonth_number
    , null as end_of_year_flag
    , null as epoch
    , null as yyyymmdd
    , null as create_user_id
    , null as create_timestamp
)
select
    -- Date key
    date_key
    , full_date
    , same_date_last_year

    -- Day columns
    , day_name as day_nm
    , day_abbreviation as day_abbr
    , day_of_week_number as day_of_week_num
    , day_of_week_number_iso as iso_day_of_week_num
    , weekday_flag as weekday_flg
    , end_of_week_flag as end_of_week_flg

    -- Month columns  
    , month_name as month_nm
    , month_abbreviation as month_abbr
    , month_number as month_num
    , month_number_overall as month_overall_num
    , month_in_quarter_number as month_in_quarter_num
    , day_of_month_number as day_of_month_num
    , last_day_of_month
    , end_of_month_flag as end_of_month_flg
    , day_number_suffix as day_suffix_txt
    , first_day_of_month
    , first_day_of_month_flag as first_day_of_month_flg

    -- Quarter columns
    , day_of_quarter_number as day_of_quarter_num
    , first_day_of_quarter
    , last_day_of_quarter

    -- Year columns
    , day_of_year_number as day_of_year_num
    , first_day_of_year
    , last_day_of_year
    , day_number_overall as day_overall_num

    -- Week columns
    , week_of_month as week_of_month_num
    , week_of_year_number as week_of_year_num
    , week_of_year_number_iso as iso_week_of_year_txt
    , week_num_overall as week_overall_num
    , week_begin_date as week_begin_dt
    , week_begin_date_id as week_begin_key
    , week_end_date as week_end_dt
    , week_end_date_id as week_end_key

    -- Quarter and Year
    , quarter_number as quarter_num
    , quarter_name as quarter_nm
    , year_number as year_num
    , year_number_iso as iso_year_num
    , yearmonth_number as yearmonth_num

    -- Other columns
    , end_of_year_flag as end_of_year_flg
    , epoch
    , yyyymmdd
    , create_user_id
    , create_timestamp
from final
