{{ config(materialized='table') }}
{{ config( post_hook="alter table {{ this }} add primary key (date_key)", ) }}
with date_spine as (
    select dateadd(day, seq4(), '2000-01-01'::date) as calendar_date
    from table(generator(rowcount => 11323))  -- 31 years of dates
    where dateadd(day, seq4(), '2000-01-01'::date) <= '2030-12-31'::date)
, trade_year_calculations as (
    select distinct
        year(calendar_date) as calendar_year
        -- Find Sunday on or before Feb 1, then go back 4 weeks (28 days)
        , dateadd('day', -28
            , date_from_parts(year(calendar_date), 2, 1)
            - dayofweek(date_from_parts(year(calendar_date), 2, 1))
          ) as trade_year_start
    from date_spine)
, trade_year_boundaries as (
    select
        calendar_year as trade_year_num
        , trade_year_start as trade_year_start_dt
        , lead(trade_year_start, 1) over (order by calendar_year) - 1 as trade_year_end_dt
        , datediff('week', trade_year_start
            , lead(trade_year_start, 1) over (order by calendar_year)) as weeks_in_year
        , datediff('day', trade_year_start
            , lead(trade_year_start, 1) over (order by calendar_year)) as days_in_year
    from trade_year_calculations)
, week_seq as (
        select seq4() + 1 as seq
        from table(generator(rowcount => 53))
    )
, trade_weeks as (
    select
        tyb.trade_year_num
        , tyb.trade_year_start_dt
        , tyb.trade_year_end_dt
        , tyb.weeks_in_year as weeks_in_trade_year_num
        , tyb.days_in_year as days_in_trade_year_num
        , row_number() over (partition by tyb.trade_year_num order by week_seq.seq) as trade_week_num
        , dateadd('week', week_seq.seq - 1, tyb.trade_year_start_dt)::date as trade_week_start_dt
        , dateadd('day', 6, dateadd('week', week_seq.seq - 1, tyb.trade_year_start_dt))::date as trade_week_end_dt
    from trade_year_boundaries as tyb
    cross join week_seq
    where week_seq.seq <= tyb.weeks_in_year)
, trade_weeks_with_patterns as (
    select
        tw.*
        -- 4-4-5 Pattern
        , case
            when tw.trade_week_num <= 4 then 1
            when tw.trade_week_num <= 8 then 2
            when tw.trade_week_num <= 13 then 3
            when tw.trade_week_num <= 17 then 4
            when tw.trade_week_num <= 21 then 5
            when tw.trade_week_num <= 26 then 6
            when tw.trade_week_num <= 30 then 7
            when tw.trade_week_num <= 34 then 8
            when tw.trade_week_num <= 39 then 9
            when tw.trade_week_num <= 43 then 10
            when tw.trade_week_num <= 47 then 11
            else 12
        end as trade_month_445_num
        -- 4-5-4 Pattern
        , case
            when tw.trade_week_num <= 4 then 1
            when tw.trade_week_num <= 9 then 2
            when tw.trade_week_num <= 13 then 3
            when tw.trade_week_num <= 17 then 4
            when tw.trade_week_num <= 22 then 5
            when tw.trade_week_num <= 26 then 6
            when tw.trade_week_num <= 30 then 7
            when tw.trade_week_num <= 35 then 8
            when tw.trade_week_num <= 39 then 9
            when tw.trade_week_num <= 43 then 10
            when tw.trade_week_num <= 48 then 11
            else 12
        end as trade_month_454_num
        -- 5-4-4 Pattern
        , case
            when tw.trade_week_num <= 5 then 1
            when tw.trade_week_num <= 9 then 2
            when tw.trade_week_num <= 13 then 3
            when tw.trade_week_num <= 18 then 4
            when tw.trade_week_num <= 22 then 5
            when tw.trade_week_num <= 26 then 6
            when tw.trade_week_num <= 31 then 7
            when tw.trade_week_num <= 35 then 8
            when tw.trade_week_num <= 39 then 9
            when tw.trade_week_num <= 44 then 10
            when tw.trade_week_num <= 48 then 11
            else 12
        end as trade_month_544_num
        -- Quarter assignment (same for all patterns)
        , case
              when tw.trade_week_num <= 13 then 1
              when tw.trade_week_num <= 26 then 2
              when tw.trade_week_num <= 39 then 3
             else 4  -- This captures weeks 40-53, including leap week
          end as trade_quarter_num
    from trade_weeks as tw)
, trade_month_boundaries as (
    select distinct
        trade_year_num
        , trade_month_445_num
        , trade_month_454_num
        , trade_month_544_num
        , trade_quarter_num
        , min(trade_week_start_dt) over (partition by trade_year_num, trade_month_445_num) as trade_month_445_start_dt
        , max(trade_week_end_dt) over (partition by trade_year_num, trade_month_445_num) as trade_month_445_end_dt
        , min(trade_week_start_dt) over (partition by trade_year_num, trade_month_454_num) as trade_month_454_start_dt
        , max(trade_week_end_dt) over (partition by trade_year_num, trade_month_454_num) as trade_month_454_end_dt
        , min(trade_week_start_dt) over (partition by trade_year_num, trade_month_544_num) as trade_month_544_start_dt
        , max(trade_week_end_dt) over (partition by trade_year_num, trade_month_544_num) as trade_month_544_end_dt
    from trade_weeks_with_patterns)
, trade_quarter_boundaries as (
    select distinct
        trade_year_num
        , trade_quarter_num
        , min(trade_week_start_dt) over (partition by trade_year_num, trade_quarter_num) as trade_quarter_start_dt
        , max(trade_week_end_dt) over (partition by trade_year_num, trade_quarter_num) as trade_quarter_end_dt
        , count(distinct trade_week_num) over (partition by trade_year_num, trade_quarter_num) as weeks_in_quarter
        , datediff('day',
            min(trade_week_start_dt) over (partition by trade_year_num, trade_quarter_num),
            max(trade_week_end_dt) over (partition by trade_year_num, trade_quarter_num)) + 1 as days_in_quarter
    from trade_weeks_with_patterns)
, trade_dates_base as (
    select
        ds.calendar_date
        , to_char(ds.calendar_date, 'YYYYMMDD')::int as date_key
        , twp.trade_year_num
        , twp.trade_week_num
        , twp.trade_week_start_dt
        , twp.trade_week_end_dt
        , twp.trade_year_start_dt
        , twp.trade_year_end_dt
        , twp.weeks_in_trade_year_num
        , twp.days_in_trade_year_num
        , twp.trade_month_445_num
        , twp.trade_month_454_num
        , twp.trade_month_544_num
        , twp.trade_quarter_num
        , datediff('day', twp.trade_year_start_dt, ds.calendar_date) + 1 as trade_day_of_year_num
        -- Week of month calculations for each pattern
        , dense_rank() over (
            partition by twp.trade_year_num, twp.trade_month_445_num
            order by twp.trade_week_num
        ) as trade_week_of_month_445_num
        , dense_rank() over (
            partition by twp.trade_year_num, twp.trade_month_454_num
            order by twp.trade_week_num
        ) as trade_week_of_month_454_num
        , dense_rank() over (
            partition by twp.trade_year_num, twp.trade_month_544_num
            order by twp.trade_week_num
        ) as trade_week_of_month_544_num
        -- Week of quarter
        , dense_rank() over (
            partition by twp.trade_year_num, twp.trade_quarter_num
            order by twp.trade_week_num
        ) as trade_week_of_quarter_num
        -- Leap week flag
        , case when twp.trade_week_num = 53 then 1 else 0 end as trade_leap_week_flg
    from date_spine as ds
    inner join trade_weeks_with_patterns as twp
        on ds.calendar_date between twp.trade_week_start_dt and twp.trade_week_end_dt)
, trade_dates_with_boundaries as (
    select
        tdb.*
        , mb.trade_month_445_start_dt
        , mb.trade_month_445_end_dt
        , mb.trade_month_454_start_dt
        , mb.trade_month_454_end_dt
        , mb.trade_month_544_start_dt
        , mb.trade_month_544_end_dt
        , qb.trade_quarter_start_dt
        , qb.trade_quarter_end_dt
        , qb.weeks_in_quarter as weeks_in_trade_quarter_num
        , qb.days_in_quarter as days_in_trade_quarter_num
        -- Day of quarter calculation
        , datediff('day', qb.trade_quarter_start_dt, tdb.calendar_date) + 1 as trade_day_of_quarter_num
    from trade_dates_base as tdb
    left join trade_month_boundaries as mb
        on tdb.trade_year_num = mb.trade_year_num
        and tdb.trade_month_445_num = mb.trade_month_445_num
        and tdb.trade_month_454_num = mb.trade_month_454_num
        and tdb.trade_month_544_num = mb.trade_month_544_num
    left join trade_quarter_boundaries as qb
        on tdb.trade_year_num = qb.trade_year_num
        and tdb.trade_quarter_num = qb.trade_quarter_num)
, trade_year_comparison_prep as (
    select
        td.*
        -- Join to prior year week 52 for NRF method
        , ly52.date_key as prior_year_week52_key
        -- Join to same year week 1 for Walmart method (52 weeks ago for week 53)
        , w1.date_key as same_year_week1_key
        -- Standard prior year same week/day
        , ly.date_key as prior_year_same_week_key
    from trade_dates_with_boundaries as td
    -- Join to get Week 52 of prior year for Week 53 comparisons
    left join trade_dates_with_boundaries ly52
        on ly52.trade_year_num = td.trade_year_num - 1
        and ly52.trade_week_num = 52
        and dayofweek(ly52.calendar_date) = dayofweek(td.calendar_date)
    -- Join to get Week 1 of same year for Walmart method (week 53 compares to week 1 of same year - 52 weeks ago)
    left join trade_dates_with_boundaries w1
        on w1.trade_year_num = td.trade_year_num
        and w1.trade_week_num = 1
        and dayofweek(w1.calendar_date) = dayofweek(td.calendar_date)
    -- Standard join to prior year same week
    left join trade_dates_with_boundaries ly
        on ly.trade_year_num = td.trade_year_num - 1
        and td.trade_week_num = ly.trade_week_num
        and dayofweek(ly.calendar_date) = dayofweek(td.calendar_date))
, enriched_trade_dates as (
    select
        tdb.date_key
        , tdb.calendar_date as full_dt
        -- Year-over-year comparison keys
        , case
            when tdb.trade_week_num = 53 then tdb.prior_year_week52_key
            when tdb.prior_year_same_week_key is not null then tdb.prior_year_same_week_key
            else null
        end as trade_date_last_year_nrf_key
        , case
            when tdb.trade_week_num = 53 then tdb.same_year_week1_key
            when tdb.prior_year_same_week_key is not null then tdb.prior_year_same_week_key
            else null
        end as trade_date_last_year_walmart_key
        , to_char(dateadd('day', -364, tdb.calendar_date), 'YYYYMMDD')::int as trade_date_last_year_364_key
        -- Day metrics
        , tdb.trade_day_of_year_num
        , tdb.trade_day_of_quarter_num
        -- Week attributes
        , tdb.trade_week_num
        , tdb.trade_week_num as trade_week_of_year_num
        , tdb.trade_week_of_month_445_num
        , tdb.trade_week_of_month_454_num
        , tdb.trade_week_of_month_544_num
        , tdb.trade_week_of_quarter_num
        , datediff('week', '2000-01-01'::date - dayofweek('2000-01-01'::date), tdb.trade_week_start_dt) + 1 as trade_week_overall_num
        , tdb.trade_week_start_dt
        , to_char(tdb.trade_week_start_dt, 'YYYYMMDD')::int as trade_week_start_key
        , tdb.trade_week_end_dt
        , to_char(tdb.trade_week_end_dt, 'YYYYMMDD')::int as trade_week_end_key
        -- Month attributes for each pattern
        , tdb.trade_month_445_num
        , tdb.trade_month_454_num
        , tdb.trade_month_544_num
        -- Month names
        , case tdb.trade_month_445_num
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as trade_month_445_nm
        , case tdb.trade_month_454_num
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as trade_month_454_nm
        , case tdb.trade_month_544_num
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as trade_month_544_nm
        -- Month abbreviation (same for all patterns)
        , case tdb.trade_month_445_num
            when 1 then 'Jan' when 2 then 'Feb' when 3 then 'Mar'
            when 4 then 'Apr' when 5 then 'May' when 6 then 'Jun'
            when 7 then 'Jul' when 8 then 'Aug' when 9 then 'Sep'
            when 10 then 'Oct' when 11 then 'Nov' when 12 then 'Dec'
        end as trade_month_abbr
        -- Month overall and yearmonth
        , (tdb.trade_year_num - 2000) * 12 + tdb.trade_month_445_num as trade_month_overall_num
        , tdb.trade_year_num * 100 + tdb.trade_month_445_num as trade_yearmonth_num
        -- Month boundaries for each pattern
        , tdb.trade_month_445_start_dt
        , to_char(tdb.trade_month_445_start_dt, 'YYYYMMDD')::int as trade_month_445_start_key
        , tdb.trade_month_445_end_dt
        , to_char(tdb.trade_month_445_end_dt, 'YYYYMMDD')::int as trade_month_445_end_key
        , tdb.trade_month_454_start_dt
        , to_char(tdb.trade_month_454_start_dt, 'YYYYMMDD')::int as trade_month_454_start_key
        , tdb.trade_month_454_end_dt
        , to_char(tdb.trade_month_454_end_dt, 'YYYYMMDD')::int as trade_month_454_end_key
        , tdb.trade_month_544_start_dt
        , to_char(tdb.trade_month_544_start_dt, 'YYYYMMDD')::int as trade_month_544_start_key
        , tdb.trade_month_544_end_dt
        , to_char(tdb.trade_month_544_end_dt, 'YYYYMMDD')::int as trade_month_544_end_key
        -- Quarter attributes
        , tdb.trade_quarter_num
        , 'Q' || tdb.trade_quarter_num::varchar as trade_quarter_nm
        , case trade_quarter_num
            when 1 then 'First'
            when 2 then 'Second'
            when 3 then 'Third'
            when 4 then 'Fourth'
        end as trade_quarter_full_nm
        , tdb.trade_quarter_start_dt
        , to_char(tdb.trade_quarter_start_dt, 'YYYYMMDD')::int as trade_quarter_start_key
        , tdb.trade_quarter_end_dt
        , to_char(tdb.trade_quarter_end_dt, 'YYYYMMDD')::int as trade_quarter_end_key
        -- Year attributes
        , tdb.trade_year_num
        , tdb.trade_year_start_dt
        , to_char(tdb.trade_year_start_dt, 'YYYYMMDD')::int as trade_year_start_key
        , tdb.trade_year_end_dt
        , to_char(tdb.trade_year_end_dt, 'YYYYMMDD')::int as trade_year_end_key
        , tdb.trade_leap_week_flg
        , tdb.weeks_in_trade_year_num
        , tdb.days_in_trade_year_num
        , tdb.weeks_in_trade_quarter_num
        , tdb.days_in_trade_quarter_num
    from trade_year_comparison_prep as tdb)
, regular_dates as (
    select
        date_key
        , full_dt
        , trade_date_last_year_nrf_key
        , trade_date_last_year_walmart_key
        , trade_date_last_year_364_key
        , trade_day_of_year_num
        , trade_day_of_quarter_num
        , trade_week_num
        , trade_week_of_year_num
        , trade_week_of_month_445_num
        , trade_week_of_month_454_num
        , trade_week_of_month_544_num
        , trade_week_of_quarter_num
        , trade_week_overall_num
        , trade_week_start_dt
        , trade_week_start_key
        , trade_week_end_dt
        , trade_week_end_key
        , trade_month_445_num
        , trade_month_454_num
        , trade_month_544_num
        , trade_month_445_nm
        , trade_month_454_nm
        , trade_month_544_nm
        , trade_month_abbr
        , trade_month_overall_num
        , trade_yearmonth_num
        , trade_month_445_start_dt
        , trade_month_445_start_key
        , trade_month_445_end_dt
        , trade_month_445_end_key
        , trade_month_454_start_dt
        , trade_month_454_start_key
        , trade_month_454_end_dt
        , trade_month_454_end_key
        , trade_month_544_start_dt
        , trade_month_544_start_key
        , trade_month_544_end_dt
        , trade_month_544_end_key
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        , trade_quarter_start_dt
        , trade_quarter_start_key
        , trade_quarter_end_dt
        , trade_quarter_end_key
        , trade_year_num
        , trade_year_start_dt
        , trade_year_start_key
        , trade_year_end_dt
        , trade_year_end_key
        , trade_leap_week_flg
        , weeks_in_trade_year_num
        , days_in_trade_year_num
        , weeks_in_trade_quarter_num
        , days_in_trade_quarter_num
        , current_timestamp() as dw_synced_ts
        , 'TRADE_CALENDAR' as dw_source_nm
        , 'ETL_PROCESS' as create_user_id
        , current_timestamp() as create_ts
    from enriched_trade_dates)
, special_records as (
    select * from (
        values
        (
            -1                      -- date_key
            , '1900-01-01'::date    -- full_dt
            , -1                    -- trade_date_last_year_nrf_key
            , -1                    -- trade_date_last_year_walmart_key
            , -1                    -- trade_date_last_year_364_key
            , -1                    -- trade_day_of_year_num
            , -1                    -- trade_day_of_quarter_num
            , -1                    -- trade_week_num
            , -1                    -- trade_week_of_year_num
            , -1                    -- trade_week_of_month_445_num
            , -1                    -- trade_week_of_month_454_num
            , -1                    -- trade_week_of_month_544_num
            , -1                    -- trade_week_of_quarter_num
            , -1                    -- trade_week_overall_num
            , '1900-01-01'::date    -- trade_week_start_dt
            , -1                    -- trade_week_start_key
            , '1900-01-01'::date    -- trade_week_end_dt
            , -1                    -- trade_week_end_key
            , -1                    -- trade_month_445_num
            , -1                    -- trade_month_454_num
            , -1                    -- trade_month_544_num
            , 'Unknown'             -- trade_month_445_nm
            , 'Unknown'             -- trade_month_454_nm
            , 'Unknown'             -- trade_month_544_nm
            , 'UNK'                 -- trade_month_abbr
            , -1                    -- trade_month_overall_num
            , -1                    -- trade_yearmonth_num
            , '1900-01-01'::date    -- trade_month_445_start_dt
            , -1                    -- trade_month_445_start_key
            , '1900-01-01'::date    -- trade_month_445_end_dt
            , -1                    -- trade_month_445_end_key
            , '1900-01-01'::date    -- trade_month_454_start_dt
            , -1                    -- trade_month_454_start_key
            , '1900-01-01'::date    -- trade_month_454_end_dt
            , -1                    -- trade_month_454_end_key
            , '1900-01-01'::date    -- trade_month_544_start_dt
            , -1                    -- trade_month_544_start_key
            , '1900-01-01'::date    -- trade_month_544_end_dt
            , -1                    -- trade_month_544_end_key
            , -1                    -- trade_quarter_num
            , 'UNK'                 -- trade_quarter_nm
            , 'Unknown'             -- trade_quarter_full_nm
            , '1900-01-01'::date    -- trade_quarter_start_dt
            , -1                    -- trade_quarter_start_key
            , '1900-01-01'::date    -- trade_quarter_end_dt
            , -1                    -- trade_quarter_end_key
            , -1                    -- trade_year_num
            , '1900-01-01'::date    -- trade_year_start_dt
            , -1                    -- trade_year_start_key
            , '1900-01-01'::date    -- trade_year_end_dt
            , -1                    -- trade_year_end_key
            , 0                     -- trade_leap_week_flg
            , -1                    -- weeks_in_trade_year_num
            , -1                    -- days_in_trade_year_num
            , -1                    -- weeks_in_trade_quarter_num
            , -1                    -- days_in_trade_quarter_num
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -2                      -- date_key
            , '1900-01-02'::date    -- full_dt
            , -2                    -- trade_date_last_year_nrf_key
            , -2                    -- trade_date_last_year_walmart_key
            , -2                    -- trade_date_last_year_364_key
            , -2                    -- trade_day_of_year_num
            , -2                    -- trade_day_of_quarter_num
            , -2                    -- trade_week_num
            , -2                    -- trade_week_of_year_num
            , -2                    -- trade_week_of_month_445_num
            , -2                    -- trade_week_of_month_454_num
            , -2                    -- trade_week_of_month_544_num
            , -2                    -- trade_week_of_quarter_num
            , -2                    -- trade_week_overall_num
            , '1900-01-02'::date    -- trade_week_start_dt
            , -2                    -- trade_week_start_key
            , '1900-01-02'::date    -- trade_week_end_dt
            , -2                    -- trade_week_end_key
            , -2                    -- trade_month_445_num
            , -2                    -- trade_month_454_num
            , -2                    -- trade_month_544_num
            , 'Invalid'             -- trade_month_445_nm
            , 'Invalid'             -- trade_month_454_nm
            , 'Invalid'             -- trade_month_544_nm
            , 'INV'                 -- trade_month_abbr
            , -2                    -- trade_month_overall_num
            , -2                    -- trade_yearmonth_num
            , '1900-01-02'::date    -- trade_month_445_start_dt
            , -2                    -- trade_month_445_start_key
            , '1900-01-02'::date    -- trade_month_445_end_dt
            , -2                    -- trade_month_445_end_key
            , '1900-01-02'::date    -- trade_month_454_start_dt
            , -2                    -- trade_month_454_start_key
            , '1900-01-02'::date    -- trade_month_454_end_dt
            , -2                    -- trade_month_454_end_key
            , '1900-01-02'::date    -- trade_month_544_start_dt
            , -2                    -- trade_month_544_start_key
            , '1900-01-02'::date    -- trade_month_544_end_dt
            , -2                    -- trade_month_544_end_key
            , -2                    -- trade_quarter_num
            , 'INV'                 -- trade_quarter_nm
            , 'Invalid'             -- trade_quarter_full_nm
            , '1900-01-02'::date    -- trade_quarter_start_dt
            , -2                    -- trade_quarter_start_key
            , '1900-01-02'::date    -- trade_quarter_end_dt
            , -2                    -- trade_quarter_end_key
            , -2                    -- trade_year_num
            , '1900-01-02'::date    -- trade_year_start_dt
            , -2                    -- trade_year_start_key
            , '1900-01-02'::date    -- trade_year_end_dt
            , -2                    -- trade_year_end_key
            , 0                     -- trade_leap_week_flg
            , -2                    -- weeks_in_trade_year_num
            , -2                    -- days_in_trade_year_num
            , -2                    -- weeks_in_trade_quarter_num
            , -2                    -- days_in_trade_quarter_num
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
        , (
            -3                      -- date_key
            , '1900-01-03'::date    -- full_dt
            , -3                    -- trade_date_last_year_nrf_key
            , -3                    -- trade_date_last_year_walmart_key
            , -3                    -- trade_date_last_year_364_key
            , -3                    -- trade_day_of_year_num
            , -3                    -- trade_day_of_quarter_num
            , -3                    -- trade_week_num
            , -3                    -- trade_week_of_year_num
            , -3                    -- trade_week_of_month_445_num
            , -3                    -- trade_week_of_month_454_num
            , -3                    -- trade_week_of_month_544_num
            , -3                    -- trade_week_of_quarter_num
            , -3                    -- trade_week_overall_num
            , '1900-01-03'::date    -- trade_week_start_dt
            , -3                    -- trade_week_start_key
            , '1900-01-03'::date    -- trade_week_end_dt
            , -3                    -- trade_week_end_key
            , -3                    -- trade_month_445_num
            , -3                    -- trade_month_454_num
            , -3                    -- trade_month_544_num
            , 'Not Applicable'      -- trade_month_445_nm
            , 'Not Applicable'      -- trade_month_454_nm
            , 'Not Applicable'      -- trade_month_544_nm
            , 'N/A'                 -- trade_month_abbr
            , -3                    -- trade_month_overall_num
            , -3                    -- trade_yearmonth_num
            , '1900-01-03'::date    -- trade_month_445_start_dt
            , -3                    -- trade_month_445_start_key
            , '1900-01-03'::date    -- trade_month_445_end_dt
            , -3                    -- trade_month_445_end_key
            , '1900-01-03'::date    -- trade_month_454_start_dt
            , -3                    -- trade_month_454_start_key
            , '1900-01-03'::date    -- trade_month_454_end_dt
            , -3                    -- trade_month_454_end_key
            , '1900-01-03'::date    -- trade_month_544_start_dt
            , -3                    -- trade_month_544_start_key
            , '1900-01-03'::date    -- trade_month_544_end_dt
            , -3                    -- trade_month_544_end_key
            , -3                    -- trade_quarter_num
            , 'N/A'                 -- trade_quarter_nm
            , 'Not Applicable'      -- trade_quarter_full_nm
            , '1900-01-03'::date    -- trade_quarter_start_dt
            , -3                    -- trade_quarter_start_key
            , '1900-01-03'::date    -- trade_quarter_end_dt
            , -3                    -- trade_quarter_end_key
            , -3                    -- trade_year_num
            , '1900-01-03'::date    -- trade_year_start_dt
            , -3                    -- trade_year_start_key
            , '1900-01-03'::date    -- trade_year_end_dt
            , -3                    -- trade_year_end_key
            , 0                     -- trade_leap_week_flg
            , -3                    -- weeks_in_trade_year_num
            , -3                    -- days_in_trade_year_num
            , -3                    -- weeks_in_trade_quarter_num
            , -3                    -- days_in_trade_quarter_num
            , current_timestamp()   -- dw_synced_ts
            , 'SPECIAL'             -- dw_source_nm
            , 'SYSTEM'              -- create_user_id
            , current_timestamp()   -- create_ts
        )
    ) as t (
        date_key
        , full_dt
        , trade_date_last_year_nrf_key
        , trade_date_last_year_walmart_key
        , trade_date_last_year_364_key
        , trade_day_of_year_num
        , trade_day_of_quarter_num
        , trade_week_num
        , trade_week_of_year_num
        , trade_week_of_month_445_num
        , trade_week_of_month_454_num
        , trade_week_of_month_544_num
        , trade_week_of_quarter_num
        , trade_week_overall_num
        , trade_week_start_dt
        , trade_week_start_key
        , trade_week_end_dt
        , trade_week_end_key
        , trade_month_445_num
        , trade_month_454_num
        , trade_month_544_num
        , trade_month_445_nm
        , trade_month_454_nm
        , trade_month_544_nm
        , trade_month_abbr
        , trade_month_overall_num
        , trade_yearmonth_num
        , trade_month_445_start_dt
        , trade_month_445_start_key
        , trade_month_445_end_dt
        , trade_month_445_end_key
        , trade_month_454_start_dt
        , trade_month_454_start_key
        , trade_month_454_end_dt
        , trade_month_454_end_key
        , trade_month_544_start_dt
        , trade_month_544_start_key
        , trade_month_544_end_dt
        , trade_month_544_end_key
        , trade_quarter_num
        , trade_quarter_nm
        , trade_quarter_full_nm
        , trade_quarter_start_dt
        , trade_quarter_start_key
        , trade_quarter_end_dt
        , trade_quarter_end_key
        , trade_year_num
        , trade_year_start_dt
        , trade_year_start_key
        , trade_year_end_dt
        , trade_year_end_key
        , trade_leap_week_flg
        , weeks_in_trade_year_num
        , days_in_trade_year_num
        , weeks_in_trade_quarter_num
        , days_in_trade_quarter_num
        , dw_synced_ts
        , dw_source_nm
        , create_user_id
        , create_ts
    )
)
, final as (
    select * from regular_dates
    union all
    select * from special_records
    )
select * from final
