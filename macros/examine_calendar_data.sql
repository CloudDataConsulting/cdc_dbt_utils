-- Macros to examine calendar dimension data for debugging and validation
-- Usage: dbt run-operation examine_quarter_data
--        dbt run-operation examine_month_data  
--        dbt run-operation examine_date_boundary

{% macro examine_quarter_data() %}
  {% set query %}
    select 
        quarter_key,
        year_num,
        quarter_num,
        quarter_nm,
        quarter_start_dt::varchar as quarter_start_dt,
        quarter_end_dt::varchar as quarter_end_dt,
        first_month_of_quarter_num,
        last_month_of_quarter_num,
        months_in_quarter_num,
        first_month_nm || ', ' || second_month_nm || ', ' || third_month_nm as months,
        weeks_in_quarter_num,
        days_in_quarter_num
    from {{ ref('dim_quarter') }}
    where year_num >= year(current_date()) - 1
    order by quarter_key
    limit 20
  {% endset %}

  {% set results = run_query(query) %}
  
  {% if execute %}
    {{ log("=== QUARTER DATA EXAMINATION ===", info=true) }}
    {% for row in results %}
      {{ log("Q" ~ row[0] ~ " Y:" ~ row[1] ~ " Q" ~ row[2] ~ " " ~ row[3] ~ " [" ~ row[4] ~ " to " ~ row[5] ~ "] Months:" ~ row[6] ~ "-" ~ row[7] ~ "(" ~ row[8] ~ ") " ~ row[9] ~ " Weeks:" ~ row[10] ~ " Days:" ~ row[11], info=true) }}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro examine_month_data() %}
  {% set query %}
    select 
        month_key,
        year_num,
        month_num,
        month_nm,
        quarter_num,
        month_start_dt::varchar as month_start_dt,
        month_end_dt::varchar as month_end_dt,
        weeks_in_month_num,
        days_in_month_num,
        first_week_of_month_num,
        last_week_of_month_num,
        month_in_quarter_num
    from {{ ref('dim_month') }}
    where year_num >= year(current_date()) - 1
    order by month_key
    limit 30
  {% endset %}

  {% set results = run_query(query) %}
  
  {% if execute %}
    {{ log("=== MONTH DATA EXAMINATION ===", info=true) }}
    {% for row in results %}
      {{ log("M" ~ row[0] ~ " Y:" ~ row[1] ~ " M" ~ row[2] ~ " " ~ row[3] ~ " Q" ~ row[4] ~ " [" ~ row[5] ~ " to " ~ row[6] ~ "] Weeks:" ~ row[7] ~ " Days:" ~ row[8] ~ " FirstW:" ~ row[9] ~ " LastW:" ~ row[10] ~ " PosInQ:" ~ row[11], info=true) }}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro examine_date_boundary() %}
  {% set query %}
    select 
        date_key,
        full_dt::varchar as full_dt,
        dayname(full_dt) as day_name,
        week_num,
        week_of_year_num,
        week_of_month_num,
        week_of_quarter_num,
        month_num,
        quarter_num,
        year_num
    from {{ ref('dim_date') }}
    where full_dt between 
        dateadd(day, -7, date_trunc('year', current_date()))
        and dateadd(day, 10, date_trunc('year', current_date()))
    order by full_dt
  {% endset %}

  {% set results = run_query(query) %}
  
  {% if execute %}
    {{ log("=== DATE BOUNDARY EXAMINATION (Year Transition) ===", info=true) }}
    {% for row in results %}
      {{ log(row[0] ~ " " ~ row[1] ~ " " ~ row[2] ~ " W:" ~ row[3] ~ " WofY:" ~ row[4] ~ " WofM:" ~ row[5] ~ " WofQ:" ~ row[6] ~ " M:" ~ row[7] ~ " Q:" ~ row[8] ~ " Y:" ~ row[9], info=true) }}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro validate_calendar_data() %}
  -- Comprehensive validation of calendar dimension data quality
  {% set checks = [] %}
  
  -- Check 1: Month names
  {% set month_check %}
    select count(*) as issues
    from {{ ref('dim_month') }}
    where (month_num = 1 and month_nm != 'January')
       or (month_num = 2 and month_nm != 'February')
       or (month_num = 3 and month_nm != 'March')
       or (month_num = 4 and month_nm != 'April')
       or (month_num = 5 and month_nm != 'May')
       or (month_num = 6 and month_nm != 'June')
       or (month_num = 7 and month_nm != 'July')
       or (month_num = 8 and month_nm != 'August')
       or (month_num = 9 and month_nm != 'September')
       or (month_num = 10 and month_nm != 'October')
       or (month_num = 11 and month_nm != 'November')
       or (month_num = 12 and month_nm != 'December')
  {% endset %}
  
  -- Check 2: Quarter names
  {% set quarter_check %}
    select count(*) as issues
    from {{ ref('dim_quarter') }}
    where (quarter_num = 1 and quarter_nm != 'First')
       or (quarter_num = 2 and quarter_nm != 'Second')
       or (quarter_num = 3 and quarter_nm != 'Third')
       or (quarter_num = 4 and quarter_nm != 'Fourth')
  {% endset %}
  
  -- Check 3: Duplicate keys
  {% set date_dup_check %}
    select count(*) as issues
    from (
      select date_key, count(*) as cnt
      from {{ ref('dim_date') }}
      group by date_key
      having count(*) > 1
    )
  {% endset %}
  
  {% set week_dup_check %}
    select count(*) as issues
    from (
      select week_key, count(*) as cnt
      from {{ ref('dim_week') }}
      group by week_key
      having count(*) > 1
    )
  {% endset %}
  
  {% set month_dup_check %}
    select count(*) as issues
    from (
      select month_key, count(*) as cnt
      from {{ ref('dim_month') }}
      group by month_key
      having count(*) > 1
    )
  {% endset %}
  
  {% set quarter_dup_check %}
    select count(*) as issues
    from (
      select quarter_key, count(*) as cnt
      from {{ ref('dim_quarter') }}
      group by quarter_key
      having count(*) > 1
    )
  {% endset %}
  
  {% if execute %}
    {{ log("=== CALENDAR DATA VALIDATION ===", info=true) }}
    
    {% set result = run_query(month_check) %}
    {% set month_issues = result.columns[0].values()[0] %}
    {{ log("Month Names: " ~ (month_issues == 0 and "✓ PASS" or "✗ FAIL (" ~ month_issues ~ " issues)"), info=true) }}
    
    {% set result = run_query(quarter_check) %}
    {% set quarter_issues = result.columns[0].values()[0] %}
    {{ log("Quarter Names: " ~ (quarter_issues == 0 and "✓ PASS" or "✗ FAIL (" ~ quarter_issues ~ " issues)"), info=true) }}
    
    {% set result = run_query(date_dup_check) %}
    {% set date_dups = result.columns[0].values()[0] %}
    {{ log("Date Key Uniqueness: " ~ (date_dups == 0 and "✓ PASS" or "✗ FAIL (" ~ date_dups ~ " duplicates)"), info=true) }}
    
    {% set result = run_query(week_dup_check) %}
    {% set week_dups = result.columns[0].values()[0] %}
    {{ log("Week Key Uniqueness: " ~ (week_dups == 0 and "✓ PASS" or "✗ FAIL (" ~ week_dups ~ " duplicates)"), info=true) }}
    
    {% set result = run_query(month_dup_check) %}
    {% set month_dups = result.columns[0].values()[0] %}
    {{ log("Month Key Uniqueness: " ~ (month_dups == 0 and "✓ PASS" or "✗ FAIL (" ~ month_dups ~ " duplicates)"), info=true) }}
    
    {% set result = run_query(quarter_dup_check) %}
    {% set quarter_dups = result.columns[0].values()[0] %}
    {{ log("Quarter Key Uniqueness: " ~ (quarter_dups == 0 and "✓ PASS" or "✗ FAIL (" ~ quarter_dups ~ " duplicates)"), info=true) }}
    
    {% set total_issues = month_issues + quarter_issues + date_dups + week_dups + month_dups + quarter_dups %}
    {{ log("", info=true) }}
    {{ log("OVERALL: " ~ (total_issues == 0 and "✓ ALL CHECKS PASS" or "✗ " ~ total_issues ~ " TOTAL ISSUES FOUND"), info=true) }}
  {% endif %}
{% endmacro %}