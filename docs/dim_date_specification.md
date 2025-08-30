# DIM_DATE and DIM_TRADE_DATE Complete Specification

## Table of Contents
1. [DIM_DATE (Standard Calendar) Specification](#dim_date-standard-calendar-specification)
2. [DIM_TRADE_DATE (4-5-4 Calendar) Specification](#dim_trade_date-4-5-4-calendar-specification)
3. [SQL Validation Queries](#sql-validation-queries)
4. [Generation Approach](#generation-approach)
5. [Critical Validation Checklist](#critical-validation-checklist)

---

## DIM_DATE (Standard Calendar) Specification

### Core Business Rules

#### Week Definition
- **Start Day**: Weeks start on Sunday (day 1) and end on Saturday (day 7)
- **Week 1 Rule**: Week 1 ALWAYS contains January 1st, regardless of what day of week it falls on
- **Partial Weeks**: Accepted and expected at year boundaries
- **Week Count**: Years have either 52 or 53 weeks depending on calendar layout

#### Year Boundary Handling
- If January 1 is not a Sunday, Week 1 starts on the Sunday BEFORE January 1 (in the previous calendar year)
- If December 31 is not a Saturday, that week extends into the next calendar year
- Dates are attributed to the calendar year they fall in (e.g., Jan 1, 2025 has YEAR_NUM = 2025)

### Example Year Boundaries

```
2024: January 1 = Monday
- Week 53 of 2023: Sun Dec 24, 2023 - Sat Dec 30, 2023
- Week 1 of 2024:  Sun Dec 31, 2023 - Sat Jan 6, 2024 (contains Jan 1, 2024)
- Week 2 of 2024:  Sun Jan 7, 2024 - Sat Jan 13, 2024

2025: January 1 = Wednesday
- Week 52 of 2024: Sun Dec 22, 2024 - Sat Dec 28, 2024
- Week 1 of 2025:  Sun Dec 29, 2024 - Sat Jan 4, 2025 (contains Jan 1, 2025)
- Week 2 of 2025:  Sun Jan 5, 2025 - Sat Jan 11, 2025

2026: January 1 = Thursday
- Week 52 of 2025: Sun Dec 21, 2025 - Sat Dec 27, 2025
- Week 1 of 2026:  Sun Dec 28, 2025 - Sat Jan 3, 2026 (contains Jan 1, 2026)
- Week 2 of 2026:  Sun Jan 4, 2026 - Sat Jan 10, 2026
```

### Required Core Columns

| Column Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| DATE_KEY | INTEGER | YYYYMMDD format | 20250101 |
| FULL_DATE | DATE | Actual date value | 2025-01-01 |
| YEAR_NUM | INTEGER | Calendar year | 2025 |
| MONTH_NUM | INTEGER | Month (1-12) | 1 |
| DAY_OF_MONTH_NUM | INTEGER | Day of month (1-31) | 1 |
| DAY_OF_WEEK_NUM | INTEGER | 1=Sunday, 7=Saturday | 4 (for Wed) |
| WEEK_NUM | INTEGER | Week of year (1-53) | 1 |
| WEEK_START_DT | DATE | Sunday of the week | 2024-12-29 |
| WEEK_END_DT | DATE | Saturday of the week | 2025-01-04 |
| QUARTER_NUM | INTEGER | Quarter (1-4) | 1 |
| DAY_OF_YEAR_NUM | INTEGER | Day number in year (1-366) | 1 |

---

## DIM_TRADE_DATE (4-5-4 Calendar) Specification

### Core Business Rules

#### Trade Year Definition
1. **Find February 1st** of the calendar year
2. **Find the Sunday** on or before February 1st
3. **Go back 4 weeks** (28 days) - this is the start of the trade year
4. Trade year always has exactly 52 or 53 complete weeks

#### Week Definition
- **Always** complete 7-day weeks (Sunday through Saturday)
- **No partial weeks** ever
- Weeks are numbered 1-52 (or 1-53 in leap years)

#### Month Pattern (4-5-4 Repeating)
The 4-5-4 pattern repeats each quarter:

| Quarter | Month | Month Number | Weeks | Week Numbers |
|---------|-------|--------------|-------|--------------|
| Q1 | January | 1 | 4 weeks | 1-4 |
| Q1 | February | 2 | 5 weeks | 5-9 |
| Q1 | March | 3 | 4 weeks | 10-13 |
| Q2 | April | 4 | 4 weeks | 14-17 |
| Q2 | May | 5 | 5 weeks | 18-22 |
| Q2 | June | 6 | 4 weeks | 23-26 |
| Q3 | July | 7 | 4 weeks | 27-30 |
| Q3 | August | 8 | 5 weeks | 31-35 |
| Q3 | September | 9 | 4 weeks | 36-39 |
| Q4 | October | 10 | 4 weeks | 40-43 |
| Q4 | November | 11 | 5 weeks | 44-48 |
| Q4 | December | 12 | 4 weeks* | 49-52 |

*December has 5 weeks (49-53) in 53-week years

#### Leap Week Handling
- Occurs approximately every 5-6 years
- Week 53 is added to December (making it a 5-week month)
- The year needs 53 weeks when the period would otherwise be too short

### Example Trade Years

```
2024 Trade Year (53 weeks - LEAP):
- Starts: Sunday, January 28, 2024
- Ends: Saturday, February 1, 2025
- Weeks: 53 (includes leap week)

2025 Trade Year (52 weeks):
- Starts: Sunday, February 2, 2025
- Ends: Saturday, January 31, 2026
- Weeks: 52

2026 Trade Year (52 weeks):
- Starts: Sunday, February 1, 2026
- Ends: Saturday, January 30, 2027
- Weeks: 52
```

### Required Core Columns

| Column Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| DATE_KEY | INTEGER | YYYYMMDD format | 20250202 |
| CALENDAR_FULL_DT | DATE | Actual date value | 2025-02-02 |
| TRADE_YEAR_NUM | INTEGER | Trade year | 2025 |
| TRADE_MONTH_NUM | INTEGER | Trade month (1-12) | 1 |
| TRADE_WEEK_NUM | INTEGER | Week in trade year (1-53) | 1 |
| TRADE_WEEK_START_DT | DATE | Sunday of trade week | 2025-02-02 |
| TRADE_WEEK_END_DT | DATE | Saturday of trade week | 2025-02-08 |
| TRADE_DAY_OF_YEAR_NUM | INTEGER | Day in trade year (1-371) | 1 |
| TRADE_QUARTER_NUM | INTEGER | Trade quarter (1-4) | 1 |
| WEEKS_IN_TRADE_YEAR_NUM | INTEGER | 52 or 53 | 52 |

---

## SQL Validation Queries

### Calendar Date (DIM_DATE) Validations

#### Test 1: Week 1 Must Contain January 1st
```sql
-- Expected: 0 rows (no violations)
WITH week_one AS (
    SELECT
        YEAR_NUM,
        MIN(FULL_DATE) as week_start,
        MAX(FULL_DATE) as week_end
    FROM DIM_DATE
    WHERE WEEK_NUM = 1
    GROUP BY YEAR_NUM
)
SELECT
    YEAR_NUM,
    week_start,
    week_end,
    YEAR_NUM || '-01-01' as jan_1,
    'ERROR: Week 1 does not contain January 1st' as error_message
FROM week_one
WHERE DATE(YEAR_NUM || '-01-01') NOT BETWEEN week_start AND week_end;
```

#### Test 2: All Weeks Must Be Sunday-Saturday
```sql
-- Expected: 0 rows
SELECT
    DATE_KEY,
    FULL_DATE,
    WEEK_START_DT,
    WEEK_END_DT,
    DAYOFWEEK(WEEK_START_DT) as start_dow,
    DAYOFWEEK(WEEK_END_DT) as end_dow,
    'ERROR: Week does not start on Sunday or end on Saturday' as error_message
FROM DIM_DATE
WHERE DAYOFWEEK(WEEK_START_DT) != 1  -- Not Sunday
   OR DAYOFWEEK(WEEK_END_DT) != 7;    -- Not Saturday
```

#### Test 3: No Missing Dates
```sql
-- Expected: 0 rows
WITH date_gaps AS (
    SELECT
        FULL_DATE as current_date,
        LEAD(FULL_DATE) OVER (ORDER BY FULL_DATE) as next_date,
        DATEDIFF('DAY', FULL_DATE, LEAD(FULL_DATE) OVER (ORDER BY FULL_DATE)) as day_gap
    FROM DIM_DATE
)
SELECT
    current_date,
    next_date,
    day_gap,
    'ERROR: Gap in dates - missing ' || (day_gap - 1) || ' day(s)' as error_message
FROM date_gaps
WHERE day_gap > 1;
```

#### Test 4: Week Numbers Are Sequential
```sql
-- Expected: 0 rows
WITH week_sequence AS (
    SELECT
        YEAR_NUM,
        WEEK_NUM,
        LAG(WEEK_NUM) OVER (PARTITION BY YEAR_NUM ORDER BY WEEK_NUM) as prev_week
    FROM (
        SELECT DISTINCT YEAR_NUM, WEEK_NUM
        FROM DIM_DATE
    ) w
)
SELECT
    YEAR_NUM,
    prev_week,
    WEEK_NUM,
    'ERROR: Gap in week numbering' as error_message
FROM week_sequence
WHERE WEEK_NUM - prev_week > 1
  AND prev_week IS NOT NULL;
```

#### Test 5: Year Attribution Matches Calendar Year
```sql
-- Expected: 0 rows
SELECT
    FULL_DATE,
    YEAR_NUM as dim_year,
    YEAR(FULL_DATE) as actual_year,
    'ERROR: Year attribution mismatch' as error_message
FROM DIM_DATE
WHERE YEAR_NUM != YEAR(FULL_DATE);
```

### Trade Date (DIM_TRADE_DATE) Validations

#### Test 6: All Trade Weeks Have Exactly 7 Days
```sql
-- Expected: 0 rows
SELECT
    TRADE_YEAR_NUM,
    TRADE_WEEK_NUM,
    COUNT(*) as days_in_week,
    'ERROR: Trade week does not have exactly 7 days' as error_message
FROM DIM_TRADE_DATE
GROUP BY TRADE_YEAR_NUM, TRADE_WEEK_NUM
HAVING COUNT(*) != 7;
```

#### Test 7: Validate 4-5-4 Pattern
```sql
-- Expected: 0 rows (all months follow pattern)
WITH month_weeks AS (
    SELECT
        TRADE_YEAR_NUM,
        TRADE_MONTH_NUM,
        COUNT(DISTINCT TRADE_WEEK_NUM) as weeks_in_month
    FROM DIM_TRADE_DATE
    GROUP BY TRADE_YEAR_NUM, TRADE_MONTH_NUM
)
SELECT
    TRADE_YEAR_NUM,
    TRADE_MONTH_NUM,
    weeks_in_month,
    CASE
        WHEN TRADE_MONTH_NUM IN (1,3,4,6,7,9,10) THEN 4
        WHEN TRADE_MONTH_NUM IN (2,5,8,11) THEN 5
        WHEN TRADE_MONTH_NUM = 12 THEN 4  -- or 5 in leap years
    END as expected_weeks,
    'ERROR: Month does not follow 4-5-4 pattern' as error_message
FROM month_weeks
WHERE NOT (
    (TRADE_MONTH_NUM IN (1,3,4,6,7,9,10) AND weeks_in_month = 4) OR
    (TRADE_MONTH_NUM IN (2,5,8,11) AND weeks_in_month = 5) OR
    (TRADE_MONTH_NUM = 12 AND weeks_in_month IN (4,5))
);
```

#### Test 8: Trade Weeks Are Complete Sunday-Saturday
```sql
-- Expected: 0 rows
SELECT
    TRADE_WEEK_NUM,
    TRADE_YEAR_NUM,
    MIN(CALENDAR_FULL_DT) as week_start,
    MAX(CALENDAR_FULL_DT) as week_end,
    DAYOFWEEK(MIN(CALENDAR_FULL_DT)) as start_dow,
    DAYOFWEEK(MAX(CALENDAR_FULL_DT)) as end_dow,
    'ERROR: Trade week is not Sunday-Saturday' as error_message
FROM DIM_TRADE_DATE
GROUP BY TRADE_WEEK_NUM, TRADE_YEAR_NUM
HAVING DAYOFWEEK(MIN(CALENDAR_FULL_DT)) != 1  -- Not Sunday
    OR DAYOFWEEK(MAX(CALENDAR_FULL_DT)) != 7;  -- Not Saturday
```

#### Test 9: Validate Quarter Assignments
```sql
-- Expected: Exactly 13 weeks per quarter (14 in leap quarter)
SELECT
    TRADE_YEAR_NUM,
    TRADE_QUARTER_NUM,
    COUNT(DISTINCT TRADE_WEEK_NUM) as weeks_in_quarter,
    CASE
        WHEN COUNT(DISTINCT TRADE_WEEK_NUM) NOT IN (13, 14)
        THEN 'ERROR: Quarter does not have 13 or 14 weeks'
        ELSE 'OK'
    END as status
FROM DIM_TRADE_DATE
GROUP BY TRADE_YEAR_NUM, TRADE_QUARTER_NUM
HAVING COUNT(DISTINCT TRADE_WEEK_NUM) NOT IN (13, 14);
```

### Cross-Calendar Validations

#### Test 10: Same DATE_KEY in Both Tables
```sql
-- Expected: 0 rows (all dates exist in both)
SELECT
    COALESCE(d.DATE_KEY, t.DATE_KEY) as date_key,
    CASE
        WHEN d.DATE_KEY IS NULL THEN 'Missing from DIM_DATE'
        WHEN t.DATE_KEY IS NULL THEN 'Missing from DIM_TRADE_DATE'
    END as error_message
FROM DIM_DATE d
FULL OUTER JOIN DIM_TRADE_DATE t ON d.DATE_KEY = t.DATE_KEY
WHERE d.DATE_KEY IS NULL OR t.DATE_KEY IS NULL;
```

---

## Generation Approach

### Generating DIM_DATE

```sql
-- Step 1: Create date spine
WITH RECURSIVE date_spine AS (
    SELECT DATE('2020-01-01') as full_date
    UNION ALL
    SELECT full_date + INTERVAL 1 DAY
    FROM date_spine
    WHERE full_date < DATE('2030-12-31')
),

-- Step 2: Calculate week boundaries
date_with_weeks AS (
    SELECT
        full_date,
        YEAR(full_date) as year_num,
        -- Find Sunday of week containing Jan 1
        DATE_TRUNC('WEEK', DATE(YEAR(full_date) || '-01-01')) as year_week1_start
    FROM date_spine
),

-- Step 3: Calculate week numbers
date_enriched AS (
    SELECT
        full_date,
        year_num,
        -- Week number based on week containing Jan 1
        FLOOR(DATEDIFF('DAY', year_week1_start, DATE_TRUNC('WEEK', full_date)) / 7) + 1 as week_num,
        DATE_TRUNC('WEEK', full_date) as week_start_dt,
        DATE_TRUNC('WEEK', full_date) + INTERVAL 6 DAY as week_end_dt
    FROM date_with_weeks
)

SELECT
    YEAR(full_date) * 10000 + MONTH(full_date) * 100 + DAY(full_date) as DATE_KEY,
    full_date as FULL_DATE,
    year_num as YEAR_NUM,
    MONTH(full_date) as MONTH_NUM,
    DAY(full_date) as DAY_OF_MONTH_NUM,
    DAYOFWEEK(full_date) as DAY_OF_WEEK_NUM,
    week_num as WEEK_NUM,
    week_start_dt as WEEK_START_DT,
    week_end_dt as WEEK_END_DT,
    QUARTER(full_date) as QUARTER_NUM,
    DAYOFYEAR(full_date) as DAY_OF_YEAR_NUM
FROM date_enriched;
```

### Generating DIM_TRADE_DATE

```sql
-- Step 1: Define trade year boundaries
WITH trade_years AS (
    SELECT
        year_num,
        -- Sunday on or before Feb 1, minus 4 weeks
        DATE_TRUNC('WEEK', DATE(year_num || '-02-01')) - INTERVAL 28 DAY as trade_year_start
    FROM (
        SELECT 2020 + n as year_num
        FROM numbers_table
        WHERE n < 10
    ) years
),

-- Step 2: Generate all dates with trade year assignment
trade_dates AS (
    SELECT
        d.full_date,
        t.year_num as trade_year_num,
        t.trade_year_start,
        -- Calculate week number within trade year
        FLOOR(DATEDIFF('DAY', t.trade_year_start, d.full_date) / 7) + 1 as trade_week_num
    FROM date_spine d
    JOIN trade_years t
        ON d.full_date >= t.trade_year_start
        AND d.full_date < DATEADD('YEAR', 1, t.trade_year_start)
),

-- Step 3: Assign months based on 4-5-4 pattern
trade_with_months AS (
    SELECT
        *,
        CASE
            WHEN trade_week_num <= 4 THEN 1
            WHEN trade_week_num <= 9 THEN 2
            WHEN trade_week_num <= 13 THEN 3
            WHEN trade_week_num <= 17 THEN 4
            WHEN trade_week_num <= 22 THEN 5
            WHEN trade_week_num <= 26 THEN 6
            WHEN trade_week_num <= 30 THEN 7
            WHEN trade_week_num <= 35 THEN 8
            WHEN trade_week_num <= 39 THEN 9
            WHEN trade_week_num <= 43 THEN 10
            WHEN trade_week_num <= 48 THEN 11
            ELSE 12
        END as trade_month_num
    FROM trade_dates
)

SELECT * FROM trade_with_months;
```

---

## Critical Validation Checklist

### DIM_DATE Checklist
- [ ] Week 1 contains January 1st for every year
- [ ] All weeks run Sunday through Saturday
- [ ] No gaps in dates
- [ ] Week numbers are sequential (1, 2, 3... no jumps)
- [ ] Year attribution matches actual calendar year
- [ ] Day of week numbers: 1=Sunday through 7=Saturday
- [ ] Months have correct number of days
- [ ] Quarters are assigned correctly (Jan-Mar = Q1, etc.)

### DIM_TRADE_DATE Checklist
- [ ] All weeks have exactly 7 days
- [ ] All weeks run Sunday through Saturday
- [ ] 4-5-4 pattern is maintained for months
- [ ] Each quarter has 13 weeks (14 in leap quarters)
- [ ] Trade year starts on correct Sunday (4 weeks before Sunday on/before Feb 1)
- [ ] No partial weeks at year boundaries
- [ ] Week 53 only exists in designated leap years
- [ ] Trade months align with week boundaries

### Cross-Calendar Checklist
- [ ] Same DATE_KEY values exist in both tables
- [ ] Same date range covered
- [ ] Consistent metadata columns (create timestamps, etc.)

---

## Common Issues and Solutions

### Issue 1: Week 1 doesn't contain January 1st
**Solution**: Your week calculation is likely using ISO weeks or starting from first full week. Adjust to ensure Week 1 = week containing Jan 1.

### Issue 2: Missing days at year boundaries
**Solution**: Ensure your date generation includes partial weeks. Week 1 may start in previous year, Week 52/53 may end in next year.

### Issue 3: Trade calendar months don't follow 4-5-4
**Solution**: Check your week-to-month assignment logic. Weeks 1-4 = Month 1, Weeks 5-9 = Month 2, etc.

### Issue 4: Gaps in week numbers
**Solution**: Check for missing dates or incorrect week calculation logic. Every week between first and last should be present.

### Issue 5: Trade year starting on wrong date
**Solution**: Verify the formula: Find Sunday on/before Feb 1, then subtract exactly 28 days.

---

## Contact and Version Information

- **Version**: 1.0
- **Last Updated**: Current as of pattern discussion
- **Purpose**: Standard specification for calendar and trade date dimensions
- **Usage**: Reference for implementation and validation of date dimension tables
