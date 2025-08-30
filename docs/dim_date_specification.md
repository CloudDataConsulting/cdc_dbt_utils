# DIM_DATE and DIM_TRADE_DATE Complete Specification

## Table of Contents
1. [Overview](#overview)
2. [DIM_DATE (Standard Calendar) Specification](#dim_date-standard-calendar-specification)
3. [DIM_TRADE_DATE (Multi-Pattern Trade Calendar) Specification](#dim_trade_date-multi-pattern-trade-calendar-specification)
4. [Special Records Specification](#special-records-specification)
5. [SQL Validation Queries](#sql-validation-queries)
6. [Generation Approach](#generation-approach)
7. [Aggregate Table Specifications](#aggregate-table-specifications)
8. [Critical Validation Checklist](#critical-validation-checklist)

---

## Overview

### Purpose
This specification defines two separate date dimension tables for data warehouse implementation:
- **DIM_DATE**: Standard calendar following US business conventions
- **DIM_TRADE_DATE**: Trade calendar supporting three patterns (4-4-5, 4-5-4, 5-4-4)

### Date Range
**2000-01-01 to 2030-12-31** (31 years of data)

### Key Design Principles
1. **Separate Tables**: DIM_DATE and DIM_TRADE_DATE are completely separate
2. **Shared Key**: Both tables use DATE_KEY (YYYYMMDD format) for joining
3. **No Column Overlap**: Except for DATE_KEY and metadata, no shared columns
4. **Special Records**: Both tables include -1, -2, -3, -4 records for NULL handling

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

### Complete Column Specification for DIM_DATE

| Column Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| DATE_KEY | INTEGER | YYYYMMDD format | 20250101 |
| FULL_DT | DATE | Actual date value | 2025-01-01 |
| date_last_year_dt | DATE | Same date last year | 2024-01-01 |
| DATE_LAST_YEAR_KEY | INTEGER | Key for last year's date | 20240101 |
| DAY_OF_WEEK_NUM | INTEGER | 1=Sunday, 7=Saturday | 4 |
| DAY_OF_MONTH_NUM | INTEGER | Day of month (1-31) | 1 |
| DAY_OF_QUARTER_NUM | INTEGER | Day within quarter | 1 |
| DAY_OF_YEAR_NUM | INTEGER | Day number in year (1-366) | 1 |
| DAY_OVERALL_NUM | INTEGER | Cumulative from 2000-01-01 | 9132 |
| DAY_NM | VARCHAR(20) | Day name | Wednesday |
| DAY_ABBR | VARCHAR(3) | Day abbreviation | Wed |
| DAY_SUFFIX_TXT | VARCHAR(4) | Ordinal suffix | st |
| EPOCH_NUM | INTEGER | Unix timestamp | 1735689600 |
| WEEKDAY_FLG | INTEGER | 1=Mon-Fri, 0=Sat-Sun | 1 |
| LAST_DAY_OF_WEEK_FLG | INTEGER | 1 if Saturday | 0 |
| FIRST_DAY_OF_MONTH_FLG | INTEGER | 1 if first day | 1 |
| LAST_DAY_OF_MONTH_FLG | INTEGER | 1 if last day | 0 |
| LAST_DAY_OF_QUARTER_FLG | INTEGER | 1 if last day of quarter | 0 |
| LAST_DAY_OF_YEAR_FLG | INTEGER | 1 if Dec 31 | 0 |
| WEEK_NUM | INTEGER | Week of year (1-53) | 1 |
| WEEK_OF_YEAR_NUM | INTEGER | Same as WEEK_NUM | 1 |
| WEEK_OF_MONTH_NUM | INTEGER | Week within month | 1 |
| WEEK_OF_QUARTER_NUM | INTEGER | Week within quarter | 1 |
| WEEK_OVERALL_NUM | INTEGER | Cumulative from 2000-01-01 | 1305 |
| WEEK_START_DT | DATE | Sunday of the week | 2024-12-29 |
| WEEK_START_KEY | INTEGER | Key for week start | 20241229 |
| WEEK_END_DT | DATE | Saturday of the week | 2025-01-04 |
| WEEK_END_KEY | INTEGER | Key for week end | 20250104 |
| MONTH_NUM | INTEGER | Month (1-12) | 1 |
| MONTH_NM | VARCHAR(20) | Month name | January |
| MONTH_ABBR | VARCHAR(3) | Month abbreviation | Jan |
| MONTH_IN_QUARTER_NUM | INTEGER | Month within quarter (1-3) | 1 |
| MONTH_OVERALL_NUM | INTEGER | Cumulative from 2000-01-01 | 301 |
| YEARMONTH_NUM | INTEGER | YYYYMM format | 202501 |
| MONTH_START_DT | DATE | First day of month | 2025-01-01 |
| MONTH_START_KEY | INTEGER | Key for month start | 20250101 |
| MONTH_END_DT | DATE | Last day of month | 2025-01-31 |
| MONTH_END_KEY | INTEGER | Key for month end | 20250131 |
| QUARTER_NUM | INTEGER | Quarter (1-4) | 1 |
| QUARTER_NM | VARCHAR(10) | Quarter name | Q1 |
| QUARTER_START_DT | DATE | Quarter start | 2025-01-01 |
| QUARTER_START_KEY | INTEGER | Quarter start key | 20250101 |
| QUARTER_END_DT | DATE | Quarter end | 2025-03-31 |
| QUARTER_END_KEY | INTEGER | Quarter end key | 20250331 |
| YEAR_NUM | INTEGER | Calendar year | 2025 |
| YEAR_START_DT | DATE | Year start | 2025-01-01 |
| YEAR_START_KEY | INTEGER | Year start key | 20250101 |
| YEAR_END_DT | DATE | Year end | 2025-12-31 |
| YEAR_END_KEY | INTEGER | Year end key | 20251231 |
| IS_LEAP_YEAR_FLG | INTEGER | 1 if leap year | 0 |
| ISO_DAY_OF_WEEK_NUM | INTEGER | 1=Monday, 7=Sunday | 3 |
| ISO_YEAR_NUM | INTEGER | ISO year | 2025 |
| ISO_WEEK_OF_YEAR_TXT | VARCHAR(10) | ISO week | W01 |
| ISO_WEEK_OVERALL_NUM | INTEGER | Cumulative ISO week | 1305 |
| ISO_WEEK_START_DT | DATE | ISO week start (Monday) | 2024-12-30 |
| ISO_WEEK_START_KEY | INTEGER | ISO week start key | 20241230 |
| ISO_WEEK_END_DT | DATE | ISO week end (Sunday) | 2025-01-05 |
| ISO_WEEK_END_KEY | INTEGER | ISO week end key | 20250105 |
| DW_SYNCED_TS | TIMESTAMP | ETL timestamp | 2025-01-01 00:00:00 |
| DW_SOURCE_NM | VARCHAR(50) | Source system | CALENDAR |
| CREATE_USER_ID | VARCHAR(50) | ETL user | ETL_PROCESS |
| CREATE_TIMESTAMP | TIMESTAMP | Load timestamp | 2025-01-01 00:00:00 |

---

## DIM_TRADE_DATE (Multi-Pattern Trade Calendar) Specification

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

#### Multiple Pattern Support (4-4-5, 4-5-4, 5-4-4)
The table supports ALL THREE patterns simultaneously through parallel columns. Each pattern repeats each quarter but distributes the 13 weeks differently:

##### 4-4-5 Pattern
| Quarter | Month | Weeks | Week Numbers |
|---------|-------|-------|--------------|
| Q1 | January | 4 | 1-4 |
| Q1 | February | 4 | 5-8 |
| Q1 | March | 5 | 9-13 |
| Q2 | April | 4 | 14-17 |
| Q2 | May | 4 | 18-21 |
| Q2 | June | 5 | 22-26 |
| Q3 | July | 4 | 27-30 |
| Q3 | August | 4 | 31-34 |
| Q3 | September | 5 | 35-39 |
| Q4 | October | 4 | 40-43 |
| Q4 | November | 4 | 44-47 |
| Q4 | December | 5* | 48-52 |

##### 4-5-4 Pattern
| Quarter | Month | Weeks | Week Numbers |
|---------|-------|-------|--------------|
| Q1 | January | 4 | 1-4 |
| Q1 | February | 5 | 5-9 |
| Q1 | March | 4 | 10-13 |
| Q2 | April | 4 | 14-17 |
| Q2 | May | 5 | 18-22 |
| Q2 | June | 4 | 23-26 |
| Q3 | July | 4 | 27-30 |
| Q3 | August | 5 | 31-35 |
| Q3 | September | 4 | 36-39 |
| Q4 | October | 4 | 40-43 |
| Q4 | November | 5 | 44-48 |
| Q4 | December | 4* | 49-52 |

##### 5-4-4 Pattern
| Quarter | Month | Weeks | Week Numbers |
|---------|-------|-------|--------------|
| Q1 | January | 5 | 1-5 |
| Q1 | February | 4 | 6-9 |
| Q1 | March | 4 | 10-13 |
| Q2 | April | 5 | 14-18 |
| Q2 | May | 4 | 19-22 |
| Q2 | June | 4 | 23-26 |
| Q3 | July | 5 | 27-31 |
| Q3 | August | 4 | 32-35 |
| Q3 | September | 4 | 36-39 |
| Q4 | October | 5 | 40-44 |
| Q4 | November | 4 | 45-48 |
| Q4 | December | 4* | 49-52 |

*In 53-week years, the leap week (week 53) is added to December in all patterns

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

### Complete Column Specification for DIM_TRADE_DATE

| Column Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| DATE_KEY | INTEGER | YYYYMMDD format | 20250202 |
| TRADE_FULL_DT | DATE | Actual date value | 2025-02-02 |
| TRADE_DATE_LAST_YEAR_KEY | INTEGER | Key for last year | 20240204 |
| TRADE_DAY_OF_YEAR_NUM | INTEGER | Day in trade year (1-371) | 1 |
| TRADE_WEEK_NUM | INTEGER | Week in trade year (1-53) | 1 |
| TRADE_WEEK_OF_YEAR_NUM | INTEGER | Same as TRADE_WEEK_NUM | 1 |
| TRADE_WEEK_OF_MONTH_445_NUM | INTEGER | Week within month (4-4-5) | 1 |
| TRADE_WEEK_OF_MONTH_454_NUM | INTEGER | Week within month (4-5-4) | 1 |
| TRADE_WEEK_OF_MONTH_544_NUM | INTEGER | Week within month (5-4-4) | 1 |
| TRADE_WEEK_OF_QUARTER_NUM | INTEGER | Week within quarter | 1 |
| TRADE_WEEK_OVERALL_NUM | INTEGER | Cumulative from 2000 | 1287 |
| TRADE_WEEK_START_DT | DATE | Sunday of trade week | 2025-02-02 |
| TRADE_WEEK_START_KEY | INTEGER | Week start key | 20250202 |
| TRADE_WEEK_END_DT | DATE | Saturday of trade week | 2025-02-08 |
| TRADE_WEEK_END_KEY | INTEGER | Week end key | 20250208 |
| TRADE_MONTH_445_NUM | INTEGER | Month in 4-4-5 pattern | 1 |
| TRADE_MONTH_454_NUM | INTEGER | Month in 4-5-4 pattern | 1 |
| TRADE_MONTH_544_NUM | INTEGER | Month in 5-4-4 pattern | 1 |
| TRADE_MONTH_445_NM | VARCHAR(20) | Month name | January |
| TRADE_MONTH_454_NM | VARCHAR(20) | Month name | January |
| TRADE_MONTH_544_NM | VARCHAR(20) | Month name | January |
| TRADE_MONTH_ABBR | VARCHAR(3) | Month abbreviation | Jan |
| TRADE_MONTH_OVERALL_NUM | INTEGER | Cumulative from 2000 | 301 |
| TRADE_YEARMONTH_NUM | INTEGER | YYYYMM format | 202501 |
| TRADE_MONTH_445_START_DT | DATE | Month start (4-4-5) | 2025-02-02 |
| TRADE_MONTH_454_START_DT | DATE | Month start (4-5-4) | 2025-02-02 |
| TRADE_MONTH_544_START_DT | DATE | Month start (5-4-4) | 2025-02-02 |
| TRADE_MONTH_445_START_KEY | INTEGER | Month start key | 20250202 |
| TRADE_MONTH_454_START_KEY | INTEGER | Month start key | 20250202 |
| TRADE_MONTH_544_START_KEY | INTEGER | Month start key | 20250202 |
| TRADE_MONTH_445_END_DT | DATE | Month end (4-4-5) | 2025-03-01 |
| TRADE_MONTH_454_END_DT | DATE | Month end (4-5-4) | 2025-03-01 |
| TRADE_MONTH_544_END_DT | DATE | Month end (5-4-4) | 2025-03-08 |
| TRADE_MONTH_445_END_KEY | INTEGER | Month end key | 20250301 |
| TRADE_MONTH_454_END_KEY | INTEGER | Month end key | 20250301 |
| TRADE_MONTH_544_END_KEY | INTEGER | Month end key | 20250308 |
| TRADE_QUARTER_NUM | INTEGER | Trade quarter (1-4) | 1 |
| TRADE_QUARTER_NM | VARCHAR(10) | Quarter name | Q1 |
| TRADE_QUARTER_START_DT | DATE | Quarter start | 2025-02-02 |
| TRADE_QUARTER_START_KEY | INTEGER | Quarter start key | 20250202 |
| TRADE_QUARTER_END_DT | DATE | Quarter end | 2025-05-03 |
| TRADE_QUARTER_END_KEY | INTEGER | Quarter end key | 20250503 |
| TRADE_YEAR_NUM | INTEGER | Trade year | 2025 |
| TRADE_YEAR_START_DT | DATE | Trade year start | 2025-02-02 |
| TRADE_YEAR_START_KEY | INTEGER | Year start key | 20250202 |
| TRADE_YEAR_END_DT | DATE | Trade year end | 2026-01-31 |
| TRADE_YEAR_END_KEY | INTEGER | Year end key | 20260131 |
| IS_TRADE_LEAP_WEEK_FLG | INTEGER | 1 if 53-week year | 0 |
| WEEKS_IN_TRADE_YEAR_NUM | INTEGER | 52 or 53 | 52 |
| DW_SYNCED_TS | TIMESTAMP | ETL timestamp | 2025-01-01 00:00:00 |
| DW_SOURCE_NM | VARCHAR(50) | Source system | TRADE_CALENDAR |
| CREATE_USER_ID | VARCHAR(50) | ETL user | ETL_PROCESS |
| CREATE_TIMESTAMP | TIMESTAMP | Load timestamp | 2025-01-01 00:00:00 |

---

## Special Records Specification

### Purpose
Special records handle cases where fact table date values are missing, invalid, or unknown. These records ensure referential integrity while providing meaningful reporting categories.

### Standard Special Records

| DATE_KEY | Meaning | Usage |
|----------|---------|-------|
| -1 | Not Available / NULL | Default for missing dates |
| -2 | Invalid Date | Date failed validation rules |
| -3 | Not Applicable | Date doesn't apply in this context |
| -4 | Unknown | Date exists but is unknown |

### Special Record Values by Data Type

#### Numeric Fields (Integer/Number)
Use the special record key value:
- DATE_KEY: -1, -2, -3, -4
- YEAR_NUM: -1, -2, -3, -4
- MONTH_NUM: -1, -2, -3, -4
- WEEK_NUM: -1, -2, -3, -4
- DAY_OF_WEEK_NUM: -1, -2, -3, -4
- All other numeric fields: Same pattern

#### Text Fields (VARCHAR)
Use descriptive text for reporting clarity:
- DAY_NM: 'Not Available', 'Invalid', 'Not Applicable', 'Unknown'
- MONTH_NM: 'Not Available', 'Invalid', 'Not Applicable', 'Unknown'
- QUARTER_NM: 'N/A', 'Invalid', 'N/A', 'Unknown'
- All abbreviated fields: 'N/A', 'INV', 'N/A', 'UNK'

#### Date Fields (DATE)
Use Sentinel Dates:
- For -1 (Not Available): DATE '1900-01-01'
- For -2 (Invalid): DATE '1900-01-02'
- For -3 (Not Applicable): DATE '1900-01-03'
- For -4 (Unknown): DATE '1900-01-04'

#### Boolean/Flag Fields
Use 0 (zero) for all special records

### DIM_DATE Special Records
```sql
INSERT INTO DIM_DATE (
    DATE_KEY, FULL_DT, YEAR_NUM, MONTH_NUM, MONTH_NM,
    DAY_OF_MONTH_NUM, DAY_OF_WEEK_NUM, DAY_NM, WEEK_NUM,
    WEEK_START_DT, WEEK_END_DT, QUARTER_NUM, QUARTER_NM,
    DW_SOURCE_NM, CREATE_USER_ID, CREATE_TIMESTAMP
) VALUES
-- Not Available record
(-1, '1900-01-01', -1, -1, 'Not Available',
 -1, -1, 'Not Available', -1,
 '1900-01-01', '1900-01-01', -1, 'N/A',
 'SPECIAL', 'SYSTEM', CURRENT_TIMESTAMP),

-- Invalid record
(-2, '1900-01-02', -2, -2, 'Invalid',
 -2, -2, 'Invalid', -2,
 '1900-01-02', '1900-01-02', -2, 'Invalid',
 'SPECIAL', 'SYSTEM', CURRENT_TIMESTAMP),

-- Not Applicable record
(-3, '1900-01-03', -3, -3, 'Not Applicable',
 -3, -3, 'Not Applicable', -3,
 '1900-01-03', '1900-01-03', -3, 'N/A',
 'SPECIAL', 'SYSTEM', CURRENT_TIMESTAMP),

-- Unknown record
(-4, '1900-01-04', -4, -4, 'Unknown',
 -4, -4, 'Unknown', -4,
 '1900-01-04', '1900-01-04', -4, 'Unknown',
 'SPECIAL', 'SYSTEM', CURRENT_TIMESTAMP);
```

### DIM_TRADE_DATE Special Records
```sql
INSERT INTO DIM_TRADE_DATE (
    DATE_KEY, TRADE_FULL_DT, TRADE_YEAR_NUM, TRADE_WEEK_NUM,
    TRADE_MONTH_445_NUM, TRADE_MONTH_445_NM,
    TRADE_MONTH_454_NUM, TRADE_MONTH_454_NM,
    TRADE_MONTH_544_NUM, TRADE_MONTH_544_NM,
    TRADE_QUARTER_NUM, TRADE_QUARTER_NM,
    DW_SOURCE_NM, CREATE_USER_ID, CREATE_TIMESTAMP
) VALUES
-- Not Available record (all patterns)
(-1, '1900-01-01', -1, -1,
 -1, 'Not Available',
 -1, 'Not Available',
 -1, 'Not Available',
 -1, 'N/A',
 'SPECIAL', 'SYSTEM', CURRENT_TIMESTAMP),

-- Invalid record
(-2, '1900-01-02', -2, -2,
 -2, 'Invalid',
 -2, 'Invalid',
 -2, 'Invalid',
 -2, 'Invalid',
 'SPECIAL', 'SYSTEM', CURRENT_TIMESTAMP),

-- Not Applicable record
(-3, '1900-01-03', -3, -3,
 -3, 'Not Applicable',
 -3, 'Not Applicable',
 -3, 'Not Applicable',
 -3, 'N/A',
 'SPECIAL', 'SYSTEM', CURRENT_TIMESTAMP),

-- Unknown record
(-4, '1900-01-04', -4, -4,
 -4, 'Unknown',
 -4, 'Unknown',
 -4, 'Unknown',
 -4, 'Unknown',
 'SPECIAL', 'SYSTEM', CURRENT_TIMESTAMP);
```

---

## SQL Validation Queries

### DIM_DATE Validations

#### Test 1: Week 1 Must Contain January 1st
```sql
-- Expected: 0 rows (no violations)
WITH week_one AS (
    SELECT
        YEAR_NUM,
        MIN(FULL_DT) as week_start,
        MAX(FULL_DT) as week_end
    FROM DIM_DATE
    WHERE WEEK_NUM = 1
      AND DATE_KEY > 0  -- Exclude special records
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
    FULL_DT,
    WEEK_START_DT,
    WEEK_END_DT,
    DAYOFWEEK(WEEK_START_DT) as start_dow,
    DAYOFWEEK(WEEK_END_DT) as end_dow,
    'ERROR: Week does not start on Sunday or end on Saturday' as error_message
FROM DIM_DATE
WHERE DATE_KEY > 0  -- Exclude special records
  AND (DAYOFWEEK(WEEK_START_DT) != 1  -- Not Sunday
   OR DAYOFWEEK(WEEK_END_DT) != 7);    -- Not Saturday
```

#### Test 3: No Missing Dates
```sql
-- Expected: 0 rows
WITH date_gaps AS (
    SELECT
        FULL_DT as current_date,
        LEAD(FULL_DT) OVER (ORDER BY FULL_DT) as next_date,
        DATEDIFF('DAY', FULL_DT, LEAD(FULL_DT) OVER (ORDER BY FULL_DT)) as day_gap
    FROM DIM_DATE
    WHERE DATE_KEY > 0  -- Exclude special records
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
        WHERE DATE_KEY > 0  -- Exclude special records
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
    FULL_DT,
    YEAR_NUM as dim_year,
    YEAR(FULL_DT) as actual_year,
    'ERROR: Year attribution mismatch' as error_message
FROM DIM_DATE
WHERE DATE_KEY > 0  -- Exclude special records
  AND YEAR_NUM != YEAR(FULL_DT);
```

#### Test 6: Special Records Exist
```sql
-- Expected: 0 rows
SELECT COUNT(*) as missing_special_records
FROM (SELECT -1 as key UNION SELECT -2 UNION SELECT -3 UNION SELECT -4) expected
LEFT JOIN DIM_DATE d ON expected.key = d.DATE_KEY
WHERE d.DATE_KEY IS NULL;
```

### DIM_TRADE_DATE Validations

#### Test 1: All Trade Weeks Have Exactly 7 Days
```sql
-- Expected: 0 rows
SELECT
    TRADE_YEAR_NUM,
    TRADE_WEEK_NUM,
    COUNT(*) as days_in_week,
    'ERROR: Trade week does not have exactly 7 days' as error_message
FROM DIM_TRADE_DATE
WHERE DATE_KEY > 0  -- Exclude special records
GROUP BY TRADE_YEAR_NUM, TRADE_WEEK_NUM
HAVING COUNT(*) != 7;
```

#### Test 2: Validate 4-4-5 Pattern
```sql
-- Expected: 0 rows
WITH month_weeks_445 AS (
    SELECT
        TRADE_YEAR_NUM,
        TRADE_MONTH_445_NUM,
        COUNT(DISTINCT TRADE_WEEK_NUM) as weeks_in_month
    FROM DIM_TRADE_DATE
    WHERE DATE_KEY > 0  -- Exclude special records
    GROUP BY TRADE_YEAR_NUM, TRADE_MONTH_445_NUM
)
SELECT
    TRADE_YEAR_NUM,
    TRADE_MONTH_445_NUM as month_num,
    weeks_in_month,
    '4-4-5' as pattern,
    'ERROR: Month does not follow 4-4-5 pattern' as error_message
FROM month_weeks_445
WHERE NOT (
    (TRADE_MONTH_445_NUM IN (1,2,4,5,7,8,10,11) AND weeks_in_month = 4) OR
    (TRADE_MONTH_445_NUM IN (3,6,9) AND weeks_in_month = 5) OR
    (TRADE_MONTH_445_NUM = 12 AND weeks_in_month IN (5,6))  -- 6 in leap years
);
```

#### Test 3: Validate 4-5-4 Pattern
```sql
-- Expected: 0 rows
WITH month_weeks_454 AS (
    SELECT
        TRADE_YEAR_NUM,
        TRADE_MONTH_454_NUM,
        COUNT(DISTINCT TRADE_WEEK_NUM) as weeks_in_month
    FROM DIM_TRADE_DATE
    WHERE DATE_KEY > 0  -- Exclude special records
    GROUP BY TRADE_YEAR_NUM, TRADE_MONTH_454_NUM
)
SELECT
    TRADE_YEAR_NUM,
    TRADE_MONTH_454_NUM as month_num,
    weeks_in_month,
    '4-5-4' as pattern,
    'ERROR: Month does not follow 4-5-4 pattern' as error_message
FROM month_weeks_454
WHERE NOT (
    (TRADE_MONTH_454_NUM IN (1,3,4,6,7,9,10) AND weeks_in_month = 4) OR
    (TRADE_MONTH_454_NUM IN (2,5,8,11) AND weeks_in_month = 5) OR
    (TRADE_MONTH_454_NUM = 12 AND weeks_in_month IN (4,5))  -- 5 in leap years
);
```

#### Test 4: Validate 5-4-4 Pattern
```sql
-- Expected: 0 rows
WITH month_weeks_544 AS (
    SELECT
        TRADE_YEAR_NUM,
        TRADE_MONTH_544_NUM,
        COUNT(DISTINCT TRADE_WEEK_NUM) as weeks_in_month
    FROM DIM_TRADE_DATE
    WHERE DATE_KEY > 0  -- Exclude special records
    GROUP BY TRADE_YEAR_NUM, TRADE_MONTH_544_NUM
)
SELECT
    TRADE_YEAR_NUM,
    TRADE_MONTH_544_NUM as month_num,
    weeks_in_month,
    '5-4-4' as pattern,
    'ERROR: Month does not follow 5-4-4 pattern' as error_message
FROM month_weeks_544
WHERE NOT (
    (TRADE_MONTH_544_NUM IN (1,4,7,10) AND weeks_in_month = 5) OR
    (TRADE_MONTH_544_NUM IN (2,3,5,6,8,9,11,12) AND weeks_in_month = 4) OR
    (TRADE_MONTH_544_NUM = 12 AND weeks_in_month = 5)  -- 5 in leap years with week 53
);
```

#### Test 5: Trade Weeks Are Complete Sunday-Saturday
```sql
-- Expected: 0 rows
SELECT
    TRADE_WEEK_NUM,
    TRADE_YEAR_NUM,
    MIN(TRADE_FULL_DT) as week_start,
    MAX(TRADE_FULL_DT) as week_end,
    DAYOFWEEK(MIN(TRADE_FULL_DT)) as start_dow,
    DAYOFWEEK(MAX(TRADE_FULL_DT)) as end_dow,
    'ERROR: Trade week is not Sunday-Saturday' as error_message
FROM DIM_TRADE_DATE
WHERE DATE_KEY > 0  -- Exclude special records
GROUP BY TRADE_WEEK_NUM, TRADE_YEAR_NUM
HAVING DAYOFWEEK(MIN(TRADE_FULL_DT)) != 1  -- Not Sunday
    OR DAYOFWEEK(MAX(TRADE_FULL_DT)) != 7;  -- Not Saturday
```

#### Test 6: Validate Quarter Assignments
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
WHERE DATE_KEY > 0  -- Exclude special records
GROUP BY TRADE_YEAR_NUM, TRADE_QUARTER_NUM
HAVING COUNT(DISTINCT TRADE_WEEK_NUM) NOT IN (13, 14);
```

#### Test 7: Special Records Exist
```sql
-- Expected: 0 rows
SELECT COUNT(*) as missing_special_records
FROM (SELECT -1 as key UNION SELECT -2 UNION SELECT -3 UNION SELECT -4) expected
LEFT JOIN DIM_TRADE_DATE d ON expected.key = d.DATE_KEY
WHERE d.DATE_KEY IS NULL;
```

### Cross-Calendar Validations

#### Test 1: Same DATE_KEY in Both Tables
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
WHERE (d.DATE_KEY IS NULL OR t.DATE_KEY IS NULL)
  AND COALESCE(d.DATE_KEY, t.DATE_KEY) > 0;  -- Exclude special records
```

#### Test 2: Same Date Range
```sql
-- Expected: 0 rows
SELECT CASE
    WHEN d.min_date = t.min_date AND d.max_date = t.max_date
    THEN 0 ELSE 1 END as date_range_mismatch
FROM (
    SELECT MIN(FULL_DT) as min_date, MAX(FULL_DT) as max_date
    FROM DIM_DATE WHERE DATE_KEY > 0
) d
CROSS JOIN (
    SELECT MIN(TRADE_FULL_DT) as min_date, MAX(TRADE_FULL_DT) as max_date
    FROM DIM_TRADE_DATE WHERE DATE_KEY > 0
) t;
```

---

## Generation Approach

### Generating DIM_DATE

```sql
-- Step 1: Create date spine
WITH RECURSIVE date_spine AS (
    SELECT DATE('2000-01-01') as full_date
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
    full_date as FULL_DT,
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

### Generating DIM_TRADE_DATE with Multi-Pattern Support

```sql
-- Step 1: Define trade year boundaries
WITH trade_years AS (
    SELECT
        year_num,
        -- Sunday on or before Feb 1, minus 4 weeks
        DATE_TRUNC('WEEK', DATE(year_num || '-02-01')) - INTERVAL 28 DAY as trade_year_start
    FROM (
        SELECT 2000 + n as year_num
        FROM numbers_table
        WHERE n < 31
    ) years
),

-- Step 2: Generate all dates with trade year and week assignment
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

-- Step 3: Assign months for ALL THREE patterns
trade_with_patterns AS (
    SELECT
        *,
        -- 4-4-5 Pattern
        CASE
            WHEN trade_week_num <= 4 THEN 1   -- Jan: weeks 1-4
            WHEN trade_week_num <= 8 THEN 2   -- Feb: weeks 5-8
            WHEN trade_week_num <= 13 THEN 3  -- Mar: weeks 9-13
            WHEN trade_week_num <= 17 THEN 4  -- Apr: weeks 14-17
            WHEN trade_week_num <= 21 THEN 5  -- May: weeks 18-21
            WHEN trade_week_num <= 26 THEN 6  -- Jun: weeks 22-26
            WHEN trade_week_num <= 30 THEN 7  -- Jul: weeks 27-30
            WHEN trade_week_num <= 34 THEN 8  -- Aug: weeks 31-34
            WHEN trade_week_num <= 39 THEN 9  -- Sep: weeks 35-39
            WHEN trade_week_num <= 43 THEN 10 -- Oct: weeks 40-43
            WHEN trade_week_num <= 47 THEN 11 -- Nov: weeks 44-47
            ELSE 12                            -- Dec: weeks 48-52/53
        END as trade_month_445_num,

        -- 4-5-4 Pattern
        CASE
            WHEN trade_week_num <= 4 THEN 1   -- Jan: weeks 1-4
            WHEN trade_week_num <= 9 THEN 2   -- Feb: weeks 5-9
            WHEN trade_week_num <= 13 THEN 3  -- Mar: weeks 10-13
            WHEN trade_week_num <= 17 THEN 4  -- Apr: weeks 14-17
            WHEN trade_week_num <= 22 THEN 5  -- May: weeks 18-22
            WHEN trade_week_num <= 26 THEN 6  -- Jun: weeks 23-26
            WHEN trade_week_num <= 30 THEN 7  -- Jul: weeks 27-30
            WHEN trade_week_num <= 35 THEN 8  -- Aug: weeks 31-35
            WHEN trade_week_num <= 39 THEN 9  -- Sep: weeks 36-39
            WHEN trade_week_num <= 43 THEN 10 -- Oct: weeks 40-43
            WHEN trade_week_num <= 48 THEN 11 -- Nov: weeks 44-48
            ELSE 12                            -- Dec: weeks 49-52/53
        END as trade_month_454_num,

        -- 5-4-4 Pattern
        CASE
            WHEN trade_week_num <= 5 THEN 1   -- Jan: weeks 1-5
            WHEN trade_week_num <= 9 THEN 2   -- Feb: weeks 6-9
            WHEN trade_week_num <= 13 THEN 3  -- Mar: weeks 10-13
            WHEN trade_week_num <= 18 THEN 4  -- Apr: weeks 14-18
            WHEN trade_week_num <= 22 THEN 5  -- May: weeks 19-22
            WHEN trade_week_num <= 26 THEN 6  -- Jun: weeks 23-26
            WHEN trade_week_num <= 31 THEN 7  -- Jul: weeks 27-31
            WHEN trade_week_num <= 35 THEN 8  -- Aug: weeks 32-35
            WHEN trade_week_num <= 39 THEN 9  -- Sep: weeks 36-39
            WHEN trade_week_num <= 44 THEN 10 -- Oct: weeks 40-44
            WHEN trade_week_num <= 48 THEN 11 -- Nov: weeks 45-48
            ELSE 12                            -- Dec: weeks 49-52/53
        END as trade_month_544_num
    FROM trade_dates
)

SELECT * FROM trade_with_patterns;
```

---

## Aggregate Table Specifications

### DIM_WEEK (from DIM_DATE)
```sql
CREATE TABLE DIM_WEEK AS
SELECT
    MIN(DATE_KEY) as WEEK_KEY,
    MIN(FULL_DT) as WEEK_START_DT,
    MAX(FULL_DT) as WEEK_END_DT,
    MIN(DATE_KEY) as WEEK_START_KEY,
    MAX(DATE_KEY) as WEEK_END_KEY,
    MAX(YEAR_NUM) as YEAR_NUM,
    MAX(WEEK_NUM) as WEEK_NUM,
    MAX(WEEK_OF_YEAR_NUM) as WEEK_OF_YEAR_NUM,
    COUNT(*) as DAYS_IN_WEEK_NUM,
    CURRENT_TIMESTAMP as CREATE_TIMESTAMP
FROM DIM_DATE
WHERE DATE_KEY > 0
GROUP BY YEAR_NUM, WEEK_NUM;
```

### DIM_MONTH (from DIM_DATE)
```sql
CREATE TABLE DIM_MONTH AS
SELECT
    MIN(DATE_KEY) as MONTH_KEY,
    MAX(YEAR_NUM) as YEAR_NUM,
    MAX(MONTH_NUM) as MONTH_NUM,
    MAX(MONTH_NM) as MONTH_NM,
    MIN(FULL_DT) as MONTH_START_DT,
    MAX(FULL_DT) as MONTH_END_DT,
    MIN(DATE_KEY) as MONTH_START_KEY,
    MAX(DATE_KEY) as MONTH_END_KEY,
    COUNT(*) as DAYS_IN_MONTH_NUM,
    COUNT(DISTINCT WEEK_NUM) as WEEKS_IN_MONTH_NUM,
    CURRENT_TIMESTAMP as CREATE_TIMESTAMP
FROM DIM_DATE
WHERE DATE_KEY > 0
GROUP BY YEAR_NUM, MONTH_NUM;
```

### DIM_QUARTER (from DIM_DATE)
```sql
CREATE TABLE DIM_QUARTER AS
SELECT
    YEAR_NUM * 10 + QUARTER_NUM as QUARTER_KEY,
    MAX(YEAR_NUM) as YEAR_NUM,
    MAX(QUARTER_NUM) as QUARTER_NUM,
    MAX(QUARTER_NM) as QUARTER_NM,
    MIN(FULL_DT) as QUARTER_START_DT,
    MAX(FULL_DT) as QUARTER_END_DT,
    MIN(DATE_KEY) as QUARTER_START_KEY,
    MAX(DATE_KEY) as QUARTER_END_KEY,
    COUNT(*) as DAYS_IN_QUARTER_NUM,
    COUNT(DISTINCT MONTH_NUM) as MONTHS_IN_QUARTER_NUM,
    COUNT(DISTINCT WEEK_NUM) as WEEKS_IN_QUARTER_NUM,
    CURRENT_TIMESTAMP as CREATE_TIMESTAMP
FROM DIM_DATE
WHERE DATE_KEY > 0
GROUP BY YEAR_NUM, QUARTER_NUM;
```

### DIM_YEAR (from DIM_DATE)
```sql
CREATE TABLE DIM_YEAR AS
SELECT
    YEAR_NUM as YEAR_KEY,
    YEAR_NUM,
    MIN(FULL_DT) as YEAR_START_DT,
    MAX(FULL_DT) as YEAR_END_DT,
    MIN(DATE_KEY) as YEAR_START_KEY,
    MAX(DATE_KEY) as YEAR_END_KEY,
    COUNT(*) as DAYS_IN_YEAR_NUM,
    COUNT(DISTINCT WEEK_NUM) as WEEKS_IN_YEAR_NUM,
    MAX(IS_LEAP_YEAR_FLG) as IS_LEAP_YEAR_FLG,
    CURRENT_TIMESTAMP as CREATE_TIMESTAMP
FROM DIM_DATE
WHERE DATE_KEY > 0
GROUP BY YEAR_NUM;
```

### DIM_TRADE_WEEK (from DIM_TRADE_DATE)
```sql
CREATE TABLE DIM_TRADE_WEEK AS
SELECT
    MIN(DATE_KEY) as TRADE_WEEK_KEY,
    MAX(TRADE_YEAR_NUM) as TRADE_YEAR_NUM,
    MAX(TRADE_WEEK_NUM) as TRADE_WEEK_NUM,
    MAX(TRADE_WEEK_OF_YEAR_NUM) as TRADE_WEEK_OF_YEAR_NUM,
    MAX(TRADE_WEEK_OF_QUARTER_NUM) as TRADE_WEEK_OF_QUARTER_NUM,
    MIN(TRADE_FULL_DT) as TRADE_WEEK_START_DT,
    MAX(TRADE_FULL_DT) as TRADE_WEEK_END_DT,
    MIN(DATE_KEY) as TRADE_WEEK_START_KEY,
    MAX(DATE_KEY) as TRADE_WEEK_END_KEY,
    MAX(TRADE_MONTH_445_NUM) as TRADE_MONTH_445_NUM,
    MAX(TRADE_MONTH_454_NUM) as TRADE_MONTH_454_NUM,
    MAX(TRADE_MONTH_544_NUM) as TRADE_MONTH_544_NUM,
    MAX(TRADE_WEEK_OF_MONTH_445_NUM) as TRADE_WEEK_OF_MONTH_445_NUM,
    MAX(TRADE_WEEK_OF_MONTH_454_NUM) as TRADE_WEEK_OF_MONTH_454_NUM,
    MAX(TRADE_WEEK_OF_MONTH_544_NUM) as TRADE_WEEK_OF_MONTH_544_NUM,
    MAX(TRADE_QUARTER_NUM) as TRADE_QUARTER_NUM,
    COUNT(*) as DAYS_IN_WEEK_NUM,
    MAX(WEEKS_IN_TRADE_YEAR_NUM) as WEEKS_IN_TRADE_YEAR_NUM,
    MAX(IS_TRADE_LEAP_WEEK_FLG) as IS_TRADE_LEAP_WEEK_FLG,
    CURRENT_TIMESTAMP as CREATE_TIMESTAMP
FROM DIM_TRADE_DATE
WHERE DATE_KEY > 0
GROUP BY TRADE_YEAR_NUM, TRADE_WEEK_NUM;
```

### DIM_TRADE_MONTH (from DIM_TRADE_WEEK)
```sql
CREATE TABLE DIM_TRADE_MONTH AS
SELECT
    TRADE_YEAR_NUM * 100 + MIN(TRADE_MONTH_445_NUM) as TRADE_MONTH_KEY,
    TRADE_YEAR_NUM,
    MIN(TRADE_MONTH_445_NUM) as TRADE_MONTH_NUM,
    MAX(TRADE_QUARTER_NUM) as TRADE_QUARTER_NUM,
    MIN(TRADE_WEEK_START_DT) as TRADE_MONTH_START_DT,
    MAX(TRADE_WEEK_END_DT) as TRADE_MONTH_END_DT,
    MIN(TRADE_WEEK_START_KEY) as TRADE_MONTH_START_KEY,
    MAX(TRADE_WEEK_END_KEY) as TRADE_MONTH_END_KEY,
    COUNT(DISTINCT TRADE_WEEK_NUM) as WEEKS_IN_MONTH_NUM,
    SUM(DAYS_IN_WEEK_NUM) as DAYS_IN_MONTH_NUM,
    MAX(CASE WHEN TRADE_WEEK_OF_MONTH_445_NUM = 5 THEN 1 ELSE 0 END) as IS_5_WEEK_MONTH_445_FLG,
    MAX(CASE WHEN TRADE_WEEK_OF_MONTH_454_NUM = 5 THEN 1 ELSE 0 END) as IS_5_WEEK_MONTH_454_FLG,
    MAX(CASE WHEN TRADE_WEEK_OF_MONTH_544_NUM = 5 THEN 1 ELSE 0 END) as IS_5_WEEK_MONTH_544_FLG,
    MAX(IS_TRADE_LEAP_WEEK_FLG) as CONTAINS_LEAP_WEEK_FLG,
    CURRENT_TIMESTAMP as CREATE_TIMESTAMP
FROM DIM_TRADE_WEEK
GROUP BY TRADE_YEAR_NUM, TRADE_QUARTER_NUM;
```

### DIM_TRADE_QUARTER (from DIM_TRADE_MONTH)
```sql
CREATE TABLE DIM_TRADE_QUARTER AS
SELECT
    TRADE_YEAR_NUM * 10 + TRADE_QUARTER_NUM as TRADE_QUARTER_KEY,
    TRADE_YEAR_NUM,
    TRADE_QUARTER_NUM,
    MIN(TRADE_MONTH_START_DT) as TRADE_QUARTER_START_DT,
    MAX(TRADE_MONTH_END_DT) as TRADE_QUARTER_END_DT,
    MIN(TRADE_MONTH_START_KEY) as TRADE_QUARTER_START_KEY,
    MAX(TRADE_MONTH_END_KEY) as TRADE_QUARTER_END_KEY,
    SUM(WEEKS_IN_MONTH_NUM) as WEEKS_IN_QUARTER_NUM,
    SUM(DAYS_IN_MONTH_NUM) as DAYS_IN_QUARTER_NUM,
    MAX(CONTAINS_LEAP_WEEK_FLG) as CONTAINS_LEAP_WEEK_FLG,
    CURRENT_TIMESTAMP as CREATE_TIMESTAMP
FROM DIM_TRADE_MONTH
GROUP BY TRADE_YEAR_NUM, TRADE_QUARTER_NUM;
```

### DIM_TRADE_YEAR (from DIM_TRADE_QUARTER)
```sql
CREATE TABLE DIM_TRADE_YEAR AS
SELECT
    TRADE_YEAR_NUM as TRADE_YEAR_KEY,
    TRADE_YEAR_NUM,
    MIN(TRADE_QUARTER_START_DT) as TRADE_YEAR_START_DT,
    MAX(TRADE_QUARTER_END_DT) as TRADE_YEAR_END_DT,
    MIN(TRADE_QUARTER_START_KEY) as TRADE_YEAR_START_KEY,
    MAX(TRADE_QUARTER_END_KEY) as TRADE_YEAR_END_KEY,
    SUM(WEEKS_IN_QUARTER_NUM) as WEEKS_IN_YEAR_NUM,
    SUM(DAYS_IN_QUARTER_NUM) as DAYS_IN_YEAR_NUM,
    MAX(CONTAINS_LEAP_WEEK_FLG) as IS_LEAP_WEEK_YEAR_FLG,
    CURRENT_TIMESTAMP as CREATE_TIMESTAMP
FROM DIM_TRADE_QUARTER
GROUP BY TRADE_YEAR_NUM;
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
- [ ] Special records (-1, -2, -3, -4) exist

### DIM_TRADE_DATE Checklist
- [ ] All weeks have exactly 7 days
- [ ] All weeks run Sunday through Saturday
- [ ] 4-4-5 pattern is maintained (4,4,5,4,4,5,4,4,5,4,4,5 weeks per month)
- [ ] 4-5-4 pattern is maintained (4,5,4,4,5,4,4,5,4,4,5,4 weeks per month)
- [ ] 5-4-4 pattern is maintained (5,4,4,5,4,4,5,4,4,5,4,4 weeks per month)
- [ ] Each quarter has 13 weeks (14 in leap quarters) across all patterns
- [ ] Trade year starts on correct Sunday (4 weeks before Sunday on/before Feb 1)
- [ ] No partial weeks at year boundaries
- [ ] Week 53 only exists in designated leap years
- [ ] Trade months align with week boundaries for each pattern
- [ ] Pattern-specific columns have consistent values within each week
- [ ] Special records (-1, -2, -3, -4) exist

### Cross-Calendar Checklist
- [ ] Same DATE_KEY values exist in both tables (excluding special records)
- [ ] Same date range covered (2000-01-01 to 2030-12-31)
- [ ] Consistent metadata columns (create timestamps, etc.)

### Aggregate Tables Checklist
- [ ] DIM_WEEK has one row per calendar week
- [ ] DIM_MONTH has one row per calendar month
- [ ] DIM_QUARTER has one row per calendar quarter
- [ ] DIM_YEAR has one row per calendar year
- [ ] DIM_TRADE_WEEK has one row per trade week
- [ ] DIM_TRADE_MONTH has one row per trade month
- [ ] DIM_TRADE_QUARTER has one row per trade quarter
- [ ] DIM_TRADE_YEAR has one row per trade year
- [ ] Row counts reconcile between base and aggregate tables

---

## Common Issues and Solutions

### Issue 1: Week 1 doesn't contain January 1st
**Solution**: Your week calculation is likely using ISO weeks or starting from first full week. Adjust to ensure Week 1 = week containing Jan 1.

### Issue 2: Missing days at year boundaries
**Solution**: Ensure your date generation includes partial weeks. Week 1 may start in previous year, Week 52/53 may end in next year.

### Issue 3: Trade calendar months don't follow patterns
**Solution**: Check your week-to-month assignment logic. Each pattern has specific week ranges for each month.

### Issue 4: Gaps in week numbers
**Solution**: Check for missing dates or incorrect week calculation logic. Every week between first and last should be present.

### Issue 5: Trade year starting on wrong date
**Solution**: Verify the formula: Find Sunday on/before Feb 1, then subtract exactly 28 days.

---

## Version Information

- **Version**: 2.0
- **Last Updated**: Current
- **Date Range**: 2000-01-01 to 2030-12-31
- **Purpose**: Complete specification for calendar and trade date dimensions
- **Key Features**: Separate tables, multi-pattern trade calendar support, comprehensive validation
