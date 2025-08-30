# Complete Prompt for Claude Code: Date Dimension Implementation

## Project Overview
Implement a comprehensive date dimension system with both standard calendar (DIM_DATE) and multi-pattern trade calendar (DIM_TRADE_DATE) tables, including all validation, testing, and aggregate tables. This is a critical data warehouse component requiring precise implementation of business rules and extensive validation.

## Key Requirements
1. **Parallel Execution**: DIM_DATE and DIM_TRADE_DATE should be developed in parallel by separate agents
2. **Strict Naming Standards**: Follow the exact column naming conventions already established (see Column Naming Standards section)
3. **Sequential Dependencies**: Base tables MUST be complete and validated before starting aggregates
4. **Comprehensive Testing**: Every business rule must have a corresponding validation query
5. **Special Records**: Include -1, -2, -3, -4 records for handling NULL/invalid dates

## Orchestration Strategy

### Phase 1: Base Table Implementation (Parallel)

**Agent 1: DIM_DATE Implementation**
- Generate date spine from 2020-01-01 to 2030-12-31
- Implement US calendar week logic (Week 1 contains January 1)
- Add special records (-1, -2, -3, -4)
- Create comprehensive test suite
- Document all issues found

**Agent 2: DIM_TRADE_DATE Implementation**
- Generate trade calendar from 2020-2030
- Implement ALL THREE patterns (4-4-5, 4-5-4, 5-4-4) in parallel columns
- Add special records (-1, -2, -3, -4)
- Create comprehensive test suite
- Document all issues found

### Phase 2: Validation Gate (Sequential)
**Orchestration Agent must verify:**
- All Test Suite 1 queries return 0 rows (no errors)
- All Test Suite 2 queries return 0 rows (no errors)
- DATE_KEY values match between both tables (excluding special records)
- Special records exist and are valid

### Phase 3: Aggregate Implementation (Parallel after Phase 2)

**Agent 3: Calendar Aggregates**
- DIM_WEEK (from DIM_DATE)
- DIM_MONTH (from DIM_DATE)
- DIM_QUARTER (from DIM_DATE)
- DIM_YEAR (from DIM_DATE)

**Agent 4: Trade Calendar Aggregates**
- DIM_TRADE_WEEK (from DIM_TRADE_DATE)
- DIM_TRADE_MONTH (from DIM_TRADE_DATE)
- DIM_TRADE_QUARTER (from DIM_TRADE_DATE)
- DIM_TRADE_YEAR (from DIM_TRADE_DATE)

## Column Naming Standards

### DIM_DATE Required Columns
```sql
DATE_KEY                    -- INTEGER, YYYYMMDD format
FULL_DATE                   -- DATE
SAME_DT_LAST_YEAR          -- DATE
DATE_LAST_YEAR_KEY         -- INTEGER
DAY_OF_WEEK_NUM            -- INTEGER, 1=Sunday, 7=Saturday
DAY_OF_MONTH_NUM           -- INTEGER
DAY_OF_QUARTER_NUM         -- INTEGER
DAY_OF_YEAR_NUM            -- INTEGER
DAY_NM                     -- VARCHAR
DAY_ABBR                   -- VARCHAR
DAY_SUFFIX_TXT             -- VARCHAR (1st, 2nd, 3rd, etc.)
DAY_OVERALL_NUM            -- INTEGER, cumulative from start
WEEKDAY_FLG                -- INTEGER, 1=Mon-Fri, 0=Sat-Sun
WEEK_NUM                   -- INTEGER, Week of year
WEEK_OF_MONTH_NUM          -- INTEGER
WEEK_OF_YEAR_NUM           -- INTEGER
WEEK_OVERALL_NUM           -- INTEGER, cumulative from start
WEEK_BEGIN_DT              -- DATE, Sunday
WEEK_BEGIN_KEY             -- INTEGER
WEEK_END_DT                -- DATE, Saturday
WEEK_END_KEY               -- INTEGER
MONTH_NUM                  -- INTEGER, 1-12
MONTH_NM                   -- VARCHAR
MONTH_ABBR                 -- VARCHAR
MONTH_IN_QUARTER_NUM       -- INTEGER, 1-3
MONTH_OVERALL_NUM          -- INTEGER, cumulative from start
FIRST_DAY_OF_MONTH_DT      -- DATE
FIRST_DAY_OF_MONTH_KEY     -- INTEGER
LAST_DAY_OF_MONTH_DT       -- DATE
LAST_DAY_OF_MONTH_KEY      -- INTEGER
FIRST_DAY_OF_MONTH_FLG     -- INTEGER
END_OF_MONTH_FLG           -- INTEGER
QUARTER_NUM                -- INTEGER, 1-4
QUARTER_NM                 -- VARCHAR
FIRST_DAY_OF_QUARTER_DT    -- DATE
LAST_DAY_OF_QUARTER_DT     -- DATE
YEAR_NUM                   -- INTEGER
YEARMONTH_NUM              -- INTEGER, YYYYMM
FIRST_DAY_OF_YEAR_DT       -- DATE
LAST_DAY_OF_YEAR_DT        -- DATE
END_OF_YEAR_FLG            -- INTEGER
END_OF_WEEK_FLG            -- INTEGER
IS_LEAP_YEAR_FLG           -- INTEGER
ISO_DAY_OF_WEEK_NUM        -- INTEGER, 1=Monday, 7=Sunday
ISO_YEAR_NUM               -- INTEGER
ISO_WEEK_OF_YEAR_TXT       -- VARCHAR
ISO_WEEK_OVERALL_NUM       -- INTEGER
ISO_WEEK_BEGIN_DT          -- DATE
ISO_WEEK_BEGIN_KEY         -- INTEGER
ISO_WEEK_END_DT            -- DATE
ISO_WEEK_END_KEY           -- INTEGER
EPOCH                      -- INTEGER
YYYYMMDD                   -- INTEGER (same as DATE_KEY)
CREATE_USER_ID             -- VARCHAR
CREATE_TS                  -- TIMESTAMP
```

### DIM_TRADE_DATE Required Columns
```sql
-- Primary Key
DATE_KEY                        -- INTEGER, YYYYMMDD format

-- DAY Level
CALENDAR_FULL_DT                -- DATE (same value for calendar and trade)
TRADE_FULL_DT                   -- DATE (same as CALENDAR_FULL_DT)
CALENDAR_SAME_DT_LAST_YEAR      -- DATE
CALENDAR_DATE_LAST_YEAR_KEY     -- INTEGER
TRADE_DATE_LAST_YEAR_KEY        -- INTEGER
CALENDAR_DAY_OF_WEEK_NUM        -- INTEGER
ISO_DAY_OF_WEEK_NUM             -- INTEGER
CALENDAR_DAY_OF_MONTH_NUM       -- INTEGER
CALENDAR_DAY_OF_QUARTER_NUM     -- INTEGER
CALENDAR_DAY_OF_YEAR_NUM        -- INTEGER
TRADE_DAY_OF_YEAR_NUM           -- INTEGER
CALENDAR_DAY_OVERALL_NUM        -- INTEGER
CALENDAR_DAY_NM                 -- VARCHAR
CALENDAR_DAY_ABBR               -- VARCHAR
CALENDAR_DAY_SUFFIX_TXT         -- VARCHAR
CALENDAR_EPOCH_NUM              -- INTEGER
CALENDAR_WEEKDAY_FLG            -- INTEGER
CALENDAR_LAST_DAY_OF_WEEK_FLG   -- INTEGER
CALENDAR_FIRST_DAY_OF_MONTH_FLG -- INTEGER
CALENDAR_LAST_DAY_OF_MONTH_FLG  -- INTEGER
CALENDAR_LAST_DAY_OF_QUARTER_FLG -- INTEGER
CALENDAR_LAST_DAY_OF_YEAR_FLG   -- INTEGER

-- WEEK Level
CALENDAR_WEEK_NUM               -- INTEGER
TRADE_WEEK_NUM                  -- INTEGER
CALENDAR_WEEK_OF_YEAR_NUM       -- INTEGER
TRADE_WEEK_OF_YEAR_NUM          -- INTEGER
CALENDAR_WEEK_OF_MONTH_NUM      -- INTEGER
TRADE_WEEK_OF_MONTH_445_NUM     -- INTEGER
TRADE_WEEK_OF_MONTH_454_NUM     -- INTEGER
TRADE_WEEK_OF_MONTH_544_NUM     -- INTEGER
CALENDAR_WEEK_OF_QUARTER_NUM    -- INTEGER
TRADE_WEEK_OF_QUARTER_NUM       -- INTEGER
CALENDAR_WEEK_OVERALL_NUM       -- INTEGER
TRADE_WEEK_OVERALL_NUM          -- INTEGER
CALENDAR_WEEK_START_DT          -- DATE
TRADE_WEEK_START_DT             -- DATE
CALENDAR_WEEK_START_KEY         -- INTEGER
TRADE_WEEK_START_KEY            -- INTEGER
CALENDAR_WEEK_END_DT            -- DATE
TRADE_WEEK_END_DT               -- DATE
CALENDAR_WEEK_END_KEY           -- INTEGER
TRADE_WEEK_END_KEY              -- INTEGER

-- MONTH Level (with pattern variants)
CALENDAR_MONTH_NUM              -- INTEGER
TRADE_MONTH_445_NUM             -- INTEGER
TRADE_MONTH_454_NUM             -- INTEGER
TRADE_MONTH_544_NUM             -- INTEGER
CALENDAR_MONTH_NM               -- VARCHAR
TRADE_MONTH_445_NM              -- VARCHAR
TRADE_MONTH_454_NM              -- VARCHAR
TRADE_MONTH_544_NM              -- VARCHAR
CALENDAR_MONTH_ABBR             -- VARCHAR
TRADE_MONTH_ABBR                -- VARCHAR
CALENDAR_MONTH_IN_QUARTER_NUM   -- INTEGER
CALENDAR_MONTH_OVERALL_NUM      -- INTEGER
TRADE_MONTH_OVERALL_NUM         -- INTEGER
CALENDAR_YEARMONTH_NUM          -- INTEGER
TRADE_YEARMONTH_NUM             -- INTEGER
CALENDAR_MONTH_START_DT         -- DATE
TRADE_MONTH_445_START_DT        -- DATE
TRADE_MONTH_454_START_DT        -- DATE
TRADE_MONTH_544_START_DT        -- DATE
CALENDAR_MONTH_START_KEY        -- INTEGER
TRADE_MONTH_445_START_KEY       -- INTEGER
TRADE_MONTH_454_START_KEY       -- INTEGER
TRADE_MONTH_544_START_KEY       -- INTEGER
CALENDAR_MONTH_END_DT           -- DATE
TRADE_MONTH_445_END_DT          -- DATE
TRADE_MONTH_454_END_DT          -- DATE
TRADE_MONTH_544_END_DT          -- DATE
CALENDAR_MONTH_END_KEY          -- INTEGER
TRADE_MONTH_445_END_KEY         -- INTEGER
TRADE_MONTH_454_END_KEY         -- INTEGER
TRADE_MONTH_544_END_KEY         -- INTEGER

-- QUARTER Level
CALENDAR_QUARTER_NUM            -- INTEGER
TRADE_QUARTER_NUM               -- INTEGER
CALENDAR_QUARTER_NM             -- VARCHAR
TRADE_QUARTER_NM                -- VARCHAR
CALENDAR_QUARTER_START_DT       -- DATE
TRADE_QUARTER_START_DT          -- DATE
CALENDAR_QUARTER_START_KEY      -- INTEGER
TRADE_QUARTER_START_KEY         -- INTEGER
CALENDAR_QUARTER_END_DT         -- DATE
TRADE_QUARTER_END_DT            -- DATE
CALENDAR_QUARTER_END_KEY        -- INTEGER
TRADE_QUARTER_END_KEY           -- INTEGER

-- YEAR Level
CALENDAR_YEAR_NUM               -- INTEGER
TRADE_YEAR_NUM                  -- INTEGER
CALENDAR_YEAR_START_DT          -- DATE
TRADE_YEAR_START_DT             -- DATE
CALENDAR_YEAR_START_KEY         -- INTEGER
TRADE_YEAR_START_KEY            -- INTEGER
CALENDAR_YEAR_END_DT            -- DATE
TRADE_YEAR_END_DT               -- DATE
CALENDAR_YEAR_END_KEY           -- INTEGER
TRADE_YEAR_END_KEY              -- INTEGER
CALENDAR_IS_LEAP_YEAR_FLG       -- INTEGER
IS_TRADE_LEAP_WEEK_FLG          -- INTEGER
WEEKS_IN_TRADE_YEAR_NUM         -- INTEGER

-- ISO Columns
ISO_YEAR_NUM                    -- INTEGER
ISO_WEEK_OF_YEAR_TXT            -- VARCHAR
ISO_WEEK_OVERALL_NUM            -- INTEGER
ISO_WEEK_START_DT               -- DATE
ISO_WEEK_START_KEY              -- INTEGER
ISO_WEEK_END_DT                 -- DATE
ISO_WEEK_END_KEY                -- INTEGER

-- Metadata
DW_SYNCED_TS                    -- TIMESTAMP
DW_SOURCE_NM                    -- VARCHAR
CREATE_USER_ID                  -- VARCHAR
CREATE_TIMESTAMP                -- TIMESTAMP
```

## Business Rules Implementation

### DIM_DATE Business Rules
1. **Week 1 Rule**: Week 1 MUST contain January 1st
2. **Week Boundaries**: All weeks run Sunday (day 1) through Saturday (day 7)
3. **Partial Weeks**: Allowed at year boundaries
4. **Year Attribution**: Date belongs to its calendar year (Jan 1, 2025 has YEAR_NUM = 2025)
5. **Week Numbering**: Sequential within year, no gaps

### DIM_TRADE_DATE Business Rules
1. **Trade Year Start**: Sunday on or before February 1, minus 28 days
2. **Complete Weeks**: Always 7-day weeks, no partial weeks ever
3. **Pattern Support**: All three patterns (4-4-5, 4-5-4, 5-4-4) in parallel columns
4. **Week Assignment**:
   - 4-4-5: Weeks 1-4 (Jan), 5-8 (Feb), 9-13 (Mar), etc.
   - 4-5-4: Weeks 1-4 (Jan), 5-9 (Feb), 10-13 (Mar), etc.
   - 5-4-4: Weeks 1-5 (Jan), 6-9 (Feb), 10-13 (Mar), etc.
5. **Leap Week**: Week 53 added to December when applicable

### Special Records Rules
```sql
-- All tables must include these special records:
DATE_KEY = -1: Not Available/NULL (dates: 1900-01-01, text: 'Not Available', numbers: -1)
DATE_KEY = -2: Invalid (dates: 1900-01-02, text: 'Invalid', numbers: -2)
DATE_KEY = -3: Not Applicable (dates: 1900-01-03, text: 'Not Applicable', numbers: -3)
DATE_KEY = -4: Unknown (dates: 1900-01-04, text: 'Unknown', numbers: -4)
```

## Test Suite 1: DIM_DATE Validations

```sql
-- Test 1.1: Week 1 contains January 1
WITH week_one AS (
    SELECT YEAR_NUM, MIN(FULL_DATE) as week_start, MAX(FULL_DATE) as week_end
    FROM DIM_DATE
    WHERE WEEK_NUM = 1 AND DATE_KEY > 0
    GROUP BY YEAR_NUM
)
SELECT COUNT(*) as error_count
FROM week_one
WHERE DATE(YEAR_NUM || '-01-01') NOT BETWEEN week_start AND week_end;
-- MUST RETURN: 0

-- Test 1.2: All weeks are Sunday-Saturday
SELECT COUNT(*) as error_count
FROM DIM_DATE
WHERE DATE_KEY > 0
  AND (DAYOFWEEK(WEEK_BEGIN_DT) != 1 OR DAYOFWEEK(WEEK_END_DT) != 7);
-- MUST RETURN: 0

-- Test 1.3: No missing dates
WITH date_gaps AS (
    SELECT FULL_DATE, LEAD(FULL_DATE) OVER (ORDER BY FULL_DATE) as next_date
    FROM DIM_DATE WHERE DATE_KEY > 0
)
SELECT COUNT(*) as error_count
FROM date_gaps
WHERE DATEDIFF('DAY', FULL_DATE, next_date) > 1;
-- MUST RETURN: 0

-- Test 1.4: Week numbers are sequential
WITH week_seq AS (
    SELECT YEAR_NUM, WEEK_NUM,
           LAG(WEEK_NUM) OVER (PARTITION BY YEAR_NUM ORDER BY WEEK_NUM) as prev_week
    FROM (SELECT DISTINCT YEAR_NUM, WEEK_NUM FROM DIM_DATE WHERE DATE_KEY > 0) w
)
SELECT COUNT(*) as error_count
FROM week_seq
WHERE WEEK_NUM - prev_week > 1 AND prev_week IS NOT NULL;
-- MUST RETURN: 0

-- Test 1.5: Year attribution correct
SELECT COUNT(*) as error_count
FROM DIM_DATE
WHERE DATE_KEY > 0 AND YEAR_NUM != YEAR(FULL_DATE);
-- MUST RETURN: 0

-- Test 1.6: Special records exist
SELECT COUNT(*) as missing_special_records
FROM (SELECT -1 as key UNION SELECT -2 UNION SELECT -3 UNION SELECT -4) expected
LEFT JOIN DIM_DATE d ON expected.key = d.DATE_KEY
WHERE d.DATE_KEY IS NULL;
-- MUST RETURN: 0
```

## Test Suite 2: DIM_TRADE_DATE Validations

```sql
-- Test 2.1: All trade weeks have exactly 7 days
SELECT COUNT(*) as error_count
FROM (
    SELECT TRADE_YEAR_NUM, TRADE_WEEK_NUM, COUNT(*) as day_count
    FROM DIM_TRADE_DATE
    WHERE DATE_KEY > 0
    GROUP BY TRADE_YEAR_NUM, TRADE_WEEK_NUM
    HAVING COUNT(*) != 7
) bad_weeks;
-- MUST RETURN: 0

-- Test 2.2: Validate 4-4-5 pattern
WITH month_445 AS (
    SELECT TRADE_YEAR_NUM, TRADE_MONTH_445_NUM,
           COUNT(DISTINCT TRADE_WEEK_NUM) as weeks
    FROM DIM_TRADE_DATE WHERE DATE_KEY > 0
    GROUP BY TRADE_YEAR_NUM, TRADE_MONTH_445_NUM
)
SELECT COUNT(*) as error_count
FROM month_445
WHERE NOT (
    (TRADE_MONTH_445_NUM IN (1,2,4,5,7,8,10,11) AND weeks = 4) OR
    (TRADE_MONTH_445_NUM IN (3,6,9) AND weeks = 5) OR
    (TRADE_MONTH_445_NUM = 12 AND weeks IN (5,6))
);
-- MUST RETURN: 0

-- Test 2.3: Validate 4-5-4 pattern
WITH month_454 AS (
    SELECT TRADE_YEAR_NUM, TRADE_MONTH_454_NUM,
           COUNT(DISTINCT TRADE_WEEK_NUM) as weeks
    FROM DIM_TRADE_DATE WHERE DATE_KEY > 0
    GROUP BY TRADE_YEAR_NUM, TRADE_MONTH_454_NUM
)
SELECT COUNT(*) as error_count
FROM month_454
WHERE NOT (
    (TRADE_MONTH_454_NUM IN (1,3,4,6,7,9,10) AND weeks = 4) OR
    (TRADE_MONTH_454_NUM IN (2,5,8,11) AND weeks = 5) OR
    (TRADE_MONTH_454_NUM = 12 AND weeks IN (4,5))
);
-- MUST RETURN: 0

-- Test 2.4: Validate 5-4-4 pattern
WITH month_544 AS (
    SELECT TRADE_YEAR_NUM, TRADE_MONTH_544_NUM,
           COUNT(DISTINCT TRADE_WEEK_NUM) as weeks
    FROM DIM_TRADE_DATE WHERE DATE_KEY > 0
    GROUP BY TRADE_YEAR_NUM, TRADE_MONTH_544_NUM
)
SELECT COUNT(*) as error_count
FROM month_544
WHERE NOT (
    (TRADE_MONTH_544_NUM IN (1,4,7,10) AND weeks = 5) OR
    (TRADE_MONTH_544_NUM IN (2,3,5,6,8,9,11,12) AND weeks = 4) OR
    (TRADE_MONTH_544_NUM = 12 AND weeks = 5)
);
-- MUST RETURN: 0

-- Test 2.5: Trade weeks are Sunday-Saturday
SELECT COUNT(*) as error_count
FROM (
    SELECT TRADE_WEEK_NUM, TRADE_YEAR_NUM,
           MIN(CALENDAR_FULL_DT) as week_start,
           MAX(CALENDAR_FULL_DT) as week_end
    FROM DIM_TRADE_DATE WHERE DATE_KEY > 0
    GROUP BY TRADE_WEEK_NUM, TRADE_YEAR_NUM
    HAVING DAYOFWEEK(MIN(CALENDAR_FULL_DT)) != 1
        OR DAYOFWEEK(MAX(CALENDAR_FULL_DT)) != 7
) bad_weeks;
-- MUST RETURN: 0

-- Test 2.6: Special records exist
SELECT COUNT(*) as missing_special_records
FROM (SELECT -1 as key UNION SELECT -2 UNION SELECT -3 UNION SELECT -4) expected
LEFT JOIN DIM_TRADE_DATE d ON expected.key = d.DATE_KEY
WHERE d.DATE_KEY IS NULL;
-- MUST RETURN: 0
```

## Test Suite 3: Cross-Table Validations

```sql
-- Test 3.1: Same DATE_KEYs in both tables
SELECT COUNT(*) as mismatched_keys
FROM (
    SELECT DATE_KEY FROM DIM_DATE WHERE DATE_KEY > 0
    EXCEPT
    SELECT DATE_KEY FROM DIM_TRADE_DATE WHERE DATE_KEY > 0
    UNION ALL
    SELECT DATE_KEY FROM DIM_TRADE_DATE WHERE DATE_KEY > 0
    EXCEPT
    SELECT DATE_KEY FROM DIM_DATE WHERE DATE_KEY > 0
) diff;
-- MUST RETURN: 0

-- Test 3.2: Same date range
SELECT CASE
    WHEN d.min_date = t.min_date AND d.max_date = t.max_date
    THEN 0 ELSE 1 END as date_range_mismatch
FROM (
    SELECT MIN(FULL_DATE) as min_date, MAX(FULL_DATE) as max_date
    FROM DIM_DATE WHERE DATE_KEY > 0
) d
CROSS JOIN (
    SELECT MIN(CALENDAR_FULL_DT) as min_date, MAX(CALENDAR_FULL_DT) as max_date
    FROM DIM_TRADE_DATE WHERE DATE_KEY > 0
) t;
-- MUST RETURN: 0
```

## Aggregate Table Specifications

### Phase 3 Implementation (After Base Tables Pass All Tests)

#### DIM_WEEK (from DIM_DATE)
```sql
-- Key columns:
WEEK_KEY (YYYYWW format)
WEEK_START_DT
WEEK_END_DT
WEEK_START_KEY
WEEK_END_KEY
YEAR_NUM
WEEK_NUM
DAYS_IN_WEEK_NUM
-- Include current week flags (as view layer)
```

#### DIM_TRADE_WEEK (from DIM_TRADE_DATE)
```sql
-- Key columns:
TRADE_WEEK_KEY (use TRADE_WEEK_START_KEY)
TRADE_WEEK_START_DT
TRADE_WEEK_END_DT
TRADE_WEEK_START_KEY
TRADE_WEEK_END_KEY
TRADE_YEAR_NUM
TRADE_WEEK_NUM
TRADE_WEEK_OF_YEAR_NUM
TRADE_WEEK_OF_QUARTER_NUM
-- Pattern-specific columns:
TRADE_MONTH_445_NUM
TRADE_MONTH_454_NUM
TRADE_MONTH_544_NUM
TRADE_WEEK_OF_MONTH_445_NUM
TRADE_WEEK_OF_MONTH_454_NUM
TRADE_WEEK_OF_MONTH_544_NUM
DAYS_IN_WEEK_NUM
WEEKS_IN_TRADE_YEAR_NUM
IS_TRADE_LEAP_WEEK_FLG
```

#### DIM_MONTH (from DIM_DATE)
```sql
-- Aggregate from DIM_WEEK for efficiency
-- Key columns:
MONTH_KEY (YYYYMM format)
YEAR_NUM
MONTH_NUM
MONTH_NM
MONTH_START_DT
MONTH_END_DT
DAYS_IN_MONTH_NUM
WEEKS_IN_MONTH_NUM (partial weeks counted)
```

#### DIM_TRADE_MONTH (from DIM_TRADE_WEEK)
```sql
-- Single row per month with all three patterns
-- Key columns:
TRADE_MONTH_KEY (TRADE_YEAR_NUM * 100 + MONTH_NUM)
TRADE_YEAR_NUM
TRADE_MONTH_NUM (same across patterns)
TRADE_MONTH_START_DT
TRADE_MONTH_END_DT
-- Pattern-specific:
TRADE_WEEKS_IN_MONTH_445_NUM
TRADE_WEEKS_IN_MONTH_454_NUM
TRADE_WEEKS_IN_MONTH_544_NUM
IS_5_WEEK_MONTH_445_FLG
IS_5_WEEK_MONTH_454_FLG
IS_5_WEEK_MONTH_544_FLG
```

## Success Criteria

### Phase 1 Complete When:
1. All Test Suite 1 queries return 0 errors
2. All Test Suite 2 queries return 0 errors
3. All Test Suite 3 queries return 0 errors
4. Special records validated in both tables
5. Column naming matches specification exactly

### Phase 2 Complete When:
1. Orchestration agent confirms all Phase 1 criteria met
2. Documented decision to proceed to Phase 3

### Phase 3 Complete When:
1. All 8 aggregate tables created
2. Each aggregate has at least 3 validation queries
3. Row counts match expected values
4. All columns follow naming standards

## Error Handling Instructions

1. **If validation fails**:
   - Document the specific test that failed
   - Show sample of bad data (LIMIT 10)
   - Propose fix
   - Implement fix
   - Re-run ALL tests

2. **If patterns don't match**:
   - Check week-to-month assignment logic
   - Verify trade year start date calculation
   - Ensure leap week handling is correct

3. **If special records cause issues**:
   - Ensure they're excluded from validation queries (DATE_KEY > 0)
   - Verify sentinel dates are used correctly (1900-01-01, etc.)

## Orchestration Agent Instructions

You are the master coordinator. Your responsibilities:

1. **Initialize**: Create two parallel work streams for Phase 1
2. **Monitor**: Check progress every 5 minutes
3. **Validate**: Run all test suites when agents report completion
4. **Gate**: Do not proceed to Phase 3 until ALL Phase 1/2 tests pass
5. **Report**: Provide status updates showing:
   - Tests passed/failed
   - Current phase
   - Estimated completion time
6. **Escalate**: If an agent is stuck for >15 minutes, intervene

## Final Deliverables

1. **DDL Scripts**: Complete CREATE TABLE statements
2. **Population Scripts**: INSERT/SELECT statements for data
3. **Validation Report**: All test results showing 0 errors
4. **Data Quality Report**: Row counts, date ranges, pattern validation
5. **Documentation**: Any deviations from specification and why

## IMPORTANT: Do Not Proceed With Aggregates Until Base Tables Are Perfect

The aggregate tables depend entirely on correct base tables. Any errors in DIM_DATE or DIM_TRADE_DATE will cascade into all aggregates. Take the time to get Phase 1 absolutely correct.

## File Organization

```
/dim_date_project/
  /ddl/
    - dim_date.sql
    - dim_trade_date.sql
    - dim_week.sql
    - dim_trade_week.sql
    - dim_month.sql
    - dim_trade_month.sql
    - dim_quarter.sql
    - dim_trade_quarter.sql
  /population/
    - populate_dim_date.sql
    - populate_dim_trade_date.sql
    - populate_aggregates.sql
  /validation/
    - test_suite_1_dim_date.sql
    - test_suite_2_dim_trade_date.sql
    - test_suite_3_cross_validation.sql
    - test_aggregates.sql
  /documentation/
    - implementation_notes.md
    - validation_results.md
    - issues_and_resolutions.md
```

Begin execution immediately. Report status every 5 minutes or upon completion of major milestones.
