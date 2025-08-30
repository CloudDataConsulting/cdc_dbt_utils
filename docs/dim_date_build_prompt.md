# Claude Code Orchestration Prompt for Date Dimension Implementation

## Project Overview
Implement a comprehensive date dimension system according to the official specification document "DIM_DATE and DIM_TRADE_DATE Complete Specification". This prompt defines the orchestration strategy and agent coordination, NOT the technical specifications.

## Reference Documents
**PRIMARY SPECIFICATION**: Use the document titled "DIM_DATE and DIM_TRADE_DATE Complete Specification" as the single source of truth for:
- Column definitions and naming standards
- Business rules
- Validation queries
- Test criteria
- Special records requirements

## Orchestration Strategy

### Phase 1: Base Table Implementation (Parallel Execution)

**Agent 1: DIM_DATE Implementation**
- Reference: Specification Section "DIM_DATE (Standard Calendar) Specification"
- Implement according to column list in specification
- Apply business rules from specification
- Include special records as defined in "Special Records Specification"
- Run Test Suite 1 from specification
- Document any deviations or issues

**Agent 2: DIM_TRADE_DATE Implementation**
- Reference: Specification Section "DIM_TRADE_DATE (Multi-Pattern Trade Calendar) Specification"
- Implement according to column list in specification
- Apply business rules from specification
- Include special records as defined in "Special Records Specification"
- Run Test Suite 2 from specification
- Document any deviations or issues

### Phase 2: Validation Gate (Sequential - Orchestration Agent)
**Do not proceed to Phase 3 until:**
1. All Test Suite 1 queries return 0 errors
2. All Test Suite 2 queries return 0 errors
3. All Test Suite 3 (Cross-validation) queries return 0 errors
4. Special records validated in both tables
5. Confirmation that column naming matches specification exactly

### Phase 3: Aggregate Table Implementation (Parallel Execution)

**Agent 3: Calendar Aggregates**
Build these aggregates from DIM_DATE:
- DIM_WEEK
- DIM_MONTH
- DIM_QUARTER
- DIM_YEAR

**Agent 4: Trade Calendar Aggregates**
Build these aggregates from DIM_TRADE_DATE:
- DIM_TRADE_WEEK (aggregate from base table)
- DIM_TRADE_MONTH (aggregate from DIM_TRADE_WEEK for efficiency)
- DIM_TRADE_QUARTER (aggregate from DIM_TRADE_MONTH for efficiency)
- DIM_TRADE_YEAR (aggregate from DIM_TRADE_QUARTER for efficiency)

## Agent Coordination Instructions

### Orchestration Agent (Master Coordinator)
You are responsible for:
1. **Initialization**:
   - Verify specification document is available
   - Assign Agent 1 and Agent 2 to parallel work
   - Create tracking dashboard for progress

2. **Monitoring**:
   - Check agent progress every 5 minutes
   - Track which tests have passed/failed
   - Ensure agents are following specification

3. **Validation Gates**:
   - Run all validation queries from specification
   - Document test results
   - Make go/no-go decision for next phase

4. **Escalation**:
   - If an agent is stuck for >15 minutes, intervene
   - If specification is unclear, request clarification
   - If tests repeatedly fail, analyze root cause

5. **Reporting**:
   ```
   Status Report Format:
   ====================
   Current Phase: [1/2/3]
   Start Time: [timestamp]

   Agent 1 (DIM_DATE):
   - Status: [In Progress/Complete/Blocked]
   - Tests Passed: X/6
   - Current Task: [description]

   Agent 2 (DIM_TRADE_DATE):
   - Status: [In Progress/Complete/Blocked]
   - Tests Passed: X/6
   - Current Task: [description]

   Issues Requiring Attention:
   - [List any blockers]

   Next Checkpoint: [time]
   ```

### Individual Agent Instructions

**All Agents Must:**
1. Read the specification document FIRST
2. Follow column naming EXACTLY as specified
3. Run ALL applicable validation queries
4. Document any deviations with justification
5. Report status to Orchestration Agent every 5 minutes
6. STOP if validation fails and report immediately

**Parallel Execution Rules:**
- Agents 1 and 2 work simultaneously in Phase 1
- Agents 3 and 4 work simultaneously in Phase 3
- No agent proceeds to next phase without orchestrator approval

## Error Handling Protocol

### When Validation Fails:
1. **Document**: Which specific test failed
2. **Analyze**: Show sample of failing records (LIMIT 10)
3. **Propose**: Specific fix with reasoning
4. **Implement**: Apply the fix
5. **Retest**: Run ALL tests again (not just the one that failed)
6. **Report**: Update orchestration agent

### Common Issues to Check:
- Week 1 not containing January 1 (DIM_DATE)
- Trade year start calculation incorrect (DIM_TRADE_DATE)
- Pattern assignments wrong (check 4-4-5, 4-5-4, 5-4-4 logic)
- Special records missing or malformed
- DATE_KEY mismatch between tables

## File Organization Structure

```
/dim_date_project/
  /specifications/
    - DIM_DATE_TRADE_DATE_Specification.md (reference document)
  /ddl/
    - dim_date.sql
    - dim_trade_date.sql
    - [aggregate tables].sql
  /population/
    - populate_dim_date.sql
    - populate_dim_trade_date.sql
    - populate_special_records.sql
    - populate_aggregates.sql
  /validation/
    - test_suite_1_dim_date.sql
    - test_suite_2_dim_trade_date.sql
    - test_suite_3_cross_validation.sql
    - test_aggregates.sql
  /results/
    - validation_results_[timestamp].txt
    - issues_log.md
  /documentation/
    - implementation_notes.md
    - deviations_from_spec.md
```

## Success Criteria

### Phase 1 Success:
- [ ] DIM_DATE passes all 6 tests in Test Suite 1
- [ ] DIM_TRADE_DATE passes all 6 tests in Test Suite 2
- [ ] Cross-validation passes all tests in Test Suite 3
- [ ] Special records present and correct
- [ ] Column names match specification exactly

### Phase 2 Success:
- [ ] Orchestration agent has verified all Phase 1 criteria
- [ ] All validation results documented
- [ ] Go decision recorded with timestamp

### Phase 3 Success:
- [ ] All 8 aggregate tables created
- [ ] Each aggregate has minimum 3 validation queries passing
- [ ] Row counts reconcile with base tables
- [ ] Performance benchmarks met (<5 seconds for queries)

## Critical Reminders

1. **THE SPECIFICATION DOCUMENT IS THE SINGLE SOURCE OF TRUTH**
2. **DO NOT PROCEED TO AGGREGATES IF BASE TABLES HAVE ANY ERRORS**
3. **ALL VALIDATION QUERIES MUST RETURN 0 ERRORS**
4. **EXCLUDE SPECIAL RECORDS (DATE_KEY > 0) FROM VALIDATION QUERIES**
5. **MAINTAIN SEPARATE TABLES - DO NOT MIX CALENDAR AND TRADE COLUMNS**

## Execution Commands

```bash
# Start Phase 1
EXECUTE: Initialize Agent 1 with DIM_DATE specification
EXECUTE: Initialize Agent 2 with DIM_TRADE_DATE specification
MONITOR: Every 5 minutes until both complete

# Validation Gate
EXECUTE: Run Test Suite 1, 2, 3
DECISION: If all pass, proceed to Phase 3

# Start Phase 3 (only after validation)
EXECUTE: Initialize Agent 3 with calendar aggregates
EXECUTE: Initialize Agent 4 with trade aggregates
MONITOR: Every 5 minutes until complete
```

## Start Execution
Begin immediately. First action: Orchestration Agent confirms access to specification document and initializes parallel agents for Phase 1.
