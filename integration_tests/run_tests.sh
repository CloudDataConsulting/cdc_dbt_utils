#!/bin/bash
# Integration test runner for cdc_dbt_utils logging functionality

echo "=================================================="
echo "CDC dbt Utils - Logging Integration Tests"
echo "=================================================="

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if we're in the right directory
if [ ! -f "dbt_project.yml" ]; then
    echo -e "${RED}Error: Must run from integration_tests directory${NC}"
    exit 1
fi

# Function to run a test and check results
run_test() {
    local test_name=$1
    local dbt_command=$2
    local expected_status=${3:-0}
    
    echo -e "\n${YELLOW}Running test: $test_name${NC}"
    echo "Command: $dbt_command"
    
    # Run the dbt command
    eval $dbt_command
    local status=$?
    
    if [ $status -eq $expected_status ]; then
        echo -e "${GREEN}✓ Test passed${NC}"
        return 0
    else
        echo -e "${RED}✗ Test failed (expected status $expected_status, got $status)${NC}"
        return 1
    fi
}

# Initialize test environment
echo -e "\n${YELLOW}Setting up test environment...${NC}"

# Install dependencies
echo "Installing package dependencies..."
dbt deps

# Test 1: Basic compilation
run_test "Compilation Test" "dbt compile"

# Test 2: Run basic logging model
run_test "Basic Logging" "dbt run --models test_basic_logging"

# Test 3: Run error logging model
run_test "Error Logging" "dbt run --models test_error_logging"

# Test 4: Run custom parameters model
run_test "Custom Parameters" "dbt run --models test_custom_params"

# Test 5: Test run-level operations
echo -e "\n${YELLOW}Testing run-level operations...${NC}"
RUN_ID=$(dbt run-operation start_run_logging --vars '{run_name: "integration_test"}' | grep "process ID:" | awk '{print $NF}')
echo "Started run with ID: $RUN_ID"

# Run all test models
dbt run --models tag:logging_test

# Stop run logging
dbt run-operation stop_run_logging --vars "{run_id: $RUN_ID}"

# Test 6: Check logging results
echo -e "\n${YELLOW}Verifying logging results...${NC}"
dbt run-operation check_logging_results --vars "{test_run_id: '$RUN_ID'}"

# Test 7: Test failure scenario (optional)
if [ "$1" == "--test-failures" ]; then
    echo -e "\n${YELLOW}Testing failure scenarios...${NC}"
    run_test "Model Failure Logging" "dbt run --models test_failed_model --vars '{test_failures: true, force_test_failure: true}'" 1
fi

# Summary
echo -e "\n=================================================="
echo -e "${GREEN}Integration tests completed!${NC}"
echo "Check the following tables for results:"
echo "  - dw_util.process_instance"
echo "  - dw_util.process_report_v"
echo "  - dw_util.error_log"
echo -e "==================================================\n"