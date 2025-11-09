#!/bin/bash
# DataHub SQL Parser Test Suite Runner
# This script runs the complete test suite and generates analysis reports

set -e  # Exit on error

# Configuration
DATAHUB_SERVER="${DATAHUB_SERVER:-http://localhost:8080}"
PLATFORM="${PLATFORM:-teradata}"
ENV="${ENV:-PROD}"
DEFAULT_DIALECT="${DEFAULT_DIALECT:-teradata}"

# Directories
TEST_DIR="test-queries/teradata"
RESULTS_DIR="test-results"
EXPECTED_DIR="expected-results"

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "================================="
echo "DataHub SQL Parser Test Suite"
echo "================================="
echo "Server: $DATAHUB_SERVER"
echo "Platform: $PLATFORM"
echo "Dialect: $DEFAULT_DIALECT"
echo "================================="
echo ""

# Function to run tests for a category
run_category() {
    local category=$1
    local category_name=$(basename "$category")

    echo "Testing: $category_name"
    echo "---------------------------------"

    python parse_sql_test.py \
        --sql-dir "$category" \
        --server "$DATAHUB_SERVER" \
        --platform "$PLATFORM" \
        --env "$ENV" \
        --default-dialect "$DEFAULT_DIALECT" \
        --expected-dir "$EXPECTED_DIR" \
        --output-csv "$RESULTS_DIR/${category_name}-results.csv" \
        --output-json "$RESULTS_DIR/${category_name}-results.json"

    echo ""
}

# Run tests for each category
for category in "$TEST_DIR"/*; do
    if [ -d "$category" ]; then
        run_category "$category"
    fi
done

# Combine all CSV results
echo "Combining results..."
{
    # Header from first file
    head -n 1 "$RESULTS_DIR/01-basic-results.csv"

    # Data from all files (skip headers)
    for csv in "$RESULTS_DIR"/*-results.csv; do
        tail -n +2 "$csv"
    done
} > "$RESULTS_DIR/all-results.csv"

echo "All results combined: $RESULTS_DIR/all-results.csv"
echo ""

# Generate analysis
echo "Generating analysis report..."
python analyze_results.py "$RESULTS_DIR/all-results.csv" \
    --output-json "$RESULTS_DIR/analysis.json"

echo ""
echo "================================="
echo "Test suite complete!"
echo "Results: $RESULTS_DIR/"
echo "================================="
