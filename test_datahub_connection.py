#!/usr/bin/env python3
"""
Test DataHub Connection and API Compatibility

This script verifies that:
1. DataHub server is reachable
2. Authentication token is valid
3. parse_sql_lineage API is available
4. Supported parameters are identified
"""

import inspect
import os
import sys

try:
    from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
except ImportError:
    print("ERROR: datahub package not installed")
    print("Install with: pip install acryl-datahub")
    sys.exit(1)

# Configuration
DATAHUB_SERVER = os.getenv("DATAHUB_SERVER", "http://localhost:8080")
DATAHUB_TOKEN = os.getenv("DATAHUB_TOKEN", "")

print("="*80)
print("DataHub Connection Test")
print("="*80)
print(f"Server: {DATAHUB_SERVER}")
print(f"Token: {'***' + DATAHUB_TOKEN[-10:] if len(DATAHUB_TOKEN) > 10 else '(not set)'}")
print()

# Test 1: Check parse_sql_lineage signature
print("1. Checking parse_sql_lineage API signature...")
try:
    sig = inspect.signature(DataHubGraph.parse_sql_lineage)
    params = list(sig.parameters.keys())
    print(f"   ✓ Method found with parameters: {', '.join(params)}")

    # Check for specific parameters
    has_default_dialect = 'default_dialect' in params
    has_sql = 'sql' in params

    print(f"   - Has 'sql' parameter: {has_sql}")
    print(f"   - Has 'default_dialect' parameter: {has_default_dialect}")

    if not has_default_dialect:
        print("   ⚠ WARNING: Your DataHub version may not support 'default_dialect'")
        print("   ⚠ The test script will automatically fallback to compatible mode")

except Exception as e:
    print(f"   ✗ Error inspecting method: {e}")
    sys.exit(1)

print()

# Test 2: Connect to server
if not DATAHUB_TOKEN:
    print("2. Testing server connection (without authentication)...")
    print("   ⚠ WARNING: DATAHUB_TOKEN not set")
    print("   Set with: export DATAHUB_TOKEN='your-token-here'")
    sys.exit(0)

print("2. Testing server connection with authentication...")
try:
    graph = DataHubGraph(DatahubClientConfig(server=DATAHUB_SERVER, token=DATAHUB_TOKEN))
    print(f"   ✓ DataHubGraph client created")
except Exception as e:
    print(f"   ✗ Error creating client: {e}")
    sys.exit(1)

print()

# Test 3: Try a simple parse
print("3. Testing parse_sql_lineage with simple query...")
test_query = "SELECT customer_id, customer_name FROM customers WHERE status = 'ACTIVE'"

try:
    # Try with default_dialect if available
    if has_default_dialect:
        result = graph.parse_sql_lineage(
            sql=test_query,
            platform="teradata",
            env="PROD",
            default_dialect="teradata"
        )
        print("   ✓ Parse successful (with default_dialect)")
    else:
        result = graph.parse_sql_lineage(
            sql=test_query,
            platform="teradata",
            env="PROD"
        )
        print("   ✓ Parse successful (without default_dialect)")

    # Check result
    in_tables = getattr(result, "in_tables", [])
    out_tables = getattr(result, "out_tables", [])
    debug_info = getattr(result, "debug_info", None)
    confidence = getattr(debug_info, "confidence", 0.0) if debug_info else 0.0

    print(f"   - Input tables found: {len(in_tables)}")
    print(f"   - Output tables found: {len(out_tables)}")
    print(f"   - Confidence score: {confidence:.3f}")

    if in_tables:
        print(f"   - First input table: {in_tables[0]}")

except Exception as e:
    print(f"   ✗ Parse failed: {e}")
    print(f"   Error type: {type(e).__name__}")

    if "401" in str(e) or "Unauthorized" in str(e):
        print("   ⚠ Authentication failed - check your DATAHUB_TOKEN")
    elif "Connection" in str(e) or "connect" in str(e).lower():
        print(f"   ⚠ Cannot connect to server at {DATAHUB_SERVER}")
        print("   ⚠ Make sure DataHub is running")

    sys.exit(1)

print()
print("="*80)
print("✓ All tests passed! DataHub connection is working.")
print("="*80)
print()
print("You can now run the test suite:")
print("  python parse_sql_test.py --sql-file test-queries/teradata/01-basic/01_simple_select.sql --verbose")
