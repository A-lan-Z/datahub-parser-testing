#!/usr/bin/env python3
"""
Diagnose DataHub and SQLGlot Version Compatibility

This script checks for version conflicts that can cause parsing errors.
"""

import sys

print("="*80)
print("DataHub & SQLGlot Version Diagnostics")
print("="*80)
print()

# Check DataHub version
print("1. Checking DataHub installation...")
try:
    import datahub
    datahub_version = getattr(datahub, "__version__", "unknown")
    print(f"   ✓ DataHub version: {datahub_version}")
except ImportError:
    print("   ✗ DataHub not installed")
    sys.exit(1)

print()

# Check SQLGlot version
print("2. Checking SQLGlot installation...")
try:
    import sqlglot
    sqlglot_version = getattr(sqlglot, "__version__", "unknown")
    print(f"   ✓ SQLGlot version: {sqlglot_version}")
except ImportError:
    print("   ✗ SQLGlot not installed")
    print("   Installing SQLGlot...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "sqlglot"])
    import sqlglot
    sqlglot_version = getattr(sqlglot, "__version__", "unknown")
    print(f"   ✓ SQLGlot version: {sqlglot_version}")

print()

# Check patchy library
print("3. Checking patchy library...")
try:
    import patchy
    patchy_version = getattr(patchy, "__version__", "unknown")
    print(f"   ✓ patchy version: {patchy_version}")
except ImportError:
    print("   ✗ patchy not installed (may be optional)")

print()

# Check for acryl-sqlglot
print("4. Checking for acryl-sqlglot (deprecated fork)...")
try:
    import pkg_resources
    try:
        acryl_sqlglot = pkg_resources.get_distribution("acryl-sqlglot")
        print(f"   ⚠ WARNING: acryl-sqlglot found (version {acryl_sqlglot.version})")
        print("   ⚠ This is a deprecated fork that may conflict with mainline SQLGlot")
        print("   ⚠ Consider uninstalling: pip uninstall acryl-sqlglot")
    except pkg_resources.DistributionNotFound:
        print("   ✓ acryl-sqlglot not installed (good)")
except ImportError:
    print("   ⚠ Cannot check for acryl-sqlglot")

print()

# Try to import sql parsing module
print("5. Checking DataHub SQL parsing module...")
try:
    from datahub.ingestion.graph.client import DataHubGraph
    print("   ✓ DataHubGraph imported successfully")

    # Check if parse_sql_lineage exists
    if hasattr(DataHubGraph, 'parse_sql_lineage'):
        print("   ✓ parse_sql_lineage method found")
    else:
        print("   ✗ parse_sql_lineage method NOT found")
        print("   ⚠ Your DataHub version may not support SQL parsing")
        sys.exit(1)

except ImportError as e:
    print(f"   ✗ Import failed: {e}")
    sys.exit(1)

print()

# Try a simple parse to see the exact error
print("6. Testing SQL parsing with simple query...")
try:
    from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
    import os

    server = os.getenv("DATAHUB_SERVER", "http://localhost:8080")
    token = os.getenv("DATAHUB_TOKEN", "")

    if not token:
        print("   ⚠ DATAHUB_TOKEN not set, skipping live test")
        print("   Set token to test: export DATAHUB_TOKEN='your-token'")
    else:
        graph = DataHubGraph(DatahubClientConfig(server=server, token=token))

        test_query = "SELECT col1, col2 FROM table1"

        try:
            result = graph.parse_sql_lineage(
                sql=test_query,
                platform="teradata",
            )
            print("   ✓ Parse succeeded!")
            print(f"   - Input tables: {getattr(result, 'in_tables', [])}")
        except Exception as e:
            print(f"   ✗ Parse failed with error:")
            print(f"   {type(e).__name__}: {e}")

            # Provide specific guidance
            if "hunk" in str(e).lower():
                print()
                print("   DIAGNOSIS: SQLGlot version mismatch")
                print("   " + "-"*70)
                print("   The 'hunk context' error means DataHub's patches to SQLGlot")
                print("   don't match your SQLGlot version.")
                print()
                print("   SOLUTIONS:")
                print("   1. Reinstall DataHub (will install compatible SQLGlot):")
                print("      pip uninstall acryl-datahub sqlglot")
                print("      pip install acryl-datahub")
                print()
                print("   2. Or downgrade SQLGlot to compatible version:")
                print("      pip install 'sqlglot<25.0.0'")
                print()
                print("   3. Or upgrade DataHub to latest:")
                print("      pip install --upgrade acryl-datahub")

except Exception as e:
    print(f"   ✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()

print()
print("="*80)
print("Diagnostic complete")
print("="*80)
