# Troubleshooting Guide

## "Could not find match for hunk context" Error

### What This Error Means

This error occurs when DataHub tries to apply patches (monkeypatches) to SQLGlot, but the code structure doesn't match what's expected. This is a **version mismatch** between DataHub and SQLGlot.

**Technical Background:**
- DataHub uses the `patchy` library to modify SQLGlot's behavior at runtime
- These patches are written for specific SQLGlot versions
- If your SQLGlot version is too new or too old, the patches fail to apply

### Quick Fix

Run the diagnostic script to identify the issue:

```bash
python diagnose_versions.py
```

This will:
1. Check installed versions of DataHub and SQLGlot
2. Identify version conflicts
3. Provide specific fix recommendations

### Solution 1: Reinstall DataHub (Recommended)

This ensures you get compatible versions of all dependencies:

```bash
# Uninstall both packages
pip uninstall -y acryl-datahub sqlglot acryl-sqlglot

# Reinstall DataHub (will install compatible SQLGlot)
pip install acryl-datahub

# Verify installation
python diagnose_versions.py
```

### Solution 2: Install Compatible SQLGlot Version

If you need a specific DataHub version, try downgrading SQLGlot:

```bash
# For older DataHub versions, try SQLGlot < 25.0.0
pip install 'sqlglot<25.0.0'

# Or try a specific known-good version
pip install sqlglot==24.0.0

# Verify
python diagnose_versions.py
```

### Solution 3: Upgrade DataHub

If you have an old DataHub version:

```bash
# Upgrade to latest
pip install --upgrade acryl-datahub

# Verify
python diagnose_versions.py
```

### Solution 4: Remove acryl-sqlglot (if present)

`acryl-sqlglot` is a deprecated fork that may conflict:

```bash
# Check if installed
pip list | grep sqlglot

# If you see acryl-sqlglot, remove it
pip uninstall acryl-sqlglot

# Reinstall mainline SQLGlot
pip install sqlglot

# Verify
python diagnose_versions.py
```

---

## Other Common Errors

### "401 Unauthorized"

**Cause:** Invalid or missing authentication token

**Fix:**
```bash
# Set your token
export DATAHUB_TOKEN="your-personal-access-token-here"

# Or for Windows PowerShell
$env:DATAHUB_TOKEN="your-personal-access-token-here"

# Verify
python test_datahub_connection.py
```

**How to get a token:**
1. Log into DataHub UI
2. Go to Settings > Access Tokens
3. Click "Generate Personal Access Token"
4. Copy the token (it won't be shown again!)

---

### "Connection refused" or "Cannot connect"

**Cause:** DataHub server not running or wrong URL

**Fix:**
```bash
# Check if DataHub is running
curl http://localhost:8080/health

# If using different host/port, set it
export DATAHUB_SERVER="http://your-datahub-host:8080"

# Verify
python test_datahub_connection.py
```

---

### "unexpected keyword argument 'default_dialect'"

**Cause:** Older DataHub version doesn't support this parameter

**Fix:** Already handled automatically by `parse_sql_test.py`! The script detects this and falls back to compatible mode.

If you want the latest features:
```bash
pip install --upgrade acryl-datahub
```

---

### "No column lineage" (Columns: 0)

**Cause:** Table schemas not registered in DataHub

**This is expected!** Column-level lineage requires:
1. Tables exist in DataHub
2. Schemas are registered with column information

**Fix for testing:**
See TESTING_FRAMEWORK.md section "DataHub Test Environment Setup" for how to register test schemas.

**For production:**
- Use DataHub's ingestion framework to sync schemas
- Or manually register via API/UI

---

### Parse succeeds but confidence is 0.0

**Cause:** Parser couldn't fully understand the query

**Possible reasons:**
- Tables don't exist in DataHub (can't resolve references)
- Complex syntax not fully supported
- Dynamic SQL or runtime references

**Fix:**
- Register table schemas in DataHub
- Simplify query if possible
- Check if dialect is correct (`--default-dialect teradata`)
- Review query for unsupported features (see Known Limitations)

---

### "Statement type: UNKNOWN"

**Cause:** The complexity analyzer couldn't identify the statement type

**This is usually harmless** - parsing may still work. Common causes:
- Query starts with a comment
- Unusual statement structure
- Multi-statement block

**Fix:** Not usually needed, but you can:
- Ensure query doesn't start with comments
- Check that statement is supported SQL

---

### Tests are very slow

**Cause:** Network latency to DataHub server, or server performance

**Check:**
```bash
# Time the health check
time curl http://localhost:8080/health

# Should be < 100ms for local server
```

**Fix:**
- Ensure DataHub is running locally or on fast network
- Check DataHub server resources (CPU, memory)
- Reduce test set size for initial testing

---

### "ModuleNotFoundError: No module named 'datahub'"

**Cause:** DataHub package not installed, or wrong Python environment

**Fix:**
```bash
# Install DataHub
pip install acryl-datahub

# If using virtual environment, make sure it's activated
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Verify
python -c "import datahub; print(datahub.__version__)"
```

---

## Version Compatibility Matrix

| DataHub Version | SQLGlot Version | Status |
|-----------------|-----------------|--------|
| 0.13.x | 23.x - 24.x | ✓ Stable |
| 0.12.x | 23.x | ✓ Stable |
| 0.11.x | 20.x - 22.x | ⚠ May need older SQLGlot |

**Note:** Always let DataHub install its preferred SQLGlot version unless you have specific requirements.

---

## Getting Help

### Step 1: Run Diagnostics

```bash
# Check versions and compatibility
python diagnose_versions.py

# Test connection
python test_datahub_connection.py
```

### Step 2: Check Logs

Look for detailed error messages in:
- Script output (stderr)
- DataHub server logs (if you have access)

### Step 3: Gather Information

When reporting issues, include:
- Output of `diagnose_versions.py`
- DataHub version: `pip show acryl-datahub`
- SQLGlot version: `pip show sqlglot`
- Python version: `python --version`
- Operating system
- Full error message and stack trace

### Step 4: Community Resources

- **DataHub Slack:** [slack.datahubproject.io](https://slack.datahubproject.io)
- **GitHub Issues:** [github.com/datahub-project/datahub](https://github.com/datahub-project/datahub)
- **Documentation:** [datahub.io/docs](https://datahub.io/docs)

---

## Quick Reference

```bash
# Diagnose version issues
python diagnose_versions.py

# Test connection
python test_datahub_connection.py

# Fix version mismatch (most common issue)
pip uninstall -y acryl-datahub sqlglot acryl-sqlglot
pip install acryl-datahub
python diagnose_versions.py

# Set authentication
export DATAHUB_TOKEN="your-token"
python test_datahub_connection.py

# Run a test
python parse_sql_test.py \
  --sql-file test-queries/teradata/01-basic/01_simple_select.sql \
  --verbose
```
