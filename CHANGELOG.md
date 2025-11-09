# Changelog

## 2025-01-09 - Initial Release

### Added
- Enhanced testing script (`parse_sql_test.py`) with batch mode, metrics, and validation
- Results analysis script (`analyze_results.py`) with statistical reporting
- Test suite with 27 sample SQL queries across 4 categories
- Automated test runner (`run_tests.sh`)
- Comprehensive documentation (4 guides)
- DataHub connection test script (`test_datahub_connection.py`)

### Fixed
- API compatibility issue: `parse_sql_test.py` now handles both old and new DataHub versions
- Automatically falls back when `default_dialect` parameter is not supported
- Added version detection and compatibility handling

### Known Issues
- `default_dialect` parameter only available in newer DataHub versions
- Script automatically detects and adapts to API version

### Testing
To verify your setup:
```bash
python test_datahub_connection.py
```

To run tests:
```bash
python parse_sql_test.py --sql-file test-queries/teradata/01-basic/01_simple_select.sql --verbose
```
