# tests — azsql_ct Test Suite

Unit and integration tests for the `azsql_ct` package. Uses pytest with shared fixtures in `conftest.py`.

---

## Running Tests

```bash
# Unit tests (no database required; mocks DB connections):
pytest

# Verbose output:
pytest -v

# Specific test file or test:
pytest tests/test_client.py
pytest tests/test_client.py::TestFlattenTableMap::test_list_format

# Integration tests (requires live Azure SQL database):
pytest -m integration -v
```

---

## Test Layout

| File | Coverage |
|------|----------|
| `conftest.py` | Shared fixtures: `FakeCursor`, `mock_conn` |
| `test_client.py` | `ChangeTracker`, config loading, table map normalization, sync orchestration |
| `test_connection.py` | `AzureSQLConnection`, connectivity |
| `test_queries.py` | SQL query builders (full/incremental, CT version checks, PK discovery) |
| `test_sync.py` | Sync engine logic (watermark, mode selection, streaming) |
| `test_writer.py` | `ParquetWriter`, `UnifiedParquetWriter`, UOID, schema version |
| `test_watermark.py` | Watermark load/save, sync history |
| `test_schema.py` | Append-only schema tracking |
| `test_output_manifest.py` | `output.yaml` manifest load/merge/save |
| `test_cli.py` | `python -m azsql_ct` entry point |
| `test_integration.py` | Live DB: restricted login sees only allowed databases |

---

## Fixtures

- **`fake_cursor`**: Returns `FakeCursor` class — a lightweight DB-API 2.0 cursor stand-in. Supply `results` (list of row lists) and `descriptions` (list of column descriptions) per `execute()` call.
- **`mock_conn`**: `MagicMock` that behaves like a DB-API connection.

Unit tests mock `mssql_python` when not installed; `conftest.py` injects a stub module so imports succeed.

---

## Integration Tests

Marked with `@pytest.mark.integration`. Skipped by default.

**Requirements**:
- Live Azure SQL database
- `TEST_USER` and `TEST_PASSWORD` in environment or `.env`
- `test_integration.py` verifies a restricted login sees only allowed databases (`dbo.AllowedDatabases`, `sys.databases`)

---

## Related

- Root `README.md` — Testing section
- `azsql_ct` package — `.cursor/rules/azsql-ct-package.mdc` for module map
