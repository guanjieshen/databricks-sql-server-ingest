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
| `test_incremental_output.py` | `incremental_output.yaml` — tables with changes, `generated_at` timestamp |
| `test_metadata_helper.py` | `parse_output_yaml` — manifest parsing, incremental fallback, `external_access` flag propagation across all code paths |
| `test_metadata_helper_uoid.py` | UOID consistency between `metadata_helper._make_uoid` and `azsql_ct.writer._make_uoid` |
| `test_generate_jobs.py` | DAB resource generation: pipeline/job YAML, `external_access` Iceberg Spark configs, stale file cleanup, dry-run |
| `test_cli.py` | `python -m azsql_ct` entry point |
| `test_integration.py` | Live DB: restricted login sees only allowed databases |

---

## `external_access` Test Coverage

The `external_access` pipeline config flag controls whether Iceberg UniForm V3 table properties are applied to silver streaming tables. Tests cover two layers:

**`test_metadata_helper.py`** — `parse_output_yaml` returns `(table_configs, data_path, external_access)`:

| Scenario | Test |
|----------|------|
| Key missing from YAML | `test_external_access_defaults_to_false` |
| Explicit `false` | `test_external_access_explicit_false` |
| Explicit `true` | `test_external_access_true` |
| `true` via incremental early-return path | `test_external_access_true_via_incremental_path` |
| `true` via incremental fallback (file missing) | `test_external_access_true_incremental_fallback_missing` |
| `true` via incremental fallback (no tables) | `test_external_access_true_incremental_fallback_empty` |

**`test_generate_jobs.py`** — DAB pipeline Spark configs:

| Scenario | Test |
|----------|------|
| Default (no flag) | `test_iceberg_configs_omitted_by_default` |
| `external_access=False` | `test_iceberg_configs_omitted_when_external_access_false` |
| `external_access=True` | `test_iceberg_configs_present_when_external_access_true` |

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
