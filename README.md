# Azure SQL restricted login setup

Azure SQL Server: **guanjiesqldb.database.windows.net**

- **sqladmin** -- Admin SQL login used to run setup. Set `ADMIN_PASSWORD` in `.env`.
- **test** -- Restricted login created by setup; access only to `master` and `database_1`, list via `dbo.AllowedDatabases`.

## Connection config

Copy `.env.example` to `.env` and set passwords (do not commit `.env`):

```bash
cp .env.example .env
# Edit .env: set ADMIN_PASSWORD and TEST_PASSWORD
```

## 1. Connect (do this first)

Use the **mssql-python** conda env (or set `PYTHON_CMD` / `PIP_CMD`). Verify you can reach the server and that admin credentials work:

```bash
conda activate mssql-python
pip install -r requirements.txt
python examples/connect.py
```

If this fails, fix firewall (Azure Portal -> SQL server -> Networking -> add your IP) or credentials before continuing.

## 2. Setup (run as sqladmin)

Run the full setup and validation:

```bash
./run_setup_and_test.sh
```

Or run manually:

1. **master** -- Run [create-login-master.sql](create-login-master.sql) in `master`.
2. **database_1** -- Run [create-user-database_1.sql](create-user-database_1.sql) in `database_1`.

The script uses the **mssql-python** conda env for Python (override with `PYTHON_CMD` and `PIP_CMD`) and `SERVER`, `ADMIN_USER`, `ADMIN_PASSWORD`, and `TEST_PASSWORD` from the environment.

## 3. Validation test

After setup, confirm the restricted login sees only the allowed databases:

```bash
pytest tests/test_integration.py -m integration -v
```

Uses `TEST_PASSWORD` (and optionally `SERVER`, `DATABASE`, `TEST_USER`) from env or `.env`.

## 4. Package usage

The `azsql_ct` package exposes an `AzureSQLConnection` class and sync utilities.
See `examples/` for runnable scripts.

```python
from azsql_ct import AzureSQLConnection

with AzureSQLConnection(database="master") as conn:
    cur = conn.cursor()
    cur.execute("SELECT 1 AS ok")
    print(cur.fetchone()[0])
```

## 5. Change tracking (database_1)

If change tracking is enabled on tables in **database_1**, the **test** user can read change tracking details after an admin grants permission once:

1. **Grant permission** (run in `database_1` as sqladmin):

   ```sql
   GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO test;
   ```

2. **Get change tracking details** as **test**:

   ```bash
   python examples/get_change_tracking.py
   python examples/get_change_tracking.py MyTable           # changes since version 0
   python examples/get_change_tracking.py MyTable 12345    # changes since version 12345
   ```

3. **Full sync** to CSV:

   ```bash
   python -m azsql_ct --config sync_config.json
   ```
