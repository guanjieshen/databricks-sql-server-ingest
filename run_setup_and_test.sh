#!/usr/bin/env bash
# 1) Connect check, 2) Run setup scripts as sqladmin, 3) Validation test as test.
# Use from a machine that can reach the server (IP allowed in Azure SQL firewall).
# Optionally copy .env.example to .env and set ADMIN_PASSWORD, TEST_PASSWORD.
# Uses conda env "mssql-python" for Python (override with PYTHON_CMD).

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Use mssql-python conda env for Python (primary); fall back to system python if env missing
if conda run -n mssql-python python -c "import sys; sys.exit(0)" 2>/dev/null; then
  PYTHON_CMD="${PYTHON_CMD:-conda run -n mssql-python python}"
  PIP_CMD="${PIP_CMD:-conda run -n mssql-python pip}"
else
  PYTHON_CMD="${PYTHON_CMD:-python3}"
  PIP_CMD="${PIP_CMD:-pip3}"
fi

# Load .env if present
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

SERVER="${SERVER:-guanjiesqldb.database.windows.net}"
ADMIN_USER="${ADMIN_USER:-sqladmin}"
TEST_USER="${TEST_USER:-test}"

if [ -z "$ADMIN_PASSWORD" ]; then
  echo "ERROR: ADMIN_PASSWORD is not set. Export it or add it to .env." >&2
  exit 1
fi
if [ -z "$TEST_PASSWORD" ]; then
  echo "ERROR: TEST_PASSWORD is not set. Export it or add it to .env." >&2
  exit 1
fi

export SERVER ADMIN_USER ADMIN_PASSWORD TEST_PASSWORD TEST_USER

echo "=== 0. Connect check ==="
$PYTHON_CMD examples/connect.py

echo "=== 1. Master: create login test, user, view, grant ==="
sqlcmd -S "$SERVER" -d master -U "$ADMIN_USER" -P "$ADMIN_PASSWORD" -i create-login-master.sql -C

echo "=== 2. database_1: create user test, db_datareader ==="
sqlcmd -S "$SERVER" -d database_1 -U "$ADMIN_USER" -P "$ADMIN_PASSWORD" -i create-user-database_1.sql -C

echo "=== 3. Validation test (as test) ==="
$PIP_CMD install -q -r requirements.txt
$PYTHON_CMD -m pytest tests/test_integration.py -m integration -v

echo "Done: privileges are correct."
