"""Azure SQL connection helper.

Provides :class:`AzureSQLConnection`, a configurable connection wrapper with
context-manager support, and a :func:`get_connection` convenience function.

Configuration is resolved in order: explicit arguments > environment variables >
built-in defaults.  A ``.env`` file is loaded automatically (if present) via
:func:`load_dotenv`.

Env vars:
    SERVER          -- Azure SQL server FQDN
    DATABASE        -- Target database (default: database_1)
    ADMIN_USER      -- SQL login (default: sqladmin)
    ADMIN_PASSWORD  -- SQL login password (**required** unless passed explicitly)
    ODBC_DRIVER     -- ODBC driver name (default: ODBC Driver 18 for SQL Server)
"""

from __future__ import annotations

import logging
import os
from types import TracebackType
from typing import Optional, Type

import pyodbc

logger = logging.getLogger(__name__)

_DEFAULT_SERVER = "guanjiesqldb.database.windows.net"
_DEFAULT_DATABASE = "database_1"
_DEFAULT_USER = "sqladmin"
_DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"


_dotenv_loaded: set = set()


def load_dotenv(path: Optional[str] = None) -> None:
    """Read a simple key=value .env file into ``os.environ`` (no dependencies).

    Subsequent calls with the same resolved *path* are no-ops.
    """
    if path is None:
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    resolved = os.path.abspath(path)
    if resolved in _dotenv_loaded:
        return
    if not os.path.isfile(resolved):
        return
    with open(resolved) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))
    _dotenv_loaded.add(resolved)
    logger.debug("Loaded environment from %s", resolved)


class AzureSQLConnection:
    """Managed connection to an Azure SQL database.

    Usage as a context manager::

        with AzureSQLConnection(password="secret") as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")

    Or manually::

        az = AzureSQLConnection(password="secret")
        conn = az.connect()
        ...
        az.close()
    """

    def __init__(
        self,
        server: Optional[str] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        driver: Optional[str] = None,
        *,
        dotenv_path: Optional[str] = None,
    ) -> None:
        load_dotenv(dotenv_path)

        self.server = server or os.environ.get("SERVER", _DEFAULT_SERVER)
        self.database = database or os.environ.get("DATABASE", _DEFAULT_DATABASE)
        self.user = user or os.environ.get("ADMIN_USER", _DEFAULT_USER)
        self.driver = driver or os.environ.get("ODBC_DRIVER", _DEFAULT_DRIVER)
        self._password = password or os.environ.get("ADMIN_PASSWORD")
        self._conn: Optional[pyodbc.Connection] = None

    @property
    def connection_string(self) -> str:
        """Build the ODBC connection string (raises if no password)."""
        if not self._password:
            raise ValueError(
                "No password supplied. Set ADMIN_PASSWORD in your environment "
                "or .env file, or pass it to the constructor."
            )
        return (
            f"Driver={{{self.driver}}};Server={self.server};"
            f"Database={self.database};Uid={self.user};Pwd={self._password};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )

    def connect(self) -> pyodbc.Connection:
        """Open and return a ``pyodbc.Connection``.

        Subsequent calls return the same connection unless :meth:`close` has
        been called.
        """
        if self._conn is not None:
            return self._conn
        logger.debug("Connecting to %s/%s as %s", self.server, self.database, self.user)
        self._conn = pyodbc.connect(self.connection_string)
        return self._conn

    def close(self) -> None:
        """Close the underlying connection if open."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def test_connectivity(self) -> bool:
        """Run ``SELECT 1`` and return ``True`` on success, ``False`` on failure."""
        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            return True
        except Exception:
            return False

    def __enter__(self) -> pyodbc.Connection:
        return self.connect()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"AzureSQLConnection(server={self.server!r}, "
            f"database={self.database!r}, user={self.user!r})"
        )


def get_connection(
    server: Optional[str] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    driver: Optional[str] = None,
    *,
    dotenv_path: Optional[str] = None,
) -> pyodbc.Connection:
    """Convenience wrapper: create an :class:`AzureSQLConnection` and return
    the open ``pyodbc.Connection``.

    Parameters fall back to environment variables, then to sensible defaults.
    Raises ``ValueError`` if *password* cannot be resolved.
    """
    az = AzureSQLConnection(
        server=server,
        database=database,
        user=user,
        password=password,
        driver=driver,
        dotenv_path=dotenv_path,
    )
    return az.connect()
