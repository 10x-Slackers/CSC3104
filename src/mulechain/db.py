"""One small SQL layer for the node's store and the case store.

PostgreSQL in the cluster (a `postgresql://` URL), SQLite in tests and the offline
evaluator. The same SQL runs on both: `?` placeholders, rows as dicts.

The advisory-lock pattern (docs/patterns.md) starts here: `try_lock` lets one of
several replicas do a job, and `transaction(lock=...)` makes a check-then-insert
safe between writers. Both are PostgreSQL advisory locks, and no-ops on SQLite.
"""

import sqlite3
import threading
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row


class Db:
    def __init__(self, url: str, schema: str) -> None:
        self.url, self.postgres = url, url.startswith("postgres")
        self.lock = threading.RLock()  # one connection, so one statement at a time
        self._connect()
        if self.postgres:
            # One transaction under a lock, so replicas starting together can't collide.
            self.conn.execute(f"SELECT pg_advisory_xact_lock(1); {schema}")
        else:
            self.conn.executescript(schema)

    def _connect(self) -> None:
        if self.postgres:
            self.conn = psycopg.connect(
                self.url, autocommit=True, row_factory=dict_row, connect_timeout=5
            )
        else:
            self.conn = sqlite3.connect(self.url, check_same_thread=False, isolation_level=None)
            self.conn.row_factory = lambda cursor, row: {
                column[0]: value for column, value in zip(cursor.description, row, strict=True)
            }

    def _execute(self, sql: str, args: tuple):
        if self.postgres and self.conn.broken:  # PostgreSQL restarted or failed over
            self._connect()
        return self.conn.execute(sql.replace("?", "%s") if self.postgres else sql, args)

    def all(self, sql: str, *args) -> list[dict]:
        with self.lock:
            return self._execute(sql, args).fetchall()

    def run(self, sql: str, *args) -> int:
        """Run a write and return how many rows it changed."""
        with self.lock:
            return self._execute(sql, args).rowcount

    @contextmanager
    def transaction(self, lock: int | None = None):
        """The writes inside commit together or not at all.

        With `lock`, the transaction also holds that PostgreSQL advisory lock until it
        ends, so the same transaction in another replica waits its turn.
        """
        with self.lock:
            self._execute("BEGIN", ())
            try:
                if lock and self.postgres:
                    self._execute("SELECT pg_advisory_xact_lock(?)", (lock,))
                yield
            except BaseException:
                self._execute("ROLLBACK", ())
                raise
            self._execute("COMMIT", ())

    def try_lock(self, key: int) -> bool:
        """Take a PostgreSQL advisory lock if it is free.

        Session-level: PostgreSQL releases it if this process dies, so there is no lease
        to time out.
        """
        return not self.postgres or self.all("SELECT pg_try_advisory_lock(?) AS ok", key)[0]["ok"]

    def unlock(self, key: int) -> None:
        if self.postgres:
            self.all("SELECT pg_advisory_unlock(?)", key)
