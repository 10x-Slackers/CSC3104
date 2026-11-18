"""Durable bank-local protocol state: the bank's PostgreSQL in the cluster (a
`postgresql://` URL, shared by the node's replicas), or SQLite for tests and local runs.
The same SQL runs on both.

- replies:  stored Trace replies, so a retried query_id gets the same answer
- searches: every search this bank started and its latest state
- chains:   one row per chain identity; the seed for Role 5's one-alert-per-bank rule
- outbox:   results still to deliver to other involved banks, until acknowledged
- case_outbox: messages still to post to this bank's case service (case.md)
"""

import json
import sqlite3
import threading
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path

import psycopg

from privacy_node import privacy_node_pb2 as pb

SCHEMA = """
CREATE TABLE IF NOT EXISTS replies (
  query_id TEXT, ref BYTEA, reply BYTEA, PRIMARY KEY (query_id, ref));
CREATE TABLE IF NOT EXISTS searches (
  search_id TEXT PRIMARY KEY, incoming TEXT, outgoing TEXT, state INTEGER,
  chain_digest TEXT);
CREATE TABLE IF NOT EXISTS chains (
  chain_digest TEXT PRIMARY KEY, state INTEGER, result BYTEA);
CREATE TABLE IF NOT EXISTS outbox (
  chain_digest TEXT, bank TEXT, result BYTEA, acked INTEGER DEFAULT 0,
  dead INTEGER DEFAULT 0,
  PRIMARY KEY (chain_digest, bank));
CREATE TABLE IF NOT EXISTS case_outbox (
  key TEXT, kind TEXT, message TEXT, acked INTEGER DEFAULT 0, dead INTEGER DEFAULT 0,
  PRIMARY KEY (key, kind));
"""
HOUSEKEEPING_LOCK, SCHEMA_LOCK = 3104, 3105  # advisory lock keys
UPSERT_CASE = "INSERT INTO case_outbox (key, kind, message) VALUES (?,?,?) ON CONFLICT (key, kind)"


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.url = str(path)
        self.pg = self.url.startswith(("postgresql://", "postgres://"))
        self.lock = threading.Lock()
        self.db = self._connect()

    def _connect(self):
        if self.pg:
            db = psycopg.connect(self.url, autocommit=True, connect_timeout=5)
            # one transaction, under a lock: replicas starting together can't race
            db.execute(f"SELECT pg_advisory_xact_lock({SCHEMA_LOCK});{SCHEMA}")
        else:
            db = sqlite3.connect(
                self.url, check_same_thread=False, isolation_level=None
            )
            db.executescript(SCHEMA)
        return db

    def _execute(self, sql, args=()):
        if self.pg and self.db.broken:  # PostgreSQL restarted: the next call reconnects
            self.db = self._connect()
        return self.db.execute(sql.replace("?", "%s") if self.pg else sql, args)

    def _run(self, sql, *args):
        with self.lock:
            cursor = self._execute(sql, args)
            return cursor.fetchall() if cursor.description else []

    @contextmanager
    def _transaction(self):
        with self.lock:
            self._execute("BEGIN")
            try:
                yield
            except BaseException:
                self._execute("ROLLBACK")
                raise
            self._execute("COMMIT")

    def try_lock(self) -> bool:
        """Take the housekeeping lock, so one replica retries and redelivers at a time.

        A session-level advisory lock: PostgreSQL releases it if the replica dies.
        """
        lock = "SELECT pg_try_advisory_lock(?)"
        return not self.pg or self._run(lock, HOUSEKEEPING_LOCK)[0][0]

    def unlock(self) -> None:
        if self.pg:
            self._run("SELECT pg_advisory_unlock(?)", HOUSEKEEPING_LOCK)

    def get_reply(self, query_id: str, ref: bytes) -> pb.TraceReply | None:
        rows = self._run(
            "SELECT reply FROM replies WHERE query_id=? AND ref=?", query_id, ref
        )
        return pb.TraceReply.FromString(rows[0][0]) if rows else None

    def put_reply(self, query_id: str, ref: bytes, reply: pb.TraceReply) -> None:
        self._run(
            "INSERT INTO replies VALUES (?,?,?) ON CONFLICT DO NOTHING",
            query_id,
            ref,
            reply.SerializeToString(),
        )

    def put_search(
        self,
        search_id: str,
        incoming: str,
        outgoing: str,
        state: pb.State,
        chain_digest: str,
    ) -> None:
        self._run(
            "INSERT INTO searches VALUES (?,?,?,?,?) ON CONFLICT (search_id) "
            "DO UPDATE SET state=excluded.state, chain_digest=excluded.chain_digest",
            search_id,
            incoming,
            outgoing,
            state,
            chain_digest,
        )

    def unfinished_searches(self) -> list[tuple[str, str]]:
        return self._run(
            "SELECT incoming, outgoing FROM searches WHERE state IN (?,?)",
            pb.PENDING,
            pb.TIMED_OUT,
        )

    def save_chain(
        self,
        result: pb.ChainResult,
        deliver_to: Iterable[str] = (),
        case: dict | None = None,
    ) -> None:
        """Insert a chain once; a later CONFIRMED result upgrades a PARTIAL one.

        The chain row, its outbox rows and its case message are written in one
        transaction, so a crash can't leave a stored result that is never delivered.
        """
        blob = result.SerializeToString()
        with self._transaction():
            changed = self._execute(
                "INSERT INTO chains VALUES (?,?,?) ON CONFLICT (chain_digest) DO "
                "UPDATE SET state=excluded.state, result=excluded.result "
                "WHERE chains.state != ? AND excluded.state = ?",
                (
                    result.chain_digest,
                    result.state,
                    blob,
                    pb.CONFIRMED,
                    pb.CONFIRMED,
                ),
            ).rowcount
            for bank in deliver_to if changed else ():
                self._execute(
                    "INSERT INTO outbox (chain_digest, bank, result) VALUES (?,?,?)"
                    " ON CONFLICT (chain_digest, bank)"
                    " DO UPDATE SET result=excluded.result, acked=0, dead=0",
                    (result.chain_digest, bank, blob),
                )
            if changed and case:
                self._execute(
                    UPSERT_CASE
                    + " DO UPDATE SET message=excluded.message, acked=0, dead=0",
                    (case["key"], case["kind"], json.dumps(case)),
                )

    def put_case(self, message: dict) -> None:
        """Queue a hit or pending message once; a repeat changes nothing."""
        self._run(
            UPSERT_CASE + " DO NOTHING",
            message["key"],
            message["kind"],
            json.dumps(message),
        )

    def pending_cases(self) -> list[tuple[str, str, str]]:
        return self._run(
            "SELECT key, kind, message FROM case_outbox WHERE acked=0 AND dead=0"
        )

    # Only if the row still holds what was sent: an upgrade may have replaced it.
    def ack_case(self, key: str, kind: str, message: str) -> None:
        self._run(
            "UPDATE case_outbox SET acked=1 WHERE key=? AND kind=? AND message=?",
            key,
            kind,
            message,
        )

    def dead_case(self, key: str, kind: str, message: str) -> None:
        self._run(
            "UPDATE case_outbox SET dead=1 WHERE key=? AND kind=? AND message=?",
            key,
            kind,
            message,
        )

    def chains(self) -> list[pb.ChainResult]:
        rows = self._run("SELECT result FROM chains ORDER BY chain_digest")
        return [pb.ChainResult.FromString(r[0]) for r in rows]

    def pending_deliveries(self) -> list[tuple[str, pb.ChainResult]]:
        rows = self._run("SELECT bank, result FROM outbox WHERE acked=0 AND dead=0")
        return [(bank, pb.ChainResult.FromString(blob)) for bank, blob in rows]

    # As for cases: only if another replica hasn't upgraded the row since it was sent.
    def ack(self, result: pb.ChainResult, bank: str) -> None:
        self._run(
            "UPDATE outbox SET acked=1 WHERE chain_digest=? AND bank=? AND result=?",
            result.chain_digest,
            bank,
            result.SerializeToString(),
        )

    def dead_letter(self, result: pb.ChainResult, bank: str) -> None:
        """Stop delivering a result the receiver rejected; kept for inspection."""
        self._run(
            "UPDATE outbox SET dead=1 WHERE chain_digest=? AND bank=? AND result=?",
            result.chain_digest,
            bank,
            result.SerializeToString(),
        )

    def dead_letters(self) -> list[tuple[str, str]]:
        return self._run("SELECT chain_digest, bank FROM outbox WHERE dead=1")
