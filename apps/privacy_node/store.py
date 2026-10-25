"""Durable bank-local protocol state (SQLite for M2; same SQL moves to PostgreSQL for M3).

- replies:  stored Trace replies, so a retried query_id gets the same answer
- searches: every search this bank started and its latest state
- chains:   one row per chain identity; the seed for Role 5's one-alert-per-bank rule
- outbox:   results still to deliver to other involved banks, until acknowledged
"""

import sqlite3
import threading

from apps.privacy_node import privacy_node_pb2 as pb

SCHEMA = """
CREATE TABLE IF NOT EXISTS replies (
  query_id TEXT, ref TEXT, reply BLOB, PRIMARY KEY (query_id, ref));
CREATE TABLE IF NOT EXISTS searches (
  search_id TEXT PRIMARY KEY, incoming TEXT, outgoing TEXT, state INTEGER,
  chain_digest TEXT);
CREATE TABLE IF NOT EXISTS chains (
  chain_digest TEXT PRIMARY KEY, state INTEGER, result BLOB);
CREATE TABLE IF NOT EXISTS outbox (
  chain_digest TEXT, bank TEXT, result BLOB, acked INTEGER DEFAULT 0,
  PRIMARY KEY (chain_digest, bank));
"""


class Store:
    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.executescript(SCHEMA)
        self.lock = threading.Lock()

    def _run(self, sql, *args):
        with self.lock:
            return self.db.execute(sql, args).fetchall()

    def get_reply(self, query_id, ref):
        rows = self._run(
            "SELECT reply FROM replies WHERE query_id=? AND ref=?", query_id, ref
        )
        return pb.TraceReply.FromString(rows[0][0]) if rows else None

    def put_reply(self, query_id, ref, reply):
        self._run(
            "INSERT OR IGNORE INTO replies VALUES (?,?,?)",
            query_id,
            ref,
            reply.SerializeToString(),
        )

    def put_search(self, search_id, incoming, outgoing, state, chain_digest=""):
        self._run(
            "INSERT INTO searches VALUES (?,?,?,?,?) ON CONFLICT (search_id) "
            "DO UPDATE SET state=excluded.state, chain_digest=excluded.chain_digest",
            search_id,
            incoming,
            outgoing,
            state,
            chain_digest,
        )

    def unfinished_searches(self):
        return self._run(
            "SELECT incoming, outgoing FROM searches WHERE state IN (?,?)",
            pb.PENDING,
            pb.TIMED_OUT,
        )

    def save_chain(self, result, deliver_to=()):
        """Insert a chain once; a later CONFIRMED result upgrades a PARTIAL one.

        The chain row and its outbox rows are written in one transaction, so a
        crash can't leave a stored result that is never delivered.
        Returns True if the row is new or upgraded.
        """
        blob = result.SerializeToString()
        with self.lock, self.db:
            self.db.execute("BEGIN")
            changed = self.db.execute(
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
                self.db.execute(
                    "INSERT INTO outbox VALUES (?,?,?,0) ON CONFLICT (chain_digest, bank)"
                    " DO UPDATE SET result=excluded.result, acked=0",
                    (result.chain_digest, bank, blob),
                )
        return changed == 1

    def chains(self):
        rows = self._run("SELECT result FROM chains ORDER BY chain_digest")
        return [pb.ChainResult.FromString(r[0]) for r in rows]

    def pending_deliveries(self):
        rows = self._run("SELECT bank, result FROM outbox WHERE acked=0")
        return [(bank, pb.ChainResult.FromString(blob)) for bank, blob in rows]

    def ack(self, chain_digest, bank):
        self._run(
            "UPDATE outbox SET acked=1 WHERE chain_digest=? AND bank=?",
            chain_digest,
            bank,
        )
