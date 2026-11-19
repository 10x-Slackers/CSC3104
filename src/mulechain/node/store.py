"""The privacy node's durable state, shared by its replicas (PostgreSQL in the cluster).

    searches   every search this bank started from a rule hit, and when to try it next
    chains     every verified chain this bank is part of, with its signed claims
    outbox     messages still owed: chains to peer banks, case messages to this bank's
               case service
    peer_keys  the public keys each peer last published
    hit_cursor how far into the scorer's stream of hits this bank has read

Two patterns live here (docs/patterns.md):

- Transactional outbox. A chain and the messages it owes are written in one
  transaction, then delivered until acknowledged. A crash between "stored" and
  "sent" can't lose a message, only send it twice, and receivers ignore repeats.
- Capped exponential backoff with full jitter. A search or a delivery that got no
  answer waits random(0, min(300 s, 10 s x 2^attempts)), so a bank that comes back
  isn't hit by every waiting message at once. Searches stop being retried after 24 h.
"""

import hashlib
import json
import random
import time

from mulechain.db import Db

SCHEMA = """
CREATE TABLE IF NOT EXISTS searches (
  id TEXT PRIMARY KEY, incoming TEXT, outgoing TEXT, state TEXT DEFAULT 'pending',
  attempts INTEGER DEFAULT 0, next_at DOUBLE PRECISION DEFAULT 0, started DOUBLE PRECISION);
CREATE INDEX IF NOT EXISTS searches_due ON searches (state, next_at);
CREATE TABLE IF NOT EXISTS chains (
  digest TEXT PRIMARY KEY, state TEXT, chain BYTEA);
CREATE TABLE IF NOT EXISTS outbox (
  dest TEXT, key TEXT, body BYTEA, state TEXT DEFAULT 'queued',
  attempts INTEGER DEFAULT 0, next_at DOUBLE PRECISION DEFAULT 0, queued_at DOUBLE PRECISION,
  PRIMARY KEY (dest, key));
CREATE INDEX IF NOT EXISTS outbox_due ON outbox (state, next_at);
CREATE TABLE IF NOT EXISTS peer_keys (bank TEXT PRIMARY KEY, keys TEXT);
CREATE TABLE IF NOT EXISTS hit_cursor (stream TEXT PRIMARY KEY, id TEXT);
"""
BACKOFF_BASE, BACKOFF_CAP, EXPIRY = 10, 300, 24 * 3600
HOUSEKEEPING_LOCK = 3104  # advisory lock key: one replica works the tables at a time
CASE = "case"  # the outbox destination that isn't a peer bank


def backoff(attempts: int) -> float:
    """Seconds to wait before attempt number `attempts + 1`."""
    return random.uniform(0, min(BACKOFF_CAP, BACKOFF_BASE * 2**attempts))


class Store:
    def __init__(self, url: str = ":memory:") -> None:
        self.db = Db(url, SCHEMA)

    # ---- searches

    def add_search(self, incoming: str, outgoing: str) -> None:
        """Queue a search from a local link once; a repeated hit changes nothing."""
        search_id = hashlib.sha256(f"{incoming}:{outgoing}".encode()).hexdigest()
        self.db.run(
            "INSERT INTO searches (id, incoming, outgoing, started) VALUES (?,?,?,?)"
            " ON CONFLICT DO NOTHING",
            *(search_id, incoming, outgoing, time.time()),
        )

    def due_searches(self, limit: int = 64) -> list[dict]:
        now = time.time()
        return self.db.all(
            "SELECT * FROM searches WHERE state = 'pending' AND next_at <= ? AND started > ?"
            " ORDER BY next_at LIMIT ?",
            *(now, now - EXPIRY, limit),
        )

    def finish_search(self, search: dict, state: str) -> None:
        """Record an attempt's outcome; `pending` means try again after a backoff."""
        self.db.run(
            "UPDATE searches SET state = ?, attempts = attempts + 1, next_at = ? WHERE id = ?",
            *(state, time.time() + backoff(search["attempts"]), search["id"]),
        )

    # ---- chains and the outbox

    def save_chain(
        self, digest: str, state: str, chain: bytes, deliver_to: list[str], case: dict
    ) -> None:
        """Store a chain once, with everything it owes, in one transaction."""
        with self.db.transaction():
            new = self.db.run(
                "INSERT INTO chains VALUES (?,?,?) ON CONFLICT DO NOTHING", digest, state, chain
            )
            if new:
                for bank in deliver_to:
                    self.queue(bank, digest, chain)
                self.queue_case(case)

    def queue(self, dest: str, key: str, body: bytes) -> None:
        """Owe `dest` a message; queuing the same one again changes nothing."""
        self.db.run(
            "INSERT INTO outbox (dest, key, body, queued_at) VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
            *(dest, key, body, time.time()),
        )

    def queue_case(self, message: dict) -> None:
        self.queue(CASE, f"{message['kind']}:{message['key']}", json.dumps(message).encode())

    def due_messages(self, limit: int = 500) -> list[dict]:
        return self.db.all(
            "SELECT dest, key, body, attempts FROM outbox WHERE state = 'queued' AND next_at <= ?"
            " ORDER BY queued_at LIMIT ?",
            *(time.time(), limit),
        )

    def settle(self, message: dict, outcome: str) -> None:
        """`sent`: acknowledged. `dead`: the receiver refused it, so stop. `queued`: retry later."""
        self.db.run(
            "UPDATE outbox SET state = ?, attempts = attempts + 1, next_at = ?"
            " WHERE dest = ? AND key = ?",
            *(outcome, time.time() + backoff(message["attempts"])),
            *(message["dest"], message["key"]),
        )

    # ---- peers' public keys, and the hit stream cursor

    def peer_keys(self) -> dict[str, str]:
        return {row["bank"]: row["keys"] for row in self.db.all("SELECT * FROM peer_keys")}

    def put_peer_keys(self, bank: str, keys: str) -> None:
        self.db.run(
            "INSERT INTO peer_keys VALUES (?,?) ON CONFLICT (bank) DO UPDATE SET keys = excluded.keys",
            *(bank, keys),
        )

    def cursor(self) -> str:
        rows = self.db.all("SELECT id FROM hit_cursor WHERE stream = 'hits'")
        return rows[0]["id"] if rows else "0"

    def set_cursor(self, entry_id: str) -> None:
        self.db.run(
            "INSERT INTO hit_cursor VALUES ('hits', ?) ON CONFLICT (stream) DO UPDATE SET id = excluded.id",
            entry_id,
        )
