"""Bank-local case store: verified search results, case identity and one alert per case.

Holds only tokens, bank IDs and claim metadata; never accounts, amounts or payment IDs.
Contract: docs/contracts/case.md. SQLite now; CaseStore is the seam for Postgres.
"""

import hashlib
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import astuple, dataclass, fields
from typing import Protocol

from apps.security.security import canonical_pair

# Weakest to strongest. A case takes the strongest state among its results, so a late
# partial or timed-out result never downgrades a confirmed case.
# ASSUMPTION: this ranking until protocol.md defines the states, replace when agreed.
STATES = ("invalid-claim", "no-match", "timed-out", "partial", "confirmed")
# Simulated analyst actions; none touches a real account. "report" queues the case
# evidence for the mock intake.
DECISIONS = ("monitor", "suspend", "freeze", "report")
REVIEWS = ("draft", "approved", "rejected")


@dataclass(frozen=True)
class Claim:
    """One bank's signed assertion that an incoming transfer moved on as an outgoing one.

    ASSUMPTION: claim fields until protocol.md defines the protobuf, replace when agreed.
    """

    signer: str  # bank that confirmed the link
    in_bank: str  # sender of the incoming transfer
    in_token: str
    out_bank: str  # receiver of the outgoing transfer
    out_token: str
    query_id: str
    key_version: str  # signer's signing-key version
    signature: str  # hex


@dataclass(frozen=True)
class SearchResult:
    """A search result delivered to this bank.

    ASSUMPTION: result fields until protocol.md defines result delivery, replace when agreed.
    """

    query_id: str
    status: str  # one of STATES
    claims: tuple[Claim, ...] = ()

    def __post_init__(self):
        if self.status not in STATES:
            raise ValueError(f"Unknown result status: {self.status}")


@dataclass(frozen=True)
class AlertRule:
    """When a case is complete enough to alert. Links are counted as transfers.

    ASSUMPTION: a confirmed case with >= 2 transfers across >= 2 banks, replace when agreed.
    """

    min_transfers: int = int(os.environ.get("CASE_MIN_TRANSFERS", "2"))
    min_banks: int = int(os.environ.get("CASE_MIN_BANKS", "2"))


class CaseStore(Protocol):
    def record(self, result: SearchResult) -> str | None: ...
    def get(self, case_id: str) -> dict | None: ...
    def cases(self) -> list[dict]: ...
    def lease_due(self, now, lease, max_attempts) -> list[dict]: ...
    def ack(self, event_id: str) -> None: ...
    def retry(self, event_id: str, at: float, error: str) -> None: ...
    def decide(self, case_id: str, action: str) -> None: ...
    def receive(self, message: dict) -> bool: ...


def transfer_key(bank_x, bank_y, token):
    """Tokens are only comparable within one bank pair, so the pair is part of the key."""
    return "-".join(canonical_pair(bank_x, bank_y)) + ":" + token


def transfers(claim):
    return [
        transfer_key(claim.in_bank, claim.signer, claim.in_token),
        transfer_key(claim.signer, claim.out_bank, claim.out_token),
    ]


def chain(claims):
    """Order claims along the chain: each claim's outgoing token is the next's incoming."""
    by_in = {c.in_token: c for c in claims}
    outs = {c.out_token for c in claims}
    starts = [c for c in claims if c.in_token not in outs] or claims[:1]  # loop
    ordered = []
    for claim in starts:
        while claim is not None and claim not in ordered:
            ordered.append(claim)
            claim = by_in.get(claim.out_token)
    return ordered + [c for c in claims if c not in ordered]


def derive_case_id(keys):
    """SHA-256 of the sorted transfer keys first seen for a case, as compact JSON."""
    return hashlib.sha256(
        json.dumps(sorted(set(keys)), separators=(",", ":")).encode()
    ).hexdigest()


CLAIM_COLUMNS = ", ".join(f.name for f in fields(Claim))
SCHEMA = f"""
CREATE TABLE IF NOT EXISTS cases (
    case_id TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    state TEXT NOT NULL,
    merged_into TEXT
);
-- Each transfer belongs to one case: a result sharing any transfer joins that case.
CREATE TABLE IF NOT EXISTS transfers (
    transfer TEXT PRIMARY KEY,
    case_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS claims (
    {CLAIM_COLUMNS.replace(", ", " TEXT NOT NULL, ")} TEXT NOT NULL,
    case_id TEXT NOT NULL,
    PRIMARY KEY (signer, in_token, out_token)
);
-- Every delivered result, once per (query, status); case_id is NULL without claims.
CREATE TABLE IF NOT EXISTS results (
    query_id TEXT NOT NULL,
    status TEXT NOT NULL,
    case_id TEXT,
    received_at REAL NOT NULL,
    PRIMARY KEY (query_id, status)
);
-- The durable uniqueness rule: at most one alert per case at this bank.
CREATE TABLE IF NOT EXISTS alerts (
    case_id TEXT PRIMARY KEY,
    raised_at REAL NOT NULL
);
-- Transactional outbox: written with the change it announces, delivered at least once.
-- event_id is "<kind>:<case_id>", so receivers deduplicate redeliveries on it.
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    case_id TEXT NOT NULL,
    created_at REAL NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_at REAL NOT NULL,
    acked_at REAL,
    error TEXT
);
-- Analyst decisions, append-only. No free text, so nobody can type an account in.
CREATE TABLE IF NOT EXISTS decisions (
    case_id TEXT NOT NULL,
    action TEXT NOT NULL,
    decided_at REAL NOT NULL
);
-- The analyst's inbox: deduplicates redelivered events, so each shows once.
CREATE TABLE IF NOT EXISTS notifications (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    case_id TEXT NOT NULL,
    received_at REAL NOT NULL
);
-- Generated reports: drafts until an analyst approves or rejects them.
CREATE TABLE IF NOT EXISTS reports (
    report_id INTEGER PRIMARY KEY,
    case_id TEXT NOT NULL,
    source TEXT NOT NULL,
    body TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    created_at REAL NOT NULL,
    reviewed_at REAL
);
"""


def notify(db, kind, case_id):
    """Queue one event per (kind, case); repeats are ignored."""
    now = time.time()
    db.execute(
        "INSERT INTO outbox (event_id, kind, case_id, created_at, next_at)"
        " VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        (f"{kind}:{case_id}", kind, case_id, now, now),
    )


class SqliteCaseStore:
    def __init__(self, path, rule=None):
        self.rule = rule or AlertRule()
        self.db = sqlite3.connect(path, isolation_level=None, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)

    @contextmanager
    def _write(self):
        # IMMEDIATE takes the write lock before the reads, so concurrent writers serialise.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")

    def record(self, result):
        """Store a result whose claims are already verified; return its case ID."""
        keys = sorted({key for claim in result.claims for key in transfers(claim)})
        with self._write() as db:
            case_id = self._case_for(db, keys) if keys else None
            db.execute(
                "INSERT INTO results VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (result.query_id, result.status, case_id, time.time()),
            )
            if case_id is None:
                return None
            db.executemany(
                "INSERT INTO transfers VALUES (?, ?) ON CONFLICT DO NOTHING",
                [(key, case_id) for key in keys],
            )
            db.executemany(
                f"INSERT INTO claims ({CLAIM_COLUMNS}, case_id)"
                f" VALUES ({', '.join('?' * (len(fields(Claim)) + 1))})"
                " ON CONFLICT DO NOTHING",
                [astuple(claim) + (case_id,) for claim in result.claims],
            )
            statuses = db.execute(
                "SELECT status FROM results WHERE case_id = ?", (case_id,)
            )
            state = max(
                [result.status, *(row[0] for row in statuses)], key=STATES.index
            )
            db.execute("UPDATE cases SET state = ? WHERE case_id = ?", (state, case_id))
            count = db.execute(
                "SELECT COUNT(*) FROM transfers WHERE case_id = ?", (case_id,)
            ).fetchone()[0]
            banks = {
                bank
                for row in db.execute(
                    "SELECT signer, in_bank, out_bank FROM claims WHERE case_id = ?",
                    (case_id,),
                )
                for bank in row
            }
            if (
                state == "confirmed"
                and count >= self.rule.min_transfers
                and len(banks) >= self.rule.min_banks
            ):
                raised = db.execute(
                    "INSERT INTO alerts VALUES (?, ?) ON CONFLICT DO NOTHING",
                    (case_id, time.time()),
                )
                if raised.rowcount:
                    notify(db, "alert", case_id)
        return case_id

    def _case_for(self, db, keys):
        """Find the case these transfers join, merging cases a result bridges."""
        found = [
            row[0]
            for row in db.execute(
                "SELECT DISTINCT case_id FROM transfers JOIN cases USING (case_id)"
                f" WHERE transfer IN ({', '.join('?' * len(keys))})"
                " ORDER BY created_at, case_id",
                keys,
            )
        ]
        if not found:
            case_id = derive_case_id(keys)
            db.execute(
                "INSERT INTO cases (case_id, created_at, state) VALUES (?, ?, ?)",
                (case_id, time.time(), STATES[0]),
            )
            return case_id
        # Merge into the oldest case, keeping its alert or adopting a merged one.
        case_id = found[0]
        for old in found[1:]:
            # Tell the analyst an alerted case now lives on under the surviving case.
            if db.execute("SELECT 1 FROM alerts WHERE case_id = ?", (old,)).fetchone():
                notify(db, "merged", old)
            for table in ("transfers", "claims", "results"):
                db.execute(
                    f"UPDATE {table} SET case_id = ? WHERE case_id = ?", (case_id, old)
                )
            db.execute(
                "UPDATE alerts SET case_id = ? WHERE case_id = ? AND NOT EXISTS"
                " (SELECT 1 FROM alerts WHERE case_id = ?)",
                (case_id, old, case_id),
            )
            db.execute("DELETE FROM alerts WHERE case_id = ?", (old,))
            db.execute(
                "UPDATE cases SET merged_into = ? WHERE case_id = ?", (case_id, old)
            )
        return case_id

    def get(self, case_id):
        row = self.db.execute(
            "SELECT * FROM cases LEFT JOIN alerts USING (case_id) WHERE case_id = ?",
            (case_id,),
        ).fetchone()
        if row is None:
            return None
        claims = self.db.execute(
            f"SELECT {CLAIM_COLUMNS} FROM claims WHERE case_id = ?"
            " ORDER BY signer, in_token, out_token",
            (case_id,),
        )
        results = self.db.execute(
            "SELECT query_id, status FROM results WHERE case_id = ?"
            " ORDER BY received_at, query_id, status",
            (case_id,),
        )
        decisions = self.db.execute(
            "SELECT action, decided_at FROM decisions WHERE case_id = ?"
            " ORDER BY decided_at",
            (case_id,),
        )
        return dict(row) | {
            "claims": [Claim(*claim) for claim in claims],
            "results": [tuple(result) for result in results],
            "decisions": [tuple(decision) for decision in decisions],
        }

    def decide(self, case_id, action):
        if action not in DECISIONS:
            raise ValueError(f"Unknown decision: {action}")
        with self._write() as db:
            db.execute(
                "INSERT INTO decisions VALUES (?, ?, ?)", (case_id, action, time.time())
            )
            if action == "report":
                notify(db, "report", case_id)

    def save_report(self, case_id, source, body):
        """Store a generated report as a draft pending analyst review; return its ID."""
        return self.db.execute(
            "INSERT INTO reports (case_id, source, body, created_at) VALUES (?, ?, ?, ?)",
            (case_id, source, body, time.time()),
        ).lastrowid

    def review_report(self, report_id, status):
        """Approve or reject a draft once; return False if it was already reviewed."""
        if status not in REVIEWS[1:]:
            raise ValueError(f"Unknown review: {status}")
        return bool(
            self.db.execute(
                "UPDATE reports SET status = ?, reviewed_at = ?"
                " WHERE report_id = ? AND status = 'draft'",
                (status, time.time(), report_id),
            ).rowcount
        )

    def reports(self, case_id):
        return [
            dict(row)
            for row in self.db.execute(
                "SELECT * FROM reports WHERE case_id = ? ORDER BY report_id DESC",
                (case_id,),
            )
        ]

    def receive(self, message):
        """Record a delivered event once; return False for a redelivery."""
        return bool(
            self.db.execute(
                "INSERT INTO notifications VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (message["event_id"], message["kind"], message["case_id"], time.time()),
            ).rowcount
        )

    def notifications(self, limit=20):
        return [
            dict(row)
            for row in self.db.execute(
                "SELECT * FROM notifications ORDER BY received_at DESC LIMIT ?",
                (limit,),
            )
        ]

    def lease_due(self, now, lease, max_attempts, limit=100):
        """Take due, unacked events; the lease stops other dispatchers resending them."""
        with self._write() as db:
            events = [
                dict(row)
                for row in db.execute(
                    "SELECT event_id, kind, case_id, attempts FROM outbox"
                    " WHERE acked_at IS NULL AND attempts < ? AND next_at <= ?"
                    " ORDER BY created_at, event_id LIMIT ?",
                    (max_attempts, now, limit),
                )
            ]
            db.executemany(
                "UPDATE outbox SET attempts = attempts + 1, next_at = ?"
                " WHERE event_id = ?",
                [(now + lease, event["event_id"]) for event in events],
            )
        return events

    def ack(self, event_id):
        """Idempotent: the first acknowledgement time is kept."""
        self.db.execute(
            "UPDATE outbox SET acked_at = COALESCE(acked_at, ?), error = NULL"
            " WHERE event_id = ?",
            (time.time(), event_id),
        )

    def retry(self, event_id, at, error):
        self.db.execute(
            "UPDATE outbox SET next_at = ?, error = ?"
            " WHERE event_id = ? AND acked_at IS NULL",
            (at, error, event_id),
        )

    def outbox(self):
        return [
            dict(row)
            for row in self.db.execute("SELECT * FROM outbox ORDER BY created_at")
        ]

    def cases(self):
        """Live (unmerged) cases, oldest first; raised_at is None until alerted."""
        return [
            dict(row)
            for row in self.db.execute(
                "SELECT case_id, created_at, state, raised_at FROM cases"
                " LEFT JOIN alerts USING (case_id) WHERE merged_into IS NULL"
                " ORDER BY created_at, case_id"
            )
        ]
