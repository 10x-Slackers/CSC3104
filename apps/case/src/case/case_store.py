"""Bank-local case store: verified chain results, case identity and one alert per case.

Holds only tokens, bank IDs and claim metadata; never accounts, amounts or payment IDs.
Input is Role 3's ChainResult (privacy_node/privacy_node.proto), already verified
with claims.check_result. Contract: docs/contracts/case.md. SQLite for now.
"""

import os
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import astuple, dataclass, fields
from typing import Literal, Self, TypedDict, get_args

from privacy_node import privacy_node_pb2 as pb

# Only confirmed and partial results become chains and are delivered (protocol.md).
# A case is confirmed once any of its results is; nothing downgrades it.
STATES = {pb.PARTIAL: "partial", pb.CONFIRMED: "confirmed"}
# Simulated analyst actions; none touches a real account. "report" queues the case
# evidence for the mock intake.
Decision = Literal["monitor", "suspend", "freeze", "report"]
DECISIONS = get_args(Decision)
CaseState = Literal["partial", "confirmed"]
ReviewStatus = Literal["approved", "rejected"]


@dataclass(frozen=True)
class Claim:
    """A stored LinkClaim: "signer holds in_ref and out_ref and its rule links them".

    An empty in or out token means the chain starts or ends at the signer.
    """

    signer: str
    recipient: str
    query_id: str
    key_version: str
    signing_key_version: str
    truncated: bool
    in_pair: str
    in_token: str
    out_pair: str
    out_token: str
    signature: str  # hex

    def __post_init__(self):
        object.__setattr__(self, "truncated", bool(self.truncated))

    @classmethod
    def from_pb(cls, c: pb.LinkClaim) -> Self:
        return cls(
            c.signer, c.recipient, c.query_id, c.key_version, c.signing_key_version, c.truncated, c.in_ref.pair,
            c.in_ref.token.hex(), c.out_ref.pair, c.out_ref.token.hex(), c.signature.hex(),
        )  # fmt: skip


@dataclass(frozen=True)
class AlertRule:
    """When a confirmed case is complete enough to alert. Links are counted as transfers.

    DEPENDS ON Role 2 (streaming and scoring): the minimum chain length, still TODO in
    streaming.md; until then >= 2 transfers across >= 2 banks. Status: pending.
    """

    min_transfers: int = int(os.environ.get("CASE_MIN_TRANSFERS", "2"))
    min_banks: int = int(os.environ.get("CASE_MIN_BANKS", "2"))


class Case(TypedDict):
    case_id: str
    created_at: float
    state: CaseState
    merged_into: str | None
    raised_at: float | None
    claims: list[Claim]
    results: list[tuple[str, str, CaseState]]  # (chain_digest, sender, state)
    decisions: list[tuple[Decision, float]]  # (action, decided_at)


class CaseSummary(TypedDict):
    case_id: str
    created_at: float
    state: CaseState
    raised_at: float | None
    decision: Decision | None


class Report(TypedDict):
    report_id: int
    case_id: str
    source: str
    body: str
    status: Literal["draft"] | ReviewStatus
    created_at: float
    reviewed_at: float | None


def transfers(claim: Claim) -> list[tuple[str, str]]:
    """Case transfer references: (pair, stable token hex)."""
    refs = ((claim.in_pair, claim.in_token), (claim.out_pair, claim.out_token))
    return [(pair, token) for pair, token in refs if token]


def banks(claim: Claim) -> set[str]:
    return {claim.signer, *claim.in_pair.split("-"), *claim.out_pair.split("-")} - {""}


def neighbours(signer: str, in_pair: str, out_pair: str) -> tuple[str, str]:
    """Banks at the far end of the incoming and outgoing transfers; start/end if none."""

    def other(pair, none):
        return next((b for b in pair.split("-") if b != signer), none) if pair else none

    return other(in_pair, "start"), other(out_pair, "end")


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
    key_version TEXT NOT NULL,
    pair TEXT NOT NULL,
    token TEXT NOT NULL,
    case_id TEXT NOT NULL,
    PRIMARY KEY (key_version, pair, token)
);
CREATE TABLE IF NOT EXISTS claims (
    {", ".join(f.name + (" INTEGER" if f.name == "truncated" else " TEXT") + " NOT NULL" for f in fields(Claim))},
    case_id TEXT NOT NULL,
    PRIMARY KEY (signer, key_version, in_pair, in_token, out_pair, out_token)
);
-- Every delivered result, once per (digest, sending bank, state).
CREATE TABLE IF NOT EXISTS results (
    chain_digest TEXT NOT NULL,
    sender TEXT NOT NULL,
    state TEXT NOT NULL,
    case_id TEXT NOT NULL,
    received_at REAL NOT NULL,
    PRIMARY KEY (chain_digest, sender, state)
);
-- The durable uniqueness rule: at most one alert per case at this bank.
CREATE TABLE IF NOT EXISTS alerts (
    case_id TEXT PRIMARY KEY,
    raised_at REAL NOT NULL
);
-- Transactional outbox for reports to the mock intake, written with the decision and
-- delivered at least once. event_id is "report:<case_id>:<decision>", so the intake
-- deduplicates redeliveries on it while a later report decision still sends.
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
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
-- The analyst's inbox, written in the same transaction as the alert or merge it
-- announces; event_id "<kind>:<case_id>" keeps each one to a single row.
CREATE TABLE IF NOT EXISTS notifications (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    case_id TEXT NOT NULL,
    created_at REAL NOT NULL
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


def notify(db: sqlite3.Connection, kind: str, case_id: str) -> None:
    """Add one inbox entry per (kind, case); repeats are ignored."""
    db.execute(
        "INSERT INTO notifications VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
        (f"{kind}:{case_id}", kind, case_id, time.time()),
    )


class SqliteCaseStore:
    def __init__(
        self, path: str | os.PathLike[str], rule: AlertRule | None = None
    ) -> None:
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

    def record(self, result: pb.ChainResult) -> str:
        """Store a verified ChainResult; return its case ID."""
        if result.state not in STATES:
            raise ValueError(f"Not a deliverable result: {pb.State.Name(result.state)}")
        claims = [Claim.from_pb(c) for c in result.claims]
        keys = sorted(
            {(claim.key_version, *key) for claim in claims for key in transfers(claim)}
        )
        with self._write() as db:
            case_id = self._case_for(db, keys, result.chain_digest)
            row = (result.chain_digest, result.sender, STATES[result.state], case_id)
            db.execute(
                "INSERT INTO results VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (*row, time.time()),
            )
            db.executemany(
                "INSERT INTO transfers VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                [(*key, case_id) for key in keys],
            )
            db.executemany(
                f"INSERT INTO claims ({CLAIM_COLUMNS}, case_id)"
                f" VALUES ({', '.join('?' * (len(fields(Claim)) + 1))})"
                " ON CONFLICT DO NOTHING",
                [astuple(claim) + (case_id,) for claim in claims],
            )
            confirmed = db.execute(
                "SELECT 1 FROM results WHERE case_id = ? AND state = 'confirmed'",
                (case_id,),
            ).fetchone()
            state = "confirmed" if confirmed else "partial"
            db.execute("UPDATE cases SET state = ? WHERE case_id = ?", (state, case_id))
            count = db.execute(
                "SELECT COUNT(*) FROM transfers WHERE case_id = ?", (case_id,)
            ).fetchone()[0]
            involved = {
                bank
                for row in db.execute(
                    f"SELECT {CLAIM_COLUMNS} FROM claims WHERE case_id = ?", (case_id,)
                )
                for bank in banks(Claim(*row))
            }
            if (
                state == "confirmed"
                and count >= self.rule.min_transfers
                and len(involved) >= self.rule.min_banks
            ):
                raised = db.execute(
                    "INSERT INTO alerts VALUES (?, ?) ON CONFLICT DO NOTHING",
                    (case_id, time.time()),
                )
                if raised.rowcount:
                    notify(db, "alert", case_id)
        return case_id

    def _case_for(self, db, keys, digest):
        """Find the case these transfers join, merging cases a result bridges.

        A new case takes Role 3's chain_digest as its ID. Overlapping partial results
        (e.g. over-limit v1, cut at different windows) share transfers, so they join
        one case even though their digests differ.
        """
        found = [
            row[0]
            for row in db.execute(
                "SELECT DISTINCT case_id FROM transfers JOIN cases USING (case_id)"
                f" WHERE (key_version, pair, token) IN ({', '.join('(?, ?, ?)' for _ in keys)})"
                " ORDER BY created_at, case_id",
                [part for key in keys for part in key],
            )
        ]
        if not found:
            db.execute(
                "INSERT INTO cases (case_id, created_at, state) VALUES (?, ?, ?)",
                (digest, time.time(), "partial"),
            )
            return digest
        # Merge into the oldest case: evidence, analyst history and pending reports move
        # with it, and it keeps its alert or adopts a merged one.
        case_id = found[0]
        for old in found[1:]:
            # Tell the analyst an alerted case now lives on under the surviving case.
            if db.execute("SELECT 1 FROM alerts WHERE case_id = ?", (old,)).fetchone():
                notify(db, "merged", old)
            for table in ("transfers", "claims", "results", "decisions", "reports"):
                db.execute(
                    f"UPDATE {table} SET case_id = ? WHERE case_id = ?", (case_id, old)
                )
            db.execute(
                "UPDATE outbox SET case_id = ? WHERE case_id = ? AND acked_at IS NULL",
                (case_id, old),
            )
            db.execute(
                "UPDATE OR IGNORE alerts SET case_id = ? WHERE case_id = ?",
                (case_id, old),
            )
            db.execute("DELETE FROM alerts WHERE case_id = ?", (old,))
            db.execute(
                "UPDATE cases SET merged_into = ? WHERE case_id = ?", (case_id, old)
            )
        return case_id

    def get(self, case_id: str) -> Case | None:
        row = self.db.execute(
            "SELECT * FROM cases LEFT JOIN alerts USING (case_id) WHERE case_id = ?",
            (case_id,),
        ).fetchone()
        if row is None:
            return None
        claims = self.db.execute(
            f"SELECT {CLAIM_COLUMNS} FROM claims WHERE case_id = ? ORDER BY rowid",
            (case_id,),
        )
        results = self.db.execute(
            "SELECT chain_digest, sender, state FROM results WHERE case_id = ?"
            " ORDER BY received_at, chain_digest, sender",
            (case_id,),
        )
        decisions = self.db.execute(
            "SELECT action, decided_at FROM decisions WHERE case_id = ?"
            " ORDER BY decided_at",
            (case_id,),
        )
        return Case(
            **row,
            claims=[Claim(*claim) for claim in claims],
            results=[tuple(result) for result in results],
            decisions=[tuple(decision) for decision in decisions],
        )

    def decide(self, case_id: str, action: Decision) -> None:
        if action not in DECISIONS:
            raise ValueError(f"Unknown decision: {action}")
        with self._write() as db:
            now = time.time()
            decision = db.execute(
                "INSERT INTO decisions VALUES (?, ?, ?)", (case_id, action, now)
            ).lastrowid
            if action == "report":
                db.execute(
                    "INSERT INTO outbox (event_id, case_id, created_at, next_at)"
                    " VALUES (?, ?, ?, ?)",
                    (f"report:{case_id}:{decision}", case_id, now, now),
                )

    def save_report(self, case_id: str, source: str, body: str) -> int | None:
        """Store a generated report as a draft pending analyst review; return its ID."""
        return self.db.execute(
            "INSERT INTO reports (case_id, source, body, created_at) VALUES (?, ?, ?, ?)",
            (case_id, source, body, time.time()),
        ).lastrowid

    def review_report(self, case_id: str, report_id: int, status: ReviewStatus) -> bool:
        """Approve or reject a case's draft once; False if it isn't a pending draft."""
        if status not in get_args(ReviewStatus):
            raise ValueError(f"Unknown review: {status}")
        return bool(
            self.db.execute(
                "UPDATE reports SET status = ?, reviewed_at = ?"
                " WHERE case_id = ? AND report_id = ? AND status = 'draft'",
                (status, time.time(), case_id, report_id),
            ).rowcount
        )

    def reports(self, case_id: str) -> list[Report]:
        return [
            Report(**row)
            for row in self.db.execute(
                "SELECT * FROM reports WHERE case_id = ? ORDER BY report_id DESC",
                (case_id,),
            )
        ]

    def notifications(self) -> list[dict]:
        return [
            dict(row)
            for row in self.db.execute(
                "SELECT * FROM notifications ORDER BY created_at DESC LIMIT 20"
            )
        ]

    def due(self, now: float, max_attempts: int) -> list[dict]:
        """Unacked reports whose next attempt is due, oldest first."""
        return [
            dict(row)
            for row in self.db.execute(
                "SELECT event_id, case_id, attempts FROM outbox"
                " WHERE acked_at IS NULL AND attempts < ? AND next_at <= ?"
                " ORDER BY created_at, event_id",
                (max_attempts, now),
            )
        ]

    def ack(self, event_id: str) -> None:
        self.db.execute(
            "UPDATE outbox SET acked_at = ?, error = NULL WHERE event_id = ?",
            (time.time(), event_id),
        )

    def failed(self, event_id: str, at: float, error: str) -> None:
        self.db.execute(
            "UPDATE outbox SET attempts = attempts + 1, next_at = ?, error = ?"
            " WHERE event_id = ?",
            (at, error, event_id),
        )

    def outbox(self, pending: bool = False) -> list[dict]:
        where = " WHERE acked_at IS NULL" if pending else ""
        return [
            dict(row)
            for row in self.db.execute(
                f"SELECT * FROM outbox{where} ORDER BY created_at"
            )
        ]

    def cases(self) -> list[CaseSummary]:
        """Live (unmerged) cases, oldest first; raised_at is None until alerted."""
        return [
            CaseSummary(**row)
            for row in self.db.execute(
                "SELECT case_id, created_at, state, raised_at, (SELECT action FROM"
                " decisions d WHERE d.case_id = cases.case_id ORDER BY decided_at DESC"
                " LIMIT 1) AS decision FROM cases LEFT JOIN alerts USING (case_id)"
                " WHERE merged_into IS NULL ORDER BY created_at, case_id"
            )
        ]
