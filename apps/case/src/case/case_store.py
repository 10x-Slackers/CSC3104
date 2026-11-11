"""Bank-local case store: the node's messages, joined into cases at the highest tier.

Fed only by this bank's privacy node, once per message key (docs/contracts/protocol.md),
so the store keeps the transfer join but little other deduplication. Other banks' legs
stay tokens; this bank's own legs are snapshotted into `own_legs` when they arrive.
PostgreSQL in the cluster (a `postgresql://` URL); a SQLite file for tests and local runs.
Nothing is ever deleted (MAS Notice 626 para 12.3). Contract: docs/contracts/case.md.
"""

import sqlite3
import time
from contextlib import contextmanager
from dataclasses import astuple, dataclass, field, fields
from typing import Literal, Self, TypedDict

from privacy_node import privacy_node_pb2 as pb

Kind = Literal["chain", "pending", "local", "hit"]
# A case's state is its strongest message's, in this order.
STATES = ("confirmed", "partial", "pending", "local", "hit")


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


class Leg(TypedDict):
    """One of this bank's own payments on a case; token is empty for intra-bank legs."""

    payment_id: str
    direction: str
    account: str
    amount: float
    pair: str
    token: str


@dataclass(frozen=True)
class Message:
    """One payload from the node, already checked (evidence.parse)."""

    key: str  # chain_digest, search ID or hit ID
    kind: Kind
    tier: int  # 2 alert, 1 local suspicion, 0 evidence only
    state: str  # one of STATES
    legs: list[Leg]
    claims: list[Claim] = field(default_factory=list)
    detail: str = ""  # sender, the bank waited for, or the rule


class Case(TypedDict):
    case_id: str
    created_at: float
    tier: int
    state: str
    merged_into: str | None
    bookmarked: bool
    reviewed: bool
    raised_at: float | None
    claims: list[Claim]
    messages: list[dict]
    legs: list[Leg]


def transfers(claim: Claim) -> list[tuple[str, str]]:
    """Case transfer references: (pair, token hex)."""
    refs = ((claim.in_pair, claim.in_token), (claim.out_pair, claim.out_token))
    return [(pair, token) for pair, token in refs if token]


def neighbours(signer: str, in_pair: str, out_pair: str) -> tuple[str, str]:
    """Banks at the far end of the incoming and outgoing transfers; start/end if none."""

    def other(pair, none):
        return next((b for b in pair.split("-") if b != signer), none) if pair else none

    return other(in_pair, "start"), other(out_pair, "end")


CLAIM_COLUMNS = ", ".join(f.name for f in fields(Claim))
SCHEMA = f"""
CREATE TABLE IF NOT EXISTS cases (
    case_id TEXT PRIMARY KEY,
    created_at DOUBLE PRECISION NOT NULL,
    tier INTEGER NOT NULL,
    state TEXT NOT NULL,
    merged_into TEXT,
    bookmarked INTEGER NOT NULL DEFAULT 0,
    reviewed INTEGER NOT NULL DEFAULT 0
);
-- Each transfer belongs to one case: a message sharing any transfer joins that case.
-- ref is "tok:<pair>:<token hex>" for a cross-bank transfer, "pay:<payment_id>" for
-- this bank's own payments.
CREATE TABLE IF NOT EXISTS transfers (
    ref TEXT PRIMARY KEY,
    case_id TEXT NOT NULL
);
-- The idempotent consumer: one row per node message key, upgraded in place.
CREATE TABLE IF NOT EXISTS messages (
    key TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    tier INTEGER NOT NULL,
    state TEXT NOT NULL,
    detail TEXT NOT NULL,
    case_id TEXT NOT NULL,
    received_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS claims (
    {", ".join(f.name + (" INTEGER" if f.name == "truncated" else " TEXT") + " NOT NULL" for f in fields(Claim))},
    case_id TEXT NOT NULL,
    received_at DOUBLE PRECISION NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (signer, key_version, in_pair, in_token, out_pair, out_token)
);
-- Protected: this bank's own legs, snapshotted once, because the token index expires
-- in days while cases are kept. Never sent to a peer.
CREATE TABLE IF NOT EXISTS own_legs (
    payment_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    account TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    pair TEXT NOT NULL,
    token TEXT NOT NULL,
    case_id TEXT NOT NULL,
    PRIMARY KEY (payment_id, direction)
);
-- At most one alert per case at this bank.
CREATE TABLE IF NOT EXISTS alerts (
    case_id TEXT PRIMARY KEY,
    raised_at DOUBLE PRECISION NOT NULL
);
-- The analyst's inbox, written in the same transaction as the alert or merge.
CREATE TABLE IF NOT EXISTS notifications (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    case_id TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL
);
"""


class Db:
    """One connection; SQL uses ? placeholders for both SQLite and PostgreSQL."""

    def __init__(self, url: str) -> None:
        self.pg = url.startswith("postgres")
        if self.pg:
            import psycopg
            from psycopg.rows import dict_row

            self.conn = psycopg.connect(url, autocommit=True, row_factory=dict_row)
            self.conn.execute(SCHEMA)
        else:
            self.conn = sqlite3.connect(url, isolation_level=None, timeout=30)
            self.conn.row_factory = lambda cur, row: {
                d[0]: v for d, v in zip(cur.description, row, strict=True)
            }
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.executescript(SCHEMA)

    def execute(self, sql: str, args=()):
        return self.conn.execute(sql.replace("?", "%s") if self.pg else sql, args)

    def executemany(self, sql: str, rows) -> None:
        if self.pg:
            self.conn.cursor().executemany(sql.replace("?", "%s"), rows)
        else:
            self.conn.executemany(sql, rows)

    @contextmanager
    def write(self):
        """One transaction; concurrent writers queue, so a merge sees every case."""
        if self.pg:
            with self.conn.transaction():
                self.conn.execute("LOCK TABLE cases IN SHARE ROW EXCLUSIVE MODE")
                yield self
            return
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")


def notify(db: Db, kind: str, case_id: str) -> None:
    """Add one inbox entry per (kind, case); repeats are ignored."""
    db.execute(
        "INSERT INTO notifications VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
        (f"{kind}:{case_id}", kind, case_id, time.time()),
    )


class CaseStore:
    def __init__(self, url: str) -> None:
        self.db = Db(str(url))

    def record(self, m: Message) -> str:
        """Store one node message and return its case ID."""
        refs = sorted(
            {f"tok:{pair}:{token}" for c in m.claims for pair, token in transfers(c)}
            | {f"tok:{leg['pair']}:{leg['token']}" for leg in m.legs if leg["token"]}
            | {f"pay:{leg['payment_id']}" for leg in m.legs}
        )
        now = time.time()
        with self.db.write() as db:
            case_id = self._case_for(db, refs, m.key, now)
            db.execute(
                "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (key)"
                " DO UPDATE SET tier = excluded.tier, state = excluded.state,"
                " detail = excluded.detail, case_id = excluded.case_id",
                (m.key, m.kind, m.tier, m.state, m.detail, case_id, now),
            )
            db.executemany(
                "INSERT INTO transfers VALUES (?, ?) ON CONFLICT DO NOTHING",
                [(ref, case_id) for ref in refs],
            )
            db.executemany(
                f"INSERT INTO claims ({CLAIM_COLUMNS}, case_id, received_at, position)"
                f" VALUES ({', '.join('?' * (len(fields(Claim)) + 3))})"
                " ON CONFLICT DO NOTHING",
                [
                    (
                        *(int(v) if type(v) is bool else v for v in astuple(c)),
                        case_id,
                        now,
                        i,
                    )
                    for i, c in enumerate(m.claims)
                ],
            )
            db.executemany(
                "INSERT INTO own_legs VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                [(*(leg[f] for f in Leg.__annotations__), case_id) for leg in m.legs],
            )
            rows = db.execute(
                "SELECT tier, state FROM messages WHERE case_id = ?", (case_id,)
            ).fetchall()
            tier = max(r["tier"] for r in rows)
            state = min((r["state"] for r in rows), key=STATES.index)
            db.execute(
                "UPDATE cases SET tier = ?, state = ? WHERE case_id = ?",
                (tier, state, case_id),
            )
            if tier == 2:
                raised = db.execute(
                    "INSERT INTO alerts VALUES (?, ?) ON CONFLICT DO NOTHING",
                    (case_id, now),
                )
                if raised.rowcount:
                    notify(db, "alert", case_id)
        return case_id

    def _case_for(self, db: Db, refs: list[str], key: str, now: float) -> str:
        """Find the case these transfers join, merging cases a message bridges.

        A new case takes the message key (Role 3's chain_digest for a chain) as its ID.
        Chains longer than the hop limit arrive as overlapping partial results with
        different digests; they share transfers, so they join one case.
        """
        found = [
            row["case_id"]
            for row in db.execute(
                "SELECT DISTINCT t.case_id, c.created_at FROM transfers t"
                " JOIN cases c ON c.case_id = t.case_id"
                f" WHERE t.ref IN ({', '.join('?' * len(refs))})"
                " ORDER BY c.created_at, t.case_id",
                refs,
            )
        ]
        if not found:
            db.execute(
                "INSERT INTO cases (case_id, created_at, tier, state) VALUES (?, ?, 0, 'hit')",
                (key, now),
            )
            return key
        # Merge into the oldest case: evidence and flags move to it, and it keeps its
        # alert or adopts a merged one.
        case_id = found[0]
        for old in found[1:]:
            if db.execute("SELECT 1 FROM alerts WHERE case_id = ?", (old,)).fetchone():
                notify(db, "merged", old)
            for table in ("transfers", "messages", "claims", "own_legs"):
                db.execute(
                    f"UPDATE {table} SET case_id = ? WHERE case_id = ?", (case_id, old)
                )
            db.execute(
                "INSERT INTO alerts SELECT ?, raised_at FROM alerts WHERE case_id = ?"
                " ON CONFLICT DO NOTHING",
                (case_id, old),
            )
            db.execute("DELETE FROM alerts WHERE case_id = ?", (old,))
            db.execute(
                "UPDATE cases SET bookmarked = (SELECT MAX(bookmarked) FROM cases"
                " WHERE case_id IN (?, ?)), reviewed = (SELECT MAX(reviewed) FROM cases"
                " WHERE case_id IN (?, ?)) WHERE case_id = ?",
                (case_id, old, case_id, old, case_id),
            )
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
            f"SELECT {CLAIM_COLUMNS} FROM claims WHERE case_id = ?"
            " ORDER BY received_at, position",
            (case_id,),
        )
        messages = self.db.execute(
            "SELECT key, kind, tier, state, detail, received_at FROM messages"
            " WHERE case_id = ? ORDER BY received_at, key",
            (case_id,),
        )
        legs = self.db.execute(
            "SELECT payment_id, direction, account, amount, pair, token FROM own_legs"
            " WHERE case_id = ? ORDER BY payment_id, direction",
            (case_id,),
        )
        return Case(
            **row | {f: bool(row[f]) for f in ("bookmarked", "reviewed")},
            claims=[Claim(**c) for c in claims],
            messages=messages.fetchall(),
            legs=legs.fetchall(),
        )

    def flag(self, case_id: str, flag: Literal["bookmarked", "reviewed"]) -> None:
        """Toggle the analyst's bookmark or reviewed flag."""
        assert flag in ("bookmarked", "reviewed")
        self.db.execute(
            f"UPDATE cases SET {flag} = 1 - {flag} WHERE case_id = ?", (case_id,)
        )

    def notifications(self) -> list[dict]:
        return self.db.execute(
            "SELECT * FROM notifications ORDER BY created_at DESC LIMIT 20"
        ).fetchall()

    def cases(self) -> list[dict]:
        """Live cases with a tier, alerts first, then oldest first."""
        return [
            row | {f: bool(row[f]) for f in ("bookmarked", "reviewed")}
            for row in self.db.execute(
                "SELECT case_id, created_at, tier, state, bookmarked, reviewed, raised_at,"
                " (SELECT MAX(detail) FROM messages m WHERE m.case_id = cases.case_id"
                " AND m.kind = 'pending') AS waiting"
                " FROM cases LEFT JOIN alerts USING (case_id)"
                " WHERE merged_into IS NULL AND tier > 0"
                " ORDER BY tier DESC, created_at, case_id"
            )
        ]
