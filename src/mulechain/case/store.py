"""A bank's cases: the messages its privacy node sends, joined into cases and tiered.

    Tier 2 (alert)         two or more banks' rules link the chain
    Tier 1 (low priority)  only this bank's rules flagged the payments

Messages that share a transfer belong to one case, so a rule hit, a pending search
and the chain that later confirms it all land on the same case, and overlapping
chains merge. The node may send a message more than once; the store keeps the first
copy and ignores the rest (an idempotent receiver). Each batch is stored under an
advisory lock (`JOIN_LOCK`), so two writers can't both open a case for one transfer.

Other banks' transfers appear only as tokens. This bank's own payments are copied in
when they arrive, because cases are kept long after Valkey has expired them.
"""

import json
import time

from mulechain.db import Db

SCHEMA = """
CREATE TABLE IF NOT EXISTS cases (
  id TEXT PRIMARY KEY, created_at DOUBLE PRECISION, tier INTEGER, state TEXT,
  alerted_at DOUBLE PRECISION, bookmarked INTEGER DEFAULT 0, reviewed INTEGER DEFAULT 0,
  merged_into TEXT);
-- Which case a transfer belongs to: "tok:<token>" or, for own payments, "pay:<payment id>".
CREATE TABLE IF NOT EXISTS refs (ref TEXT PRIMARY KEY, case_id TEXT);
CREATE TABLE IF NOT EXISTS messages (
  key TEXT, kind TEXT, case_id TEXT, rule TEXT, waiting TEXT, links TEXT,
  received_at DOUBLE PRECISION, PRIMARY KEY (key, kind));
CREATE TABLE IF NOT EXISTS legs (
  payment_id TEXT, direction TEXT, account TEXT, amount DOUBLE PRECISION, peer TEXT,
  token TEXT, case_id TEXT, PRIMARY KEY (payment_id, direction));
CREATE INDEX IF NOT EXISTS refs_case ON refs (case_id);
CREATE INDEX IF NOT EXISTS messages_case ON messages (case_id);
CREATE INDEX IF NOT EXISTS legs_case ON legs (case_id);
"""
STATES = ("confirmed", "partial", "pending", "local", "hit")  # strongest first
JOIN_LOCK = 3105  # advisory lock key: one message joins cases at a time


def tier(bank: str, message: dict) -> int:
    """2 if two banks' rules back the chain, 1 if only this bank's rule fired, else 0.

    A claim with both an incoming and an outgoing token is rule-backed: that bank's own
    rule linked the two transfers. A bank that only sent or received money signs a
    one-sided claim, which confirms the transfer but backs nothing.
    """
    if message["kind"] != "chain":
        return 1
    backed = {link["signer"] for link in message["links"] if link["in_token"] and link["out_token"]}
    return 2 if len(backed) >= 2 else int(bank in backed)


def flow_order(links: list[dict]) -> list[dict]:
    """The links of a case's (often overlapping) chains as one path, in money-flow order."""
    rest = [link for i, link in enumerate(links) if link not in links[:i]]
    ordered = []
    while rest:
        fed = {link["out_token"] for link in rest} - {""}
        link = next((link for link in rest if link["in_token"] not in fed), rest[0])
        while link:  # follow the money from a start (or from anywhere on a loop)
            ordered.append(link)
            rest.remove(link)
            out = link["out_token"]
            link = next((x for x in rest if out and x["in_token"] == out), None)
    return ordered


class CaseStore:
    def __init__(self, url: str, bank: str) -> None:
        self.db, self.bank = Db(url, SCHEMA), bank

    def record(self, messages: list[dict]) -> list[dict]:
        """Store a batch of the node's messages; returns the case each one belongs to."""
        with self.db.transaction(lock=JOIN_LOCK):
            return [self._record(message) for message in messages]

    def _record(self, message: dict) -> dict:
        key, kind, legs = message["key"], message["kind"], message["legs"]
        links = message.get("links", [])
        new_tier = tier(self.bank, message)
        state = message["state"] if kind == "chain" else kind
        rule = message.get("rule") or ("chain" if kind == "chain" and new_tier else None)
        refs = sorted(
            {f"tok:{link[side]}" for link in links for side in ("in_token", "out_token")}
            | {f"tok:{leg['token']}" for leg in legs}
            | {f"pay:{leg['payment_id']}" for leg in legs}
        )
        refs = [ref for ref in refs if ref != "tok:"]
        now = time.time()
        seen = self.db.all("SELECT case_id FROM messages WHERE key = ? AND kind = ?", key, kind)
        if seen:  # a repeat: nothing changes
            return self.db.all("SELECT * FROM cases WHERE id = ?", seen[0]["case_id"])[0]
        case = self._join(refs, key, now)
        if new_tier > case["tier"]:
            case["reviewed"] = 0  # a case that got more serious needs another look
        if new_tier == 2 and not case["alerted_at"]:
            case["alerted_at"] = now  # one alert per case, however many chains join it
        case["tier"] = max(case["tier"], new_tier)
        case["state"] = min(case["state"], state, key=STATES.index)
        self.db.run(
            "UPDATE cases SET tier = ?, state = ?, alerted_at = ?, bookmarked = ?, reviewed = ?"
            " WHERE id = ?",
            *(case[f] for f in ("tier", "state", "alerted_at", "bookmarked", "reviewed", "id")),
        )
        self.db.run(
            "INSERT INTO messages VALUES (?,?,?,?,?,?,?)",
            key,
            kind,
            case["id"],
            rule,
            message.get("waiting_for"),
            json.dumps(links),
            now,
        )
        for ref in refs:
            self.db.run("INSERT INTO refs VALUES (?,?) ON CONFLICT DO NOTHING", ref, case["id"])
        for leg in legs:
            self.db.run(
                "INSERT INTO legs VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                *(
                    leg[f]
                    for f in ("payment_id", "direction", "account", "amount", "peer", "token")
                ),
                case["id"],
            )
        return case

    def _join(self, refs: list[str], key: str, now: float) -> dict:
        """The case these transfers belong to: a new one, the one that already has
        them, or, if they bridge several cases, the oldest with the others merged in."""
        found = self.db.all(
            "SELECT DISTINCT c.* FROM refs r JOIN cases c ON c.id = r.case_id"
            f" WHERE r.ref IN ({','.join('?' * len(refs))}) ORDER BY c.created_at, c.id",
            *refs,
        )
        if not found:
            case = {"id": key, "created_at": now, "tier": 0, "state": "hit", "alerted_at": None}
            case |= {"bookmarked": 0, "reviewed": 0, "merged_into": None}
            self.db.run("INSERT INTO cases VALUES (?,?,?,?,?,?,?,?)", *case.values())
            return case
        case, *others = found
        for other in others:
            for table in ("refs", "messages", "legs"):
                self.db.run(
                    f"UPDATE {table} SET case_id = ? WHERE case_id = ?", case["id"], other["id"]
                )
            self.db.run("UPDATE cases SET merged_into = ? WHERE id = ?", case["id"], other["id"])
            case["tier"] = max(case["tier"], other["tier"])
            case["state"] = min(case["state"], other["state"], key=STATES.index)
            alerts = [c["alerted_at"] for c in (case, other) if c["alerted_at"]]
            case["alerted_at"] = min(alerts, default=None)
            case["bookmarked"] = max(case["bookmarked"], other["bookmarked"])
            case["reviewed"] = min(case["reviewed"], other["reviewed"])
        return case

    def cases(self, limit: int = 10**9) -> list[dict]:
        """Open cases, most urgent first: alerts, then Tier 1 by the number of this bank's
        rules that fired, then by amount, then oldest first."""
        return self.db.all(
            "SELECT c.*, m.rules, m.waiting, l.amount FROM cases c"
            " JOIN (SELECT case_id, COUNT(DISTINCT rule) AS rules, MAX(waiting) AS waiting"
            "  FROM messages GROUP BY case_id) m ON m.case_id = c.id"
            " JOIN (SELECT case_id, SUM(amount) AS amount FROM legs GROUP BY case_id) l"
            "  ON l.case_id = c.id"
            " WHERE c.merged_into IS NULL AND c.tier > 0"
            " ORDER BY c.tier DESC, CASE WHEN c.tier = 1 THEN m.rules END DESC,"
            " CASE WHEN c.tier = 1 THEN l.amount END DESC, c.created_at, c.id LIMIT ?",
            limit,
        )

    def payments(self) -> dict[str, list[str]]:
        """Each case's own payment IDs, for comparing a cluster run with an offline one."""
        found: dict[str, list[str]] = {}
        for leg in self.db.all("SELECT DISTINCT case_id, payment_id FROM legs ORDER BY payment_id"):
            found.setdefault(leg["case_id"], []).append(leg["payment_id"])
        return found

    def get(self, case_id: str) -> dict | None:
        """One case with everything the analyst page and the summary service show."""
        found = self.db.all("SELECT * FROM cases WHERE id = ?", case_id)
        if not found:
            return None
        messages = self.db.all(
            "SELECT * FROM messages WHERE case_id = ? ORDER BY received_at, key", case_id
        )
        links = [link for m in messages for link in json.loads(m.pop("links"))]
        legs = self.db.all(
            "SELECT payment_id, direction, account, amount, peer, token FROM legs"
            " WHERE case_id = ? ORDER BY payment_id, direction",
            case_id,
        )
        waiting = next((m["waiting"] for m in messages if m["waiting"]), None)
        seen = {"messages": messages, "legs": legs, "links": flow_order(links)}
        return found[0] | {"bank": self.bank, "waiting": waiting} | seen

    def toggle(self, case_id: str, flag: str) -> None:
        """Flip the analyst's `bookmarked` or `reviewed` flag."""
        self.db.run(f"UPDATE cases SET {flag} = 1 - {flag} WHERE id = ?", case_id)
