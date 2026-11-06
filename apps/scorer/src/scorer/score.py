"""Bank-local rules, independent of Kafka. Amounts are compared as decimals.

Events are trusted as they are: the ingestor validates them before Kafka. The rules
are in streaming.md and issue #27; they only look at bank transfers.
"""

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TypedDict

from scorer.state import Event, MemoryState, read_events

BANK_TRANSFERS = ("GIRO", "MEPS+")
CHAIN_RATIO = (Decimal("0.90"), Decimal("1.10"))
CHAIN_WINDOW = 72 * 3600  # also the allowed lateness
REPEAT_WINDOW = 24 * 3600


class Link(TypedDict):
    detection_id: str
    bank: str
    account: str
    incoming_payment_id: str
    outgoing_payment_id: str
    incoming_time: str
    outgoing_time: str
    ratio: float
    delay_seconds: float
    rules: list[str]


def digest(*parts: object) -> str:
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()


class Scorer:
    """Link every in/out pair on one account (fast, chain) and write rule hits.

    An event more than CHAIN_WINDOW behind its account's latest event is ignored as
    late; events are kept for twice that, so late ones match in either arrival order.
    """

    def __init__(
        self,
        bank: str,
        min_ratio: str | float | Decimal = "0.90",
        max_ratio: str | float | Decimal = "0.98",
        min_delay: float = 60,
        window: float = 1800,
    ) -> None:
        if bank not in ("A", "B", "C"):
            raise ValueError("invalid bank")
        self.bank = bank
        self.min_ratio, self.max_ratio = (
            Decimal(str(min_ratio)),
            Decimal(str(max_ratio)),
        )
        if not 0 < self.min_ratio <= self.max_ratio or not 0 <= min_delay <= window:
            raise ValueError("invalid scoring thresholds")
        self.min_delay, self.window = min_delay, window
        self.state = MemoryState()

    def hit(self, rule: str, account: str, *payments: str) -> None:
        hit_id = digest(self.bank, rule, *payments)
        self.state.put_hit(
            {
                "rule": rule,
                "account": account,
                "payments": list(payments),
                "hit_id": hit_id,
            }
        )

    def process(self, e: Event) -> list[Link]:
        if e["payment_format"] not in BANK_TRANSFERS:
            return []
        account, now = e["account"], datetime.fromisoformat(e["event_time"]).timestamp()
        self.state.put_payment(
            e["payment_id"],
            e["direction"],
            {k: e[k] for k in ("account", "amount", "event_time", "counterparty_bank")},
        )
        events = self.state.window(account)
        key = (e["payment_id"], e["direction"])
        latest = max([now, *(ts for ts, _ in events.values())])
        if key in events or now < latest - CHAIN_WINDOW:
            return []  # a repeat, or too late
        self.state.trim(account, latest - 2 * CHAIN_WINDOW)

        links: list[Link] = []
        for ts, other in list(events.values()):
            if (
                other["direction"] == e["direction"]
                or other["payment_id"] == e["payment_id"]
            ):
                continue
            inc, out = (other, e) if e["direction"] == "out" else (e, other)
            delay = now - ts if e["direction"] == "out" else ts - now
            ratio = Decimal(str(out["amount"])) / Decimal(str(inc["amount"]))
            rules = []
            if (
                self.min_delay <= delay <= self.window
                and self.min_ratio <= ratio <= self.max_ratio
            ):
                rules.append("fast")
            cross = (
                self.bank != inc["counterparty_bank"]
                or self.bank != out["counterparty_bank"]
            )
            if (
                60 <= delay <= CHAIN_WINDOW
                and CHAIN_RATIO[0] <= ratio <= CHAIN_RATIO[1]
                and cross
            ):
                rules.append("chain")
            if not rules:
                continue
            self.state.add_link(inc["payment_id"], out["payment_id"])
            if "chain" in rules:
                self.hit("chain", account, inc["payment_id"], out["payment_id"])
            links.append(
                {
                    "detection_id": digest(
                        self.bank, inc["payment_id"], out["payment_id"]
                    ),
                    "bank": self.bank,
                    "account": account,
                    "incoming_payment_id": inc["payment_id"],
                    "outgoing_payment_id": out["payment_id"],
                    "incoming_time": inc["event_time"],
                    "outgoing_time": out["event_time"],
                    "ratio": float(ratio),
                    "delay_seconds": delay,
                    "rules": rules,
                }
            )

        if e["direction"] == "out":
            # new payee and quick repeat: one entry per payer and payee
            payee, cross = (
                e["counterparty_account"],
                e["counterparty_bank"] != self.bank,
            )
            last = self.state.last_paid(account, payee)
            flagged = any("chain" in link["rules"] for link in links)
            if cross and last is None:
                self.hit("new_payee", account, e["payment_id"])
                flagged = True
            elif cross and last[1] and 0 <= now - last[0] <= REPEAT_WINDOW:
                self.hit("quick_repeat", account, e["payment_id"])
            self.state.set_last_paid(account, payee, now, flagged)
        self.state.add_event(account, key, now, e)
        return links


def score_file(workload: str | Path, bank: str) -> MemoryState:
    """Run the live rules over one bank file, as the offline evaluator does."""
    scorer = Scorer(bank)
    for e in read_events(Path(workload) / f"bank_{bank}.csv"):
        scorer.process(e)
    return scorer.state
