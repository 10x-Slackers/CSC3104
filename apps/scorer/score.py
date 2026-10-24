"""Bank-local rules, independent of Kafka. Amounts are compared as decimals."""

import hashlib
import json
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

FIELDS = (
    "payment_id",
    "event_time",
    "bank",
    "direction",
    "account",
    "counterparty_bank",
    "amount",
    "payment_format",
)


def validate(event, bank):
    """Accept CSV or JSON events; strip extra fields (including evaluation labels)."""
    if not isinstance(event, dict) or any(key not in event for key in FIELDS):
        raise ValueError("missing transaction fields")
    e = {key: event[key] for key in FIELDS}
    if any(not isinstance(e[k], str) or not e[k] for k in FIELDS if k != "amount"):
        raise ValueError("transaction fields must be nonempty strings")
    if bank not in "ABC" or len(bank) != 1 or e["bank"] != bank:
        raise ValueError("wrong bank")
    if e["counterparty_bank"] not in ("A", "B", "C") or e["direction"] not in (
        "in",
        "out",
    ):
        raise ValueError("invalid bank or direction")
    try:
        amount = Decimal(str(e["amount"]))
        timestamp = datetime.fromisoformat(e["event_time"])
    except (ValueError, TypeError, InvalidOperation) as exc:
        raise ValueError("invalid amount or timestamp") from exc
    if not amount.is_finite() or amount <= 0 or timestamp.utcoffset() != timedelta(0):
        raise ValueError("amount must be positive and finite; timestamp must be UTC")
    return e


class Scorer:
    """Match all local in/out pairs, without consuming an incoming payment.

    Retain at most max_events globally and window seconds behind each account's
    greatest timestamp. Late events match in either arrival order; older
    events are ignored. Deduplication has the same retention bound. A payment's
    in/out endpoints are distinct events. State is in memory, not checkpointed.
    """

    def __init__(
        self,
        bank,
        min_ratio="0.90",
        max_ratio="0.98",
        min_delay=60,
        window=1800,
        max_events=10000,
    ):
        if bank not in ("A", "B", "C"):
            raise ValueError("invalid bank")
        self.bank = bank
        self.min_ratio, self.max_ratio = (
            Decimal(str(min_ratio)),
            Decimal(str(max_ratio)),
        )
        if (
            not 0 < self.min_ratio <= self.max_ratio
            or not 0 <= min_delay <= window
            or max_events < 1
        ):
            raise ValueError("invalid scoring thresholds")
        self.min_delay, self.window, self.max_events = min_delay, window, max_events
        self.history = OrderedDict()
        self.accounts = defaultdict(dict)
        self.latest = {}

    def _remove(self, key):
        self.history.pop(key)
        account = key[2]
        del self.accounts[account][key]
        if not self.accounts[account]:
            del self.accounts[account]
            self.latest.pop(account, None)

    def process(self, event):
        e = validate(event, self.bank)
        account = e["account"]
        now = datetime.fromisoformat(e["event_time"])
        latest = max(self.latest.get(account, now), now)
        cutoff = latest - timedelta(seconds=self.window)
        for old_key, (timestamp, _) in list(self.accounts.get(account, {}).items()):
            if timestamp < cutoff:
                self._remove(old_key)
        self.latest[account] = latest
        key = (e["payment_id"], e["direction"], e["account"])
        if now < cutoff or key in self.history:
            return []
        results = []
        for timestamp, previous in self.accounts[e["account"]].values():
            if previous["direction"] == e["direction"]:
                continue
            incoming, outgoing = (
                (previous, e) if e["direction"] == "out" else (e, previous)
            )
            if incoming["payment_id"] == outgoing["payment_id"]:
                continue
            delay = (now - timestamp).total_seconds() * (
                1 if e["direction"] == "out" else -1
            )
            ratio = Decimal(str(outgoing["amount"])) / Decimal(str(incoming["amount"]))
            if (
                self.min_delay <= delay <= self.window
                and self.min_ratio <= ratio <= self.max_ratio
            ):
                results.append(
                    {
                        "detection_id": hashlib.sha256(
                            json.dumps(
                                [
                                    self.bank,
                                    incoming["payment_id"],
                                    outgoing["payment_id"],
                                ],
                                separators=(",", ":"),
                            ).encode()
                        ).hexdigest(),
                        "bank": self.bank,
                        "account": e["account"],
                        "incoming_payment_id": incoming["payment_id"],
                        "outgoing_payment_id": outgoing["payment_id"],
                        "incoming_time": incoming["event_time"],
                        "outgoing_time": outgoing["event_time"],
                        "ratio": float(ratio),
                        "delay_seconds": delay,
                    }
                )
        self.history[key] = (now, e)
        self.accounts[e["account"]][key] = (now, e)
        if len(self.history) > self.max_events:
            self._remove(next(iter(self.history)))
        return results
