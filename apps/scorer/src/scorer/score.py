"""Bank-local rules, independent of Kafka. Amounts are compared as decimals.

Events are trusted as they are: the ingestor validates them before Kafka.
"""

import hashlib
import json
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal


class Scorer:
    """Match all local in/out pairs, without consuming an incoming payment.

    Accept events up to window seconds behind each account's greatest timestamp
    (never forgotten); older events are ignored. Retain events for twice the window,
    so late events match in either arrival order, and at most max_events globally.
    Deduplication has the same retention bound. A payment's
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

    def process(self, e):
        account = e["account"]
        now = datetime.fromisoformat(e["event_time"])
        latest = max(self.latest.get(account, now), now)
        cutoff = latest - timedelta(seconds=self.window)
        # A late event at the cutoff can still pair with an event a window before it.
        for old_key, (timestamp, _) in list(self.accounts.get(account, {}).items()):
            if timestamp < cutoff - timedelta(seconds=self.window):
                self._remove(old_key)
        self.latest[account] = latest
        key = (e["payment_id"], e["direction"], account)
        if now < cutoff or key in self.history:
            return []
        results = []
        for timestamp, previous in self.accounts[account].values():
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
                ids = [self.bank, incoming["payment_id"], outgoing["payment_id"]]
                results.append(
                    {
                        "detection_id": hashlib.sha256(
                            json.dumps(ids, separators=(",", ":")).encode()
                        ).hexdigest(),
                        "bank": self.bank,
                        "account": account,
                        "incoming_payment_id": incoming["payment_id"],
                        "outgoing_payment_id": outgoing["payment_id"],
                        "incoming_time": incoming["event_time"],
                        "outgoing_time": outgoing["event_time"],
                        "ratio": float(ratio),
                        "delay_seconds": delay,
                    }
                )
        self.history[key] = (now, e)
        self.accounts[account][key] = (now, e)
        if len(self.history) > self.max_events:
            self._remove(next(iter(self.history)))
        return results
