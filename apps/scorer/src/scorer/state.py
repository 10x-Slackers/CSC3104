"""In-memory stand-in for a bank's Valkey, with the entries of streaming.md's Valkey layout."""

import csv
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
from typing import Literal, TypedDict, cast

Direction = Literal["in", "out"]


class Event(TypedDict):
    """A Kafka record value (amount a float) or a bank-file CSV row (all str)."""

    payment_id: str
    event_time: str
    bank: str
    direction: Direction
    account: str
    counterparty_account: str
    counterparty_bank: str
    amount: str | float
    payment_format: str


class Hit(TypedDict):
    rule: str
    account: str
    payments: list[str]
    hit_id: str


def read_events(path: str | Path) -> Iterator[Event]:
    with Path(path).open() as source:
        for row in csv.DictReader(source):
            yield cast(Event, row)


class MemoryState:
    def __init__(self) -> None:
        # win:<account>: (payment_id, direction) -> (ts, event)
        self.win = defaultdict(dict)
        self.pay = {}  # pay:<payment_id>:<direction>
        self.link = defaultdict(set)  # link:in:<payment_id> and link:out:<payment_id>
        self.hit = {}  # hit:<hit_id>, in the order written (the hits stream)
        self.paid = {}  # last:<account>:<payee> -> (ts, flagged)

    # written by the scorer

    def window(self, account: str) -> dict[tuple[str, Direction], tuple[float, Event]]:
        return self.win[account]

    def add_event(
        self, account: str, key: tuple[str, Direction], ts: float, event: Event
    ) -> None:
        self.win[account][key] = (ts, event)

    def trim(self, account: str, before: float) -> None:
        for key, (ts, _) in list(self.win[account].items()):
            if ts < before:
                del self.win[account][key]

    def put_payment(self, payment_id: str, direction: Direction, entry: dict) -> None:
        self.pay[payment_id, direction] = entry

    def add_link(self, incoming: str, outgoing: str) -> None:
        self.link["in", incoming].add(outgoing)
        self.link["out", outgoing].add(incoming)

    def put_hit(self, hit: Hit) -> None:
        self.hit.setdefault(hit["hit_id"], hit)

    def last_paid(self, account: str, payee: str) -> tuple[float, bool] | None:
        return self.paid.get((account, payee))

    def set_last_paid(self, account: str, payee: str, ts: float, flagged: bool) -> None:
        # a late, older payment must not replace a newer one
        if ts >= self.paid.get((account, payee), (ts,))[0]:
            self.paid[account, payee] = (ts, flagged)

    # read by the node and the offline evaluator

    def links(self, payment_id: str, direction: Direction) -> list[str]:
        return sorted(self.link.get((direction, payment_id), ()))

    def payment(self, payment_id: str, direction: Direction) -> dict | None:
        return self.pay.get((payment_id, direction))

    def hits(self) -> list[Hit]:
        return list(self.hit.values())
