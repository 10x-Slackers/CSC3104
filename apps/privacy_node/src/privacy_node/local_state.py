"""Bank-local view the privacy node reads: tokens, local rule links, counterparties.

CSV mode and the integrated Kafka consumer use the same in-memory state.
Nothing here is ever sent to a peer except tokens.
"""

import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Self

from ingestor import Direction, Event
from privacy_node import privacy_node_pb2 as pb
from scorer.score import Link, Scorer
from scorer.state import read_events
from scorer.token_index import TokenIndex
from security import AgreementFiles, canonical_pair


class LocalBank:
    def __init__(
        self,
        bank: str,
        key_version: str,
        key_paths: AgreementFiles,
        events: Iterable[Event] = (),
        max_token_references: int | None = None,
        **rule,
    ) -> None:
        self.bank = bank
        self.index = TokenIndex(bank, key_version, key_paths, max_token_references)
        self.scorer = Scorer(bank, **rule)
        self.lock = threading.RLock()
        self.counterparty = {}  # (payment_id, direction) -> peer bank
        self.onward = {}  # incoming payment_id -> [outgoing payment_id]
        self.backward = {}  # outgoing payment_id -> [incoming payment_id]
        self._detections = {}
        for event in events:
            self.process(event)

    @property
    def detections(self) -> list[Link]:
        with self.lock:
            return list(self._detections.values())

    def process(self, event: Event) -> list[Link]:
        """Publish tokens, references and candidate links before Kafka commits."""
        with self.lock:
            token = self.index.add(event)
            key = (event["payment_id"], event["direction"])
            if token:
                self.counterparty[key] = event["counterparty_bank"]
            detections = self.scorer.process(event)
            for d in detections:
                i, o = d["incoming_payment_id"], d["outgoing_payment_id"]
                # Intra-bank legs have no token, so there is nothing to trace.
                if (
                    (i, "in") in self.counterparty
                    and (o, "out") in self.counterparty
                    and d["detection_id"] not in self._detections
                ):
                    self.onward.setdefault(i, []).append(o)
                    self.backward.setdefault(o, []).append(i)
                    self._detections[d["detection_id"]] = d
            return detections

    @classmethod
    def from_workload(
        cls,
        workload: str | Path,
        bank: str,
        key_version: str,
        key_paths: AgreementFiles,
        **rule,
    ) -> Self:
        events = read_events(Path(workload) / f"bank_{bank}.csv")
        return cls(bank, key_version, key_paths, events, **rule)

    def ref(
        self, payment_id: str, direction: Direction, key_version: str
    ) -> pb.TokenRef:
        """Read a reference in the search's version, including during overlap."""
        banks = canonical_pair(self.bank, self.counterparty[payment_id, direction])
        pair = "-".join(banks)
        token = self.index.token_for(payment_id, banks, key_version)
        return pb.TokenRef(pair=pair, token=token)

    def lookup(
        self, pair: str, key_version: str, token: bytes, direction: Direction
    ) -> str | None:
        """Return this bank's payment_id for a peer token, or None."""
        with self.lock:
            for ref in self.index.lookup(pair.split("-"), key_version, token):
                if ref["direction"] == direction:
                    return ref["payment_id"]
        return None

    def links(self, payment_id: str, direction: Direction) -> list[str]:
        """Local rule links: from an incoming payment (downstream) or into an outgoing one."""
        with self.lock:
            table = self.onward if direction == "in" else self.backward
            return sorted(table.get(payment_id, []))
