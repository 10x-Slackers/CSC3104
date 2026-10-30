"""Bank-local view the privacy node reads: tokens, local rule links, counterparties.

CSV mode and the integrated Kafka consumer use the same in-memory state.
Nothing here is ever sent to a peer except tokens.
"""

import csv
import threading
from pathlib import Path

from privacy_node import privacy_node_pb2 as pb
from scorer.score import Scorer
from scorer.token_index import TokenIndex
from security import canonical_pair


class LocalBank:
    def __init__(
        self, bank, key_version, key_paths, events=(), max_token_references=None, **rule
    ):
        self.bank = bank
        self.index = TokenIndex(bank, key_version, key_paths, max_token_references)
        self.scorer = Scorer(bank, **rule)
        self.lock = threading.RLock()
        self.refs = {}  # (payment_id, direction) -> TokenRef
        self.counterparty = {}  # (payment_id, direction) -> peer bank
        self.onward = {}  # incoming payment_id -> [outgoing payment_id]
        self.backward = {}  # outgoing payment_id -> [incoming payment_id]
        self._detections = {}
        for event in events:
            self.process(event)

    @property
    def detections(self):
        with self.lock:
            return list(self._detections.values())

    def process(self, event):
        """Publish tokens, references and candidate links before Kafka commits."""
        with self.lock:
            token = self.index.add(event)
            key = (event["payment_id"], event["direction"])
            if token:
                pair = "-".join(canonical_pair(self.bank, event["counterparty_bank"]))
                self.refs[key] = pb.TokenRef(pair=pair, token=token)
                self.counterparty[key] = event["counterparty_bank"]
            detections = self.scorer.process(event)
            for d in detections:
                i, o = d["incoming_payment_id"], d["outgoing_payment_id"]
                # Intra-bank legs have no token, so there is nothing to trace.
                if (
                    (i, "in") in self.refs
                    and (o, "out") in self.refs
                    and d["detection_id"] not in self._detections
                ):
                    self.onward.setdefault(i, []).append(o)
                    self.backward.setdefault(o, []).append(i)
                    self._detections[d["detection_id"]] = d
            return detections

    @classmethod
    def from_workload(cls, workload, bank, key_version, key_paths, **rule):
        with (Path(workload) / f"bank_{bank}.csv").open() as source:
            return cls(bank, key_version, key_paths, csv.DictReader(source), **rule)

    def lookup(self, pair, key_version, token, direction):
        """Return this bank's payment_id for a peer token, or None."""
        with self.lock:
            for ref in self.index.lookup(tuple(pair.split("-")), key_version, token):
                if ref["direction"] == direction:
                    return ref["payment_id"]
        return None

    def links(self, payment_id, direction):
        """Local rule links: from an incoming payment (downstream) or into an outgoing one."""
        with self.lock:
            table = self.onward if direction == "in" else self.backward
            return sorted(table.get(payment_id, []))
