"""Bank-local view the privacy node reads: tokens, local rule links, counterparties.

M2 shortcut: rebuilt from the bank's event file with Role 2's Scorer and TokenIndex.
For M3 this moves to the bank's PostgreSQL, written by the scorer (streaming.md).
Nothing here is ever sent to a peer except tokens.
"""

import csv
from pathlib import Path

from apps.privacy_node import privacy_node_pb2 as pb
from apps.scorer.score import Scorer
from apps.scorer.token_index import TokenIndex


def pair_name(pair):
    return "-".join(pair)


class LocalBank:
    def __init__(self, bank, key_version, key_paths, events, **rule):
        self.bank = bank
        self.index = TokenIndex(bank, key_version, key_paths)
        scorer = Scorer(bank, **rule)
        self.refs = {}  # (payment_id, direction) -> (pair name, token)
        self.counterparty = {}  # (payment_id, direction) -> peer bank
        self.onward = {}  # incoming payment_id -> [outgoing payment_id]
        self.backward = {}  # outgoing payment_id -> [incoming payment_id]
        self.detections = []
        for event in events:
            token = self.index.add(event)
            key = (event["payment_id"], event["direction"])
            if token:
                pair = sorted((bank, event["counterparty_bank"]))
                self.refs[key] = (pair_name(pair), token)
                self.counterparty[key] = event["counterparty_bank"]
            for d in scorer.process(event):
                i, o = d["incoming_payment_id"], d["outgoing_payment_id"]
                self.onward.setdefault(i, []).append(o)
                self.backward.setdefault(o, []).append(i)
                self.detections.append(d)

    @classmethod
    def from_workload(cls, workload, bank, key_version, key_paths, **rule):
        with (Path(workload) / f"bank_{bank}.csv").open() as source:
            return cls(bank, key_version, key_paths, csv.DictReader(source), **rule)

    def lookup(self, pair, key_version, token, direction):
        """Return this bank's payment_id for a peer token, or None."""
        for ref in self.index.lookup(tuple(pair.split("-")), key_version, token):
            if ref["direction"] == direction:
                return ref["payment_id"]
        return None

    def links(self, payment_id, direction):
        """Local rule links: from an incoming payment (downstream) or into an outgoing one."""
        table = self.onward if direction == "in" else self.backward
        return sorted(table.get(payment_id, []))

    def ref(self, payment_id, direction):
        pair, token = self.refs[payment_id, direction]
        return pb.TokenRef(pair=pair, token=token)
