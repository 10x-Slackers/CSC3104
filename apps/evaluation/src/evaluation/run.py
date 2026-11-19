"""Run every bank's live path in one process: the scorer writing Valkey state, the
privacy node consuming its hits and searching, and the case store tiering the cases."""

import json
import tempfile
from collections import Counter
from pathlib import Path

import fakeredis
from google.protobuf.message import Message

from case.case_store import CaseStore
from case.evidence import parse
from evaluation.score import Case
from ingestor import read_events
from privacy_node import privacy_node_pb2 as pb
from privacy_node.demo import prepare_demo_keys
from privacy_node.keys import BANKS, own_key, pair_paths, public_keys
from privacy_node.node import Node
from privacy_node.store import Store
from privacy_node.valkey_bank import ValkeyBank
from scorer.score import Scorer
from scorer.state import ValkeyState
from scorer.token_index import TokenIndex

KEY_VERSION = "k1"


class OfflineNode(Node):
    """A privacy node that calls its peers directly instead of over gRPC, and hands
    its case messages to this bank's case store instead of posting them."""

    def __init__(self, *args, cases: CaseStore, **kwargs) -> None:
        super().__init__(*args, case_url="offline", **kwargs)
        self.cases = cases

    def call[R: Message](
        self, bank: str, method: str, request: Message, timeout: float, fallback: R
    ) -> R:
        return getattr(self.peers[bank], method)(request, OfflineContext())

    def deliver_cases(self, stop: object = None) -> None:
        for key, kind, body in self.store.pending_cases():
            self.cases.record(parse(json.loads(body), self.public_keys, self.bank))
            self.store.ack_case(key, kind, body)


class OfflineContext:
    def time_remaining(self) -> None:
        return None  # no deadline: the peer allows a whole search

    def abort(self, code: object, details: str):
        raise RuntimeError(f"peer rejected a call: {code} {details}")


def start_nodes(workload: str | Path, key_dir: str) -> dict[str, OfflineNode]:
    """Score each bank's file into its own Valkey, then start a node on it."""
    prepare_demo_keys(key_dir)
    nodes = {}
    for bank in BANKS:
        db = fakeredis.FakeValkey(decode_responses=True)
        paths = pair_paths(key_dir, bank)
        index = TokenIndex(bank, KEY_VERSION, paths)
        scorer = Scorer(bank, state=ValkeyState(db), token_index=index)
        for event in read_events(Path(workload) / f"bank_{bank}.csv"):
            scorer.process(event)  # it skips all but bank transfers, as replay does
        nodes[bank] = OfflineNode(
            ValkeyBank(db, bank, KEY_VERSION, paths),
            Store(),
            own_key(key_dir, bank),
            public_keys(key_dir, bank),
            nodes,
            cases=CaseStore(":memory:"),
        )
    return nodes


def run(workload: str | Path) -> tuple[list[Case], dict[str, int]]:
    """Run every bank's rules, searches and case store, as the cluster does after a
    replay settles; return (cases, search state counts)."""
    with tempfile.TemporaryDirectory() as key_dir:
        nodes = start_nodes(workload, key_dir)
        for node in nodes.values():
            while node.consume_hits(block_ms=None):
                pass
        # the grace is over: unresolved hits post local cases; twice, so each bank
        # stores the results the others delivered after its own pass
        for node in [*nodes.values()] * 2:
            node.housekeeping()
        assert not any(node.store.pending_cases() for node in nodes.values())
        cases = [case for node in nodes.values() for case in bank_cases(node)]
    searches = Counter(
        pb.State.Name(state)  # each search's latest attempt
        for node in nodes.values()
        for (state,) in node.store.db.execute("SELECT state FROM searches")
    )
    return cases, dict(sorted(searches.items()))


def bank_cases(node: OfflineNode) -> list[Case]:
    """This bank's live cases with a tier, by its own payment IDs, as its UI shows them."""
    return [
        Case(node.bank, c["tier"], {leg["payment_id"] for leg in c["legs"]})
        for c in (node.cases.get(row["case_id"]) for row in node.cases.cases())
    ]
