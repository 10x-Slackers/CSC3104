"""Run every bank's live path in one process: the scorer writing Valkey state, the
privacy node consuming its hits and searching, and the case store tiering the cases."""

import json
import tempfile
from collections.abc import Iterator
from pathlib import Path

import fakeredis
from google.protobuf.message import Message

from case.case_store import CaseStore
from case.evidence import parse
from evaluation.score import Case
from ingestor import Event, read_events
from privacy_node import privacy_node_pb2 as pb
from privacy_node.demo import prepare_demo_keys
from privacy_node.keys import BANKS, own_key, pair_paths, public_keys
from privacy_node.node import Node
from privacy_node.store import Store
from privacy_node.valkey_bank import ValkeyBank
from scorer.score import BANK_TRANSFERS, Scorer
from scorer.state import ValkeyState
from scorer.token_index import TokenIndex

KEY_VERSION = "k1"


class OfflineNode(Node):
    """A privacy node that calls its peers directly instead of over gRPC, and hands
    its case messages to this bank's case store instead of posting them."""

    def __init__(self, *args, cases: CaseStore, **kwargs) -> None:
        super().__init__(*args, case_url="offline", **kwargs)
        self.cases, self.states = cases, {}

    def call[R: Message](
        self, bank: str, method: str, request: Message, timeout: float, fallback: R
    ) -> R:
        return getattr(self.peers[bank], method)(request, OfflineContext())

    def search(self, incoming: str, outgoing: str) -> pb.ChainResult:
        result = super().search(incoming, outgoing)
        self.states[incoming, outgoing] = result.state  # a retry replaces it
        return result

    def deliver_cases(self, stop: object = None) -> None:
        for key, kind, body in self.store.pending_cases():
            self.cases.record(parse(json.loads(body), self.public_keys, self.bank))
            self.store.ack_case(key, kind, body)


class OfflineContext:
    def time_remaining(self) -> None:
        return None  # no deadline: the peer allows a whole search

    def abort(self, code: object, details: str):
        raise RuntimeError(f"peer rejected a call: {code} {details}")


def bank_transfers(workload: str | Path, bank: str) -> Iterator[Event]:
    """A bank file as replay sends it: bank transfers only."""
    for event in read_events(Path(workload) / f"bank_{bank}.csv"):
        if event["payment_format"] in BANK_TRANSFERS:
            yield event


def start_nodes(workload: str | Path, work_dir: str) -> dict[str, OfflineNode]:
    """Score each bank's file into its own Valkey, then start a node on it."""
    prepare_demo_keys(work_dir)
    nodes = {}
    for bank in BANKS:
        db = fakeredis.FakeValkey(decode_responses=True)
        paths = pair_paths(work_dir, bank)
        index = TokenIndex(bank, KEY_VERSION, paths)
        scorer = Scorer(bank, state=ValkeyState(db), token_index=index)
        for event in bank_transfers(workload, bank):
            scorer.process(event)
        nodes[bank] = OfflineNode(
            ValkeyBank(db, bank, KEY_VERSION, paths),
            Store(),
            own_key(work_dir, bank),
            public_keys(work_dir, bank),
            {},
            cases=CaseStore(f"{work_dir}/cases-{bank}.db"),
        )
    for node in nodes.values():
        node.peers.update(nodes)
    return nodes


def run(workload: str | Path) -> tuple[list[Case], dict[str, int]]:
    """Run every bank's rules, searches and case store, as the cluster does after a
    replay settles; return (cases, search state counts)."""
    with tempfile.TemporaryDirectory() as work_dir:
        nodes = start_nodes(workload, work_dir)
        for node in nodes.values():
            while node.consume_hits(block_ms=None):
                pass
        # the grace is over: unresolved hits post local cases; twice, so each bank
        # stores the results the others delivered after its own pass
        for node in [*nodes.values()] * 2:
            node.housekeeping()
        searches = [s for node in nodes.values() for s in node.states.values()]
        cases = [case for node in nodes.values() for case in bank_cases(node)]
    counts = {pb.State.Name(s): searches.count(s) for s in set(searches)}
    return cases, dict(sorted(counts.items()))


def bank_cases(node: OfflineNode) -> list[Case]:
    """This bank's live cases with a tier, by its own payment IDs, as its UI shows them."""
    return [
        Case(node.bank, c["tier"], {leg["payment_id"] for leg in c["legs"]})
        for c in (node.cases.get(c["case_id"]) for c in node.cases.cases())
    ]
