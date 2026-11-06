"""Run every bank's live rules and privacy node trace in one process; tier the cases."""

import csv
import tempfile
from collections import Counter
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from google.protobuf.message import Message

from evaluation.score import Case
from privacy_node import privacy_node_pb2 as pb
from privacy_node.claims import refs
from privacy_node.demo import prepare_demo_keys
from privacy_node.keys import BANKS, own_key, pair_paths, public_keys
from privacy_node.local_state import LocalBank
from privacy_node.node import Node
from privacy_node.store import Store
from scorer.score import BANK_TRANSFERS

KEY_VERSION = "k1"


class InProcessNode(Node):
    """A privacy node that calls its peers directly instead of over gRPC."""

    def call(
        self, bank: str, method: str, request: Message, deadline: datetime
    ) -> Message:
        return getattr(self.peers[bank], method)(request, OfflineContext())


class OfflineContext:
    def abort(self, code: object, details: str):
        raise RuntimeError(f"peer rejected a call: {code} {details}")


def bank_transfers(workload: str | Path, bank: str) -> Iterator[dict]:
    """A bank file as replay sends it: bank transfers only."""
    with (Path(workload) / f"bank_{bank}.csv").open() as source:
        for event in csv.DictReader(source):
            if event["payment_format"] in BANK_TRANSFERS:
                yield event


def start_nodes(workload: str | Path, key_dir: str) -> dict[str, InProcessNode]:
    prepare_demo_keys(key_dir)
    nodes, peers = {}, {}
    for bank in BANKS:
        events = bank_transfers(workload, bank)
        local = LocalBank(bank, KEY_VERSION, pair_paths(key_dir, bank), events)
        trusted = public_keys(key_dir, bank)
        nodes[bank] = InProcessNode(
            local, Store(), own_key(key_dir, bank), trusted, peers
        )
    peers.update(nodes)
    return nodes


def run(workload: str | Path) -> tuple[list[Case], dict[str, int]]:
    """Run every bank's rules and searches; return (cases, search state counts)."""
    with tempfile.TemporaryDirectory() as key_dir:
        nodes = start_nodes(workload, key_dir)
        searches: Counter[str] = Counter()
        for node in nodes.values():
            for result in node.search_detections():
                searches[pb.State.Name(result.state)] += 1
    cases: list[Case] = []
    for node in nodes.values():
        cases += bank_cases(node, nodes)
    return cases, dict(sorted(searches.items()))


def payment_id(ref: pb.TokenRef, nodes: dict[str, InProcessNode]) -> str | None:
    """Both banks of a pair index its transfers; ask the first one."""
    local = nodes[ref.pair.split("-")[0]].local
    return local.lookup(ref.pair, KEY_VERSION, ref.token, "out") or local.lookup(
        ref.pair, KEY_VERSION, ref.token, "in"
    )


def bank_cases(node: InProcessNode, nodes: dict[str, InProcessNode]) -> list[Case]:
    found = [(1, hit["payments"]) for hit in node.local.scorer.state.hits()]
    for chain in node.store.chains():
        tier = chain_tier(chain, node.bank)
        if tier:
            found.append((tier, [payment_id(r, nodes) for r in refs(chain.claims)]))
    return merge(node.bank, found)


def chain_tier(chain: pb.ChainResult, bank: str) -> int | None:
    """Tier 2 if two banks' rules link the chain, else Tier 1 where one did."""
    rule_backed = {c.signer for c in chain.claims if c.in_ref.token and c.out_ref.token}
    if len(rule_backed) >= 2:
        return 2
    return 1 if bank in rule_backed else None


def merge(bank: str, found: list[tuple[int, list[str]]]) -> list[Case]:
    """Join results sharing a payment into one case at the highest tier (case.md)."""
    case_of: dict[str, Case] = {}
    for tier, ids in found:
        case = Case(bank, tier, set(ids))
        for other in {case_of[p] for p in ids if p in case_of}:
            case.tier = max(case.tier, other.tier)
            case.ids |= other.ids
        for p in case.ids:
            case_of[p] = case
    return list(dict.fromkeys(case_of.values()))
