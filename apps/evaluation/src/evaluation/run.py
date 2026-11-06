"""Run every bank's live rules and privacy node trace in one process; tier the cases."""

import csv
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from privacy_node import privacy_node_pb2 as pb
from privacy_node.claims import check_payload, key, refs
from privacy_node.keys import BANKS, make_keys, pair_paths
from privacy_node.local_state import LocalBank
from privacy_node.node import Node
from privacy_node.store import Store
from scorer.score import BANK_TRANSFERS
from security import load_signing_key, load_verification_key

KEY_VERSION = "k1"


@dataclass(eq=False)
class Case:
    bank: str
    tier: int
    ids: set


class InProcessNode(Node):
    """A privacy node that calls its peers directly instead of over gRPC."""

    def call(self, bank, method, request, deadline):
        check_payload(request)
        return getattr(self.peers[bank], method)(request, OfflineContext())


class OfflineContext:
    def abort(self, code, details):
        raise RuntimeError(f"peer rejected a call: {code} {details}")


def bank_transfers(workload, bank):
    """A bank file as replay sends it: bank transfers only."""
    with (Path(workload) / f"bank_{bank}.csv").open() as source:
        for event in csv.DictReader(source):
            if event["payment_format"] in BANK_TRANSFERS:
                yield event


def start_nodes(workload, key_dir):
    make_keys(key_dir)
    public_keys = {b: load_verification_key(f"{key_dir}/public/{b}.pem") for b in BANKS}
    nodes = {}
    for bank in BANKS:
        events = bank_transfers(workload, bank)
        local = LocalBank(bank, KEY_VERSION, pair_paths(key_dir, bank), events)
        signing_key = load_signing_key(f"{key_dir}/signing/{bank}.pem")
        nodes[bank] = InProcessNode(
            local, Store(), signing_key, public_keys, {}, KEY_VERSION
        )
    for node in nodes.values():
        node.peers.update(nodes)
    return nodes


def run(workload):
    """Run every bank's rules and searches; return (cases, search state counts)."""
    with tempfile.TemporaryDirectory() as key_dir:
        nodes = start_nodes(workload, key_dir)
    searches = Counter()
    for node in nodes.values():
        for result in node.search_detections():
            searches[pb.State.Name(result.state)] += 1
    payment_of = {}
    for node in nodes.values():
        for (payment_id, _), ref in node.local.refs.items():
            payment_of[key(ref)] = payment_id
    cases = []
    for bank, node in nodes.items():
        cases += bank_cases(bank, node, payment_of)
    return cases, dict(sorted(searches.items()))


def bank_cases(bank, node, payment_of):
    found = [(1, hit["payments"]) for hit in node.local.scorer.state.hits()]
    for chain in node.store.chains():
        tier = chain_tier(chain, bank)
        if tier:
            found.append((tier, [payment_of[key(ref)] for ref in refs(chain.claims)]))
    return merge(bank, found)


def chain_tier(chain, bank):
    """Tier 2 if two banks' rules link the chain, else Tier 1 where one did."""
    rule_backed = {c.signer for c in chain.claims if c.in_ref.token and c.out_ref.token}
    if len(rule_backed) >= 2:
        return 2
    return 1 if bank in rule_backed else None


def merge(bank, found):
    """Join results sharing a payment into one case at the highest tier (case.md)."""
    case_of = {}
    for tier, ids in found:
        case = Case(bank, tier, set(ids))
        for other in {case_of[p] for p in ids if p in case_of}:
            case.tier = max(case.tier, other.tier)
            case.ids |= other.ids
        for p in case.ids:
            case_of[p] = case
    return list(dict.fromkeys(case_of.values()))
