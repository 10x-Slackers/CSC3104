"""Real chain results for Role 5 tests, from Role 3's privacy nodes on the tiny fixture.

DEPENDS ON Role 3 (privacy node): ChainResult/LinkClaim from privacy_node.proto and
demo.start_banks; every Role 5 test built on these helpers relies on that format.

Runs every local detection once on three in-process nodes (apps.privacy_node.demo) and
keeps the results, so tests use the same signed ChainResults a bank would receive.
"""

import csv
import re
import tempfile
from functools import cache
from pathlib import Path

from apps.case.case_store import transfers
from apps.privacy_node import privacy_node_pb2 as pb
from apps.privacy_node.claims import chain_digest, key
from apps.privacy_node.demo import start_banks

TINY = Path(__file__).parents[1] / "dataset/fixtures/tiny"


@cache
def searches():
    """({(bank, incoming, outgoing): ChainResult}, {bank: public key}, {transfer: id}).

    The last map turns a transfer key back into its fixture payment ID; only tests use
    it, to check which planted chain a case holds.
    """
    with tempfile.TemporaryDirectory() as key_dir:
        nodes, servers = start_banks(TINY, key_dir)
        try:
            results = {
                (bank, d["incoming_payment_id"], d["outgoing_payment_id"]): r
                for bank, node in nodes.items()
                for d, r in zip(node.local.detections, node.search_detections())
            }
            payments = {
                key(node.local.ref(*ref)): ref[0]
                for node in nodes.values()
                for ref in node.local.refs
            }
        finally:
            for server in servers.values():
                server.stop(0)
    return results, dict(nodes["A"].public_keys), payments


def result(bank, incoming, outgoing):
    """A copy, so a test can change it without affecting others."""
    copy = pb.ChainResult()
    copy.CopyFrom(searches()[0][bank, incoming, outgoing])
    return copy


def public_keys():
    return searches()[1]


def payment_ids(case):
    """Fixture payment IDs of the transfers in a stored case (test-side lookup only)."""
    return {searches()[2][k] for c in case["claims"] for k in transfers(c)}


def delivered_to(bank):
    """Every final result a bank signed: what its privacy node stores and hands on."""
    return [
        r
        for r in searches()[0].values()
        if r.state in (pb.CONFIRMED, pb.PARTIAL)
        and bank in {c.signer for c in r.claims}
    ]


def m1():
    """A -> B -> C, detected at B: confirmed, 2 transfers."""
    return result("B", "p06", "p07")


def o1(start="B"):
    """Loop A -> B -> C -> A, searched from B or C: confirmed, one digest."""
    return result("B", "p10", "p11") if start == "B" else result("C", "p11", "p12")


def v1():
    """Over-limit chain: five partial results, cut at different 5-transfer windows."""
    return [
        result(*k) for k in searches()[0] if k[1] in {"p15", "p16", "p17", "p18", "p19"}
    ]


def piece(source, start, stop, state=pb.PARTIAL):
    """A result made of some of another result's claims, with its digest recomputed."""
    claims = source.claims[start:stop]
    return pb.ChainResult(
        chain_digest=chain_digest(claims),
        sender=source.sender,
        state=state,
        claims=claims,
    )


def as_partial(source):
    copy = pb.ChainResult()
    copy.CopyFrom(source)
    copy.state = pb.PARTIAL
    return copy


def bridged():
    """Two confirmed pieces of one v1 result with no transfer in common, then the whole."""
    full = v1()[0]
    return (
        piece(full, 0, 2, pb.CONFIRMED),
        piece(full, 4, 6, pb.CONFIRMED),
        piece(full, 0, 6, pb.CONFIRMED),
    )


@cache
def sensitive_values():
    """Every payment ID, account, amount and payment format in the fixture's bank files."""
    values = set()
    for bank in "ABC":
        with (TINY / f"bank_{bank}.csv").open() as source:
            for row in csv.DictReader(source):
                fields = ("payment_id", "account", "amount", "payment_format")
                values |= {row[field] for field in fields}
    return values


def leaks(text):
    """Sensitive values found in text, as whole words: 120.0 inside 1793000120.05 is not one."""
    return {
        value
        for value in sensitive_values()
        if re.search(rf"(?<![\w.]){re.escape(value)}(?!\w)", text)
    }
