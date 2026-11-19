"""All three banks end to end on the tiny workload (workloads/tiny/transfers.csv).

Its chains: m1 A->B->C but only B's rule fires; o1 a loop through all three; v1 six
transfers, longer than the hop limit; x1 with a missing middle transfer; l1 a
lookalike whose amounts don't match.
"""

import grpc
import pytest
from conftest import cases, retry_now

from mulechain.evaluate import Context, Refused, settle
from mulechain.node import protocol_pb2 as pb
from mulechain.node.claims import digest
from mulechain.node.node import KEY_SETTLE_SECONDS

EXPECTED = [
    ("A", 1, "p06"),
    ("A", 1, "p08"),
    ("A", 1, "p13"),
    ("A", 2, "p10 p12"),
    ("A", 2, "p15 p17 p18 p20"),
    ("B", 1, "p05"),
    ("B", 1, "p06 p07"),
    ("B", 1, "p09"),
    ("B", 2, "p10 p11"),
    ("B", 2, "p15 p16 p18 p19"),
    ("C", 1, "p03"),
    ("C", 1, "p14"),
    ("C", 2, "p11 p12"),
    ("C", 2, "p16 p17 p19 p20"),
]


def chains(node) -> list[pb.Chain]:
    return [pb.Chain.FromString(row["chain"]) for row in node.store.db.all("SELECT * FROM chains")]


def test_every_bank_ends_with_its_own_view_of_the_same_chains(nodes):
    settle(nodes)
    assert cases(nodes) == EXPECTED
    # The loop is one confirmed chain however many banks started a search for it.
    loop = [
        c for c in chains(nodes["A"]) if len(c.claims) == 4 and not any(x.cut for x in c.claims)
    ]
    assert len({digest(list(c.claims)) for c in loop}) == 1
    for node in nodes.values():
        assert not node.store.db.all("SELECT 1 FROM outbox WHERE state != 'sent'")


def test_nothing_but_tokens_crosses_between_banks(nodes):
    wire = []
    for node in nodes.values():

        def record(peer, method, request, timeout, call=node.call):
            reply = call(peer, method, request, timeout)
            wire.extend([request.SerializeToString(), reply.SerializeToString()])
            return reply

        node.call = record
    settle(nodes)
    sent = b"".join(wire)
    accounts = {
        leg["account"] for n in nodes.values() for leg in n.cases.db.all("SELECT * FROM legs")
    }
    assert len(wire) > 20 and len(accounts) > 10
    assert not [account for account in accounts if account.encode() in sent]
    fields = {f.name for m in pb.DESCRIPTOR.message_types_by_name.values() for f in m.fields}
    assert fields == {
        *("agreement", "signing", "keys", "token", "direction", "hops_remaining", "visited"),
        *("signer", "in_from", "in_token", "out_to", "out_token", "cut", "signature", "claims"),
    }


def test_a_bank_that_is_down_delays_cases_but_loses_none(nodes):
    down = True

    for node in nodes.values():

        def call(peer, method, request, timeout, call=node.call):
            if down and peer == "C":
                raise Refused(grpc.StatusCode.UNAVAILABLE, "bank C is down")
            return call(peer, method, request, timeout)

        node.call = call
    for bank in "AB":
        settle({bank: nodes[bank]})
    waiting = nodes["B"].cases.cases()
    assert {c["state"] for c in waiting} >= {"pending"}
    assert {c["waiting"] for c in waiting if c["state"] == "pending"} == {"C"}
    assert not [c for bank in "AB" for c in nodes[bank].cases.cases() if c["tier"] == 2]

    down = False
    retry_now(nodes)
    settle(nodes)
    retry_now(nodes)  # chains queued for C while it was down
    settle(nodes)
    assert cases(nodes) == EXPECTED


def test_a_forged_chain_is_refused_and_a_repeated_one_is_harmless(nodes):
    settle(nodes)
    held = chains(nodes["A"])
    chain = next(c for c in held if {x.signer for x in c.claims} == {"A", "B", "C"})
    nodes["A"].Deliver(chain, Context("B", 5))  # delivered again: nothing new
    assert len(chains(nodes["A"])) == len(held)

    forged = pb.Chain.FromString(chain.SerializeToString())
    forged.claims[0].cut = not forged.claims[0].cut  # no longer what the signer signed
    with pytest.raises(Refused) as refused:
        nodes["A"].Deliver(forged, Context("B", 5))
    assert refused.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_a_chain_is_not_refused_just_because_a_signers_keys_cannot_be_fetched(nodes):
    """Refusing means a dead letter at the sender, so "can't check yet" must not refuse."""
    settle(nodes)
    node, chain = nodes["A"], chains(nodes["A"])[0]

    def unreachable(peer, method, request, timeout):
        raise Refused(grpc.StatusCode.UNAVAILABLE, "")

    node.call, node.keys = unreachable, {}  # a replica that has just started, cut off
    node.store.db.run("DELETE FROM peer_keys")
    with pytest.raises(Refused) as refused:
        node.Deliver(chain, Context("B", 5))
    assert refused.value.code() == grpc.StatusCode.UNAVAILABLE


def test_a_refused_delivery_becomes_a_dead_letter_and_others_are_retried(nodes):
    store = nodes["A"].store
    store.queue("B", "bad", pb.Chain().SerializeToString())
    store.queue("C", "lost", pb.Chain().SerializeToString())

    def call(peer, method, request, timeout):
        code = grpc.StatusCode.INVALID_ARGUMENT if peer == "B" else grpc.StatusCode.UNAVAILABLE
        raise Refused(code, "")

    nodes["A"].call = call
    nodes["A"].deliver()
    states = {row["key"]: row["state"] for row in store.db.all("SELECT * FROM outbox")}
    assert states == {"bad": "dead", "lost": "queued"}


def test_a_key_change_is_published_only_after_it_has_settled(nodes):
    node = nodes["B"]
    (node.key_dir / "2").write_bytes(bytes(64))
    node.refresh_keys()
    assert len(node.own) == 2  # used here at once
    assert len(node.Keys(pb.Empty(), None).keys) == 1  # but peers aren't told yet
    node.latest_at -= KEY_SETTLE_SECONDS
    node.refresh_keys()
    assert len(node.Keys(pb.Empty(), None).keys) == 2
