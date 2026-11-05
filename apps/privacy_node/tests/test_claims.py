"""Attack cases for the authoritative result validator and the peer schema."""

from datetime import timedelta
from pathlib import Path

import grpc
import pytest
from google.protobuf.descriptor import FieldDescriptor

from privacy_node import privacy_node_pb2 as pb
from privacy_node.claims import (
    APPROVED_FIELDS,
    chain_digest,
    check_result,
    claim_bytes,
    valid_chain,
    valid_claim,
)
from privacy_node.demo import start_banks
from privacy_node.node import now
from security import FORBIDDEN_FIELDS, generate_signing_key, sign

TINY = Path(__file__).parents[2] / "dataset/fixtures/tiny"


@pytest.fixture
def banks(tmp_path):
    nodes, servers = start_banks(TINY, tmp_path)
    yield nodes
    for server in servers.values():
        server.stop(0)


def resign(result, nodes):
    for c in result.claims:
        c.signature = sign(claim_bytes(c), nodes[c.signer].signing_key)
    result.chain_digest = chain_digest(result.claims)


def test_schema_explicitly_approves_every_recursive_peer_field():
    seen = set()

    def visit(descriptor):
        if descriptor.full_name in seen:
            return
        seen.add(descriptor.full_name)
        actual = {f.name for f in descriptor.fields}
        assert actual == APPROVED_FIELDS[descriptor.name]
        assert not actual & FORBIDDEN_FIELDS
        for field in descriptor.fields:
            if field.message_type is not None:
                visit(field.message_type)

    for service in pb.DESCRIPTOR.services_by_name.values():
        for method in service.methods:
            visit(method.input_type)
            visit(method.output_type)
    assert {name.rsplit(".", 1)[1] for name in seen} == set(APPROVED_FIELDS)
    assert (
        pb.TokenRef.DESCRIPTOR.fields_by_name["token"].type
        == FieldDescriptor.TYPE_BYTES
    )


def test_shared_validator_accepts_confirmed_and_partial(banks):
    for payment, outgoing, state in (
        ("p06", "p07", pb.CONFIRMED),
        ("p15", "p16", pb.PARTIAL),
    ):
        result = banks["B"].search(payment, outgoing)
        assert {c.signer for c in result.claims} == {"A", "B", "C"}
        assert check_result(result, banks["B"].public_keys, "B").state == state


def test_one_bank_cannot_sign_both_sides_of_a_shared_transfer(banks):
    result = banks["B"].search("p06", "p07")
    # Add an A-B transfer allegedly confirmed on both sides by A. All signatures
    # and query/recipient bindings remain genuine; B never vouched for this token.
    unconfirmed = pb.TokenRef(pair="A-B", token=b"x" * 16)
    first = result.claims[0]
    first.in_ref.CopyFrom(unconfirmed)
    result.claims.insert(
        0, banks["A"].claim("A", first.query_id, pb.TokenRef(), unconfirmed)
    )
    resign(result, banks)
    keys = banks["B"].public_keys
    assert all(valid_claim(c, keys) for c in result.claims)
    assert not valid_chain(result.claims, keys)
    with pytest.raises(ValueError, match="invalid chain result"):
        check_result(result, keys, "B")
    with pytest.raises(grpc.RpcError) as error:
        banks["B"].call("C", "Deliver", result, now() + timedelta(seconds=5))
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert banks["C"].store.chains() == []


def test_one_claim_cannot_count_the_same_transfer_as_both_sides(banks):
    ref = pb.TokenRef(pair="A-B", token=b"x" * 16)
    claim = banks["A"].claim("A", "self", ref, ref)
    result = pb.ChainResult(
        sender="A",
        state=pb.CONFIRMED,
        claims=[claim],
        chain_digest=chain_digest([claim]),
    )
    assert not valid_claim(claim, banks["A"].public_keys)
    with pytest.raises(ValueError, match="invalid chain result"):
        check_result(result, banks["A"].public_keys, "A")


def test_one_bank_cannot_close_a_loop_with_its_own_unconfirmed_endpoints(banks):
    ab = pb.TokenRef(pair="A-B", token=b"x" * 16)
    ac_in = pb.TokenRef(pair="A-C", token=b"y" * 16)
    ac_out = pb.TokenRef(pair="A-C", token=b"z" * 16)
    claims = [
        banks["A"].claim("C", "up", ab, ac_in),
        banks["C"].claim("C", "anchor", ac_in, ac_out),
        banks["A"].claim("C", "down", ac_out, ab),
    ]
    result = pb.ChainResult(
        sender="C", state=pb.CONFIRMED, claims=claims, chain_digest=chain_digest(claims)
    )
    keys = banks["C"].public_keys
    assert all(valid_claim(c, keys) for c in claims)
    # The interior A-C transfers are independently signed. Only A signed the
    # repeated A-B endpoint, so it cannot turn this cut into a confirmed loop.
    assert not valid_chain(claims, keys)
    with pytest.raises(ValueError, match="invalid chain result"):
        check_result(result, keys, "C")
    with pytest.raises(grpc.RpcError) as error:
        banks["C"].call("A", "Deliver", result, now() + timedelta(seconds=5))
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert banks["A"].store.chains() == []


def test_legitimate_loop_with_repeated_banks_keeps_independent_witnesses(banks):
    for bank, incoming, outgoing in (("B", "p10", "p11"), ("C", "p11", "p12")):
        result = banks[bank].search(incoming, outgoing)
        signers = [c.signer for c in result.claims]
        assert len(signers) > len(set(signers))
        assert check_result(result, banks[bank].public_keys, bank).state == pb.CONFIRMED


@pytest.mark.parametrize(
    "attack",
    [
        "tamper",
        "wrong_signer",
        "missing_key",
        "wrong_version",
        "digest",
        "uninvolved",
        "anchor",
        "query",
        "recipient",
    ],
)
def test_shared_result_attack_cases(banks, attack):
    result = banks["B"].search("p06", "p07")
    keys = dict(banks["B"].public_keys)
    bank = "A"
    if attack == "tamper":
        result.claims[0].out_ref.token = b"x" * 16
    elif attack == "wrong_signer":
        result.claims[0].signer = "C"
        resign(result, banks)
    elif attack == "missing_key":
        del keys["A", "k1"]
    elif attack == "wrong_version":
        result.claims[0].signing_key_version = "unavailable"
        resign(result, banks)
    elif attack == "digest":
        result.chain_digest = "0" * 64
    elif attack == "uninvolved":
        bank = "D"
    elif attack == "anchor":
        result.sender = "A"  # A's claim was signed for B, not itself.
    elif attack == "query":
        # Two claims in one downstream leg must use the same query ID.
        result = banks["B"].search("p10", "p11")
        anchor = next(
            i for i, c in enumerate(result.claims) if c.signer == c.recipient == "B"
        )
        result.claims[anchor + 2].query_id = "other-query"
        resign(result, banks)
    elif attack == "recipient":
        result.claims[0].recipient = "C"
        resign(result, banks)
    with pytest.raises(ValueError):
        check_result(result, keys, bank)


def test_cut_confirmed_returns_partial_copy_without_mutating_input(banks):
    result = banks["B"].search("p15", "p16")
    result.state = pb.CONFIRMED
    checked = check_result(result, banks["B"].public_keys, "C")
    assert checked.state == pb.PARTIAL
    assert result.state == pb.CONFIRMED


def test_old_and_new_signing_keys_coexist_and_wrong_version_fails(banks):
    node = banks["B"]
    old = node.search("p06", "p07").claims[1]
    new_key = generate_signing_key()
    keys = dict(node.public_keys)
    keys["B", "k2"] = new_key.public_key()
    new = pb.LinkClaim()
    new.CopyFrom(old)
    new.signing_key_version = "k2"
    new.signature = sign(claim_bytes(new), new_key)
    assert valid_claim(old, keys) and valid_claim(new, keys)
    del keys["B", "k1"]
    assert not valid_claim(old, keys) and valid_claim(new, keys)
    new.signing_key_version = "k1"
    assert not valid_claim(new, keys)


def test_raw_token_roundtrips_without_text_encoding():
    token = bytes.fromhex("86ef4e3f811c077dcde10d9cef4d807c")
    ref = pb.TokenRef(pair="A-B", token=token)
    assert pb.TokenRef.FromString(ref.SerializeToString()).token == token


def test_signed_branch_cut_cannot_create_confirmed_evidence(banks):
    result = banks["B"].search("p06", "p07")
    result.claims[1].truncated = True
    resign(result, banks)
    # These natural endpoints would otherwise appear complete.
    assert result.state == pb.CONFIRMED
    assert check_result(result, banks["B"].public_keys, "A").state == pb.PARTIAL
    result.claims[1].truncated = False  # Removing the signed cut invalidates it.
    with pytest.raises(ValueError):
        check_result(result, banks["B"].public_keys, "A")


def test_disconnected_signed_fragments_are_rejected_by_validator_and_deliver(banks):
    result = banks["B"].search("p06", "p07")
    split_ref = pb.TokenRef()
    split_ref.CopyFrom(result.claims[1].out_ref)
    result.claims[1].ClearField("out_ref")
    tail = result.claims[2]
    tail.ClearField("in_ref")
    tail.out_ref.CopyFrom(split_ref)
    result.claims.append(banks["B"].claim("C", tail.query_id, split_ref, pb.TokenRef()))
    resign(result, banks)
    # Every signature is genuine; there is no transfer connecting the two fragments.
    with pytest.raises(ValueError, match="invalid chain result"):
        check_result(result, banks["B"].public_keys, "C")
    with pytest.raises(grpc.RpcError) as error:
        banks["B"].call("C", "Deliver", result, now() + timedelta(seconds=5))
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert banks["C"].store.chains() == []
