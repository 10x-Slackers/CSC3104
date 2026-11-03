"""Attack cases for the authoritative result validator and the peer schema."""

from pathlib import Path

import grpc
import pytest
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.text_format import Parse

from privacy_node import privacy_node_pb2 as pb
from privacy_node.claims import (
    APPROVED_FIELDS,
    chain_digest,
    check_result,
    claim_bytes,
    reject_unknown_fields,
    valid_claim,
)
from privacy_node.demo import start_banks
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
    assert "deadline" not in pb.TraceRequest.DESCRIPTOR.fields_by_name


def test_unknown_fields_rejected_recursively_by_receivers(banks):
    request = pb.TraceRequest(query_id="unknown", sender="B")
    request.ref.CopyFrom(pb.TokenRef.FromString(b"\x98\x06\x01"))
    with pytest.raises(ValueError, match="unknown protobuf"):
        reject_unknown_fields(request)
    with pytest.raises(ValueError, match="unknown protobuf"):
        banks["B"].call("C", "Trace", request, 5)


def test_unknown_wire_field_rejected_by_grpc_receiver(banks):
    result = banks["B"].search("p06", "p07")
    target = banks["B"].peers["C"]
    with grpc.insecure_channel(target) as channel:
        for method, payload in (
            ("Trace", pb.TraceRequest(sender="B").SerializeToString()),
            ("Deliver", result.SerializeToString()),
        ):
            call = channel.unary_unary(
                f"/csc3104.privacy.v1.PrivacyNode/{method}",
                request_serializer=lambda value: value,
                response_deserializer=lambda value: value,
            )
            with pytest.raises(grpc.RpcError) as error:
                call(payload + b"\x98\x06\x01", timeout=5)
            assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_shared_validator_accepts_confirmed_and_partial(banks):
    for payment, outgoing, state in (
        ("p06", "p07", pb.CONFIRMED),
        ("p15", "p16", pb.PARTIAL),
    ):
        result = banks["B"].search(payment, outgoing)
        assert check_result(result, banks["B"].public_keys, "B").state == state


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


def test_shared_sample_matches_producer_and_consumer(banks):
    sample_path = (
        Path(__file__).parents[3] / "docs/contracts/samples/trace_request.textproto"
    )
    sample = Parse(sample_path.read_text(encoding="utf-8-sig"), pb.TraceRequest())
    producer = banks["A"].request("sample-q1", sample.ref, pb.DOWNSTREAM, 3, [], "k1")
    assert producer == sample
    assert sample.ref.token.hex() == "86ef4e3f811c077dcde10d9cef4d807c"
    consumed = pb.TraceRequest.FromString(sample.SerializeToString())
    assert banks["B"].Trace(consumed, None).state == pb.NO_MATCH


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
        banks["B"].call("C", "Deliver", result, 5)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert banks["C"].store.chains() == []
