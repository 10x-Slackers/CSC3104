"""Zero trust over real gRPC and TLS: who a caller is comes from its certificate alone."""

import socket

import grpc
import pytest
from conftest import Authority, settle

from mulechain.node import protocol_pb2 as pb
from mulechain.node.node import Mtls, identity, serve


@pytest.fixture
def bank_b(nodes, tmp_path):
    """Bank B's node behind real mTLS, and a function to call it with any certificate."""
    settle(nodes)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    lab, peers = Authority(), {"B": f"127.0.0.1:{port}"}
    server = serve(nodes["B"], Mtls(lab.issue(tmp_path / "b", identity("B")), peers), port)
    token = nodes["B"].bank.token("p06", "in")  # p06 went from A to B
    request = pb.TraceRequest(token=token, direction=pb.DOWNSTREAM, hops_remaining=2)

    def trace(directory):
        return Mtls(directory, peers).call("B", "Trace", request, 5)

    yield lab, trace, tmp_path
    server.stop(0)


def test_the_certificate_decides_what_a_caller_may_ask(bank_b):
    lab, trace, tmp = bank_b
    assert trace(lab.issue(tmp / "a", identity("A"))).claims
    assert not trace(lab.issue(tmp / "c", identity("C"))).claims  # C isn't on that transfer


def test_a_certificate_that_names_no_bank_is_unauthenticated(bank_b):
    lab, trace, tmp = bank_b
    with pytest.raises(grpc.RpcError) as refused:
        trace(lab.issue(tmp / "scorer", "spiffe://csc3104/bank/A/scorer"))
    assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED


def test_a_certificate_from_another_authority_cannot_connect(bank_b):
    _, trace, tmp = bank_b
    stranger = Authority().issue(tmp / "stranger", identity("A"))
    (stranger / "ca.crt").write_bytes((tmp / "b" / "ca.crt").read_bytes())  # it trusts the lab
    with pytest.raises(grpc.RpcError) as refused:
        trace(stranger)
    assert refused.value.code() == grpc.StatusCode.UNAVAILABLE


def test_a_renewed_certificate_is_served_without_a_restart(bank_b):
    lab, trace, tmp = bank_b
    renewed = Authority().issue(tmp / "new", identity("B"))
    for name in ("tls.crt", "tls.key"):  # what cert-manager does to the mounted Secret
        (tmp / "b" / name).write_bytes((renewed / name).read_bytes())
    client = lab.issue(tmp / "a", identity("A"))
    (client / "ca.crt").write_bytes((renewed / "ca.crt").read_bytes())  # accepts only the new one
    assert trace(client).claims
