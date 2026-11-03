"""Real local TLS handshakes; all test-only keys live in pytest's temporary dir."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from privacy_node import privacy_node_pb2 as pb
from privacy_node.node import Node, serve
from privacy_node.transport import TlsFiles, authenticated_bank


def authority():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-only lab CA")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def material(directory, ca, bank, *, uri=True, hostname="localhost"):
    directory.mkdir(exist_ok=True)
    ca_key, ca_cert = ca
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    sans = [x509.DNSName(hostname)]
    if uri:
        sans.append(
            x509.UniformResourceIdentifier(f"spiffe://csc3104/bank/{bank}/privacy-node")
        )
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, bank)]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName(sans), critical=False)
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    (directory / "tls.crt").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (directory / "tls.key").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    (directory / "ca.crt").write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    return TlsFiles(directory / "tls.crt", directory / "tls.key", directory / "ca.crt")


@pytest.fixture
def tls_server(tmp_path):
    ca = authority()
    tls = material(tmp_path / "server", ca, "B")
    server = grpc.server(ThreadPoolExecutor(max_workers=2))

    def check(sender, context):
        return authenticated_bank(context, sender.decode()).encode()

    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "test.Bank", {"Check": grpc.unary_unary_rpc_method_handler(check)}
            ),
        )
    )
    port = server.add_secure_port("localhost:0", tls.server_credentials())
    server.start()
    yield f"localhost:{port}", ca, tls
    server.stop(0).wait()


def call(channel, sender=b"A"):
    return channel.unary_unary("/test.Bank/Check")(sender, timeout=3)


def test_trusted_bank_identity(tls_server, tmp_path):
    target, ca, _ = tls_server
    client = material(tmp_path / "client", ca, "A")
    with client.channel(target) as channel:
        assert call(channel) == b"A"


@pytest.mark.parametrize(
    "bank,uri,code",
    [
        ("C", True, grpc.StatusCode.PERMISSION_DENIED),
        ("A", False, grpc.StatusCode.UNAUTHENTICATED),
    ],
)
def test_wrong_bank_or_missing_bank_uri_rejected(tls_server, tmp_path, bank, uri, code):
    target, ca, _ = tls_server
    client = material(tmp_path / "client", ca, bank, uri=uri)
    with client.channel(target) as channel, pytest.raises(grpc.RpcError) as error:
        call(channel)
    assert error.value.code() == code


def test_no_client_certificate_rejected(tls_server):
    target, _, tls = tls_server
    ca = tls.ca.read_bytes()
    with (
        grpc.secure_channel(target, grpc.ssl_channel_credentials(ca)) as channel,
        pytest.raises(grpc.RpcError) as error,
    ):
        call(channel)
    assert error.value.code() == grpc.StatusCode.UNAVAILABLE


def test_untrusted_client_certificate_rejected(tls_server, tmp_path):
    target, _, tls = tls_server
    client = material(tmp_path / "client", authority(), "A")
    # Trust the real server while presenting a client certificate from another CA.
    client.ca.write_bytes(tls.ca.read_bytes())
    with client.channel(target) as channel, pytest.raises(grpc.RpcError) as error:
        call(channel)
    assert error.value.code() == grpc.StatusCode.UNAVAILABLE


def test_hostname_verification_is_enabled(tmp_path):
    ca = authority()
    tls = material(tmp_path / "server", ca, "B", hostname="wrong.example")
    server = grpc.server(ThreadPoolExecutor(max_workers=1))
    port = server.add_secure_port("localhost:0", tls.server_credentials())
    server.start()
    client = material(tmp_path / "client", ca, "A")
    try:
        with (
            client.channel(f"localhost:{port}") as channel,
            pytest.raises(grpc.RpcError) as error,
        ):
            call(channel)
        assert error.value.code() == grpc.StatusCode.UNAVAILABLE
    finally:
        server.stop(0).wait()


def test_server_and_client_reload_mounted_material_without_restart(
    tls_server, tmp_path
):
    target, ca, server_tls = tls_server
    client = material(tmp_path / "client", ca, "A")
    with client.channel(target) as channel:
        assert call(channel) == b"A"
    # Renew both ends under a fresh CA: success requires server AND client reload.
    renewed_ca = authority()
    material(server_tls.certificate.parent, renewed_ca, "B")
    material(client.certificate.parent, renewed_ca, "A")
    with client.channel(target) as channel:
        assert call(channel) == b"A"
    # A renewed client for C cannot keep claiming to be A.
    material(client.certificate.parent, renewed_ca, "C")
    with client.channel(target) as channel, pytest.raises(grpc.RpcError) as error:
        call(channel)
    assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED


def test_node_transport_binds_both_methods_before_reading_state(tmp_path):
    ca = authority()
    server_tls = material(tmp_path / "server", ca, "B")
    client_tls = material(tmp_path / "client", ca, "A")
    store = Mock()
    store.get_reply.return_value = pb.TraceReply(state=pb.NO_MATCH)
    receiver = Node(
        SimpleNamespace(bank="B"), store, None, {}, {}, "k1", tls=server_tls
    )
    server, port = serve(receiver)
    sender = Node(
        SimpleNamespace(bank="A"),
        Mock(),
        None,
        {},
        {"B": f"localhost:{port}"},
        "k1",
        tls=client_tls,
    )
    request = pb.TraceRequest(sender="A", query_id="cached-query")
    try:
        assert sender.call("B", "Trace", request, 3).state == pb.NO_MATCH
        store.get_reply.assert_called_once()
        store.reset_mock()
        material(client_tls.certificate.parent, ca, "C")
        for method, message in [
            ("Trace", request),
            ("Deliver", pb.ChainResult(sender="A")),
        ]:
            with pytest.raises(grpc.RpcError) as error:
                sender.call("B", method, message, 3)
            assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
        store.get_reply.assert_not_called()
        store.save_chain.assert_not_called()
    finally:
        server.stop(0).wait()
