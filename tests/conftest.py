import datetime
import ipaddress
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from mulechain import evaluate

TINY = Path(__file__).parent.parent / "workloads" / "tiny"


def settle(nodes) -> None:
    """Let every bank work until nothing is left to do."""
    while sum(node.work_once() for node in nodes.values()):
        pass


def retry_now(nodes) -> None:
    """Skip the backoff waits."""
    for node in nodes.values():
        node.store.db.run("UPDATE searches SET next_at = 0")
        node.store.db.run("UPDATE outbox SET next_at = 0")


def cases(nodes) -> list[tuple[str, int, str]]:
    """(bank, tier, the bank's own payment IDs) per case, as each bank's analyst sees them."""
    found = []
    for bank, node in nodes.items():
        payments = node.cases.payments()
        found += [(bank, c["tier"], " ".join(payments[c["id"]])) for c in node.cases.cases()]
    return sorted(found)


@pytest.fixture
def nodes(tmp_path):
    """The three banks with the tiny workload scored, before any node has worked."""
    return evaluate.start(TINY, tmp_path)


class Authority:
    """A throwaway certificate authority, as cert-manager's lab CA is in the cluster."""

    def __init__(self) -> None:
        self.key = ec.generate_private_key(ec.SECP256R1())
        self.name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test CA")])
        self.certificate = self._sign(self.name, self.key.public_key(), ca=True)

    def _sign(self, subject, public_key, ca=False, names=()):
        now = datetime.datetime.now(datetime.UTC)
        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(self.name)
            .public_key(public_key)
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=1))
            .not_valid_after(now + datetime.timedelta(hours=1))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        )
        if names:
            builder = builder.add_extension(x509.SubjectAlternativeName(names), critical=False)
        return builder.sign(self.key, hashes.SHA256())

    def issue(self, directory: Path, uri: str | None) -> Path:
        """Write tls.crt, tls.key and ca.crt for a workload, as cert-manager mounts them."""
        key = ec.generate_private_key(ec.SECP256R1())
        names = [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
        names += [x509.UniformResourceIdentifier(uri)] if uri else []
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "workload")])
        certificate = self._sign(subject, key.public_key(), names=names)
        pem = serialization.Encoding.PEM
        directory.mkdir()
        (directory / "tls.crt").write_bytes(certificate.public_bytes(pem))
        (directory / "ca.crt").write_bytes(self.certificate.public_bytes(pem))
        (directory / "tls.key").write_bytes(
            key.private_bytes(pem, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        )
        return directory
