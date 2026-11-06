"""Peer mTLS: cert-manager files are re-read for new TLS connections."""

from dataclasses import dataclass
from pathlib import Path

import grpc

BANK_URIS = {
    f"spiffe://csc3104/bank/{bank}/privacy-node".encode(): bank for bank in "ABC"
}


def authenticated_bank(context: grpc.ServicerContext, sender: str) -> str:
    """Bind the message sender to a bank URI SAN authenticated by the lab CA."""
    auth = context.auth_context()
    banks = {
        BANK_URIS[san]
        for san in auth.get("x509_subject_alternative_name", ())
        if san in BANK_URIS
    }
    if auth.get("transport_security_type") != [b"ssl"] or len(banks) != 1:
        context.abort(grpc.StatusCode.UNAUTHENTICATED, "trusted bank URI SAN required")
    bank = banks.pop()
    if sender != bank:
        context.abort(
            grpc.StatusCode.PERMISSION_DENIED, "sender differs from bank URI SAN"
        )
    return bank


@dataclass(frozen=True)
class TlsFiles:
    certificate: str | Path
    private_key: str | Path
    ca: str | Path

    def read(self) -> tuple[bytes, bytes, bytes]:
        return (
            Path(self.certificate).read_bytes(),
            Path(self.private_key).read_bytes(),
            Path(self.ca).read_bytes(),
        )

    def channel(self, target: str) -> grpc.Channel:
        """A new channel uses current mounted credentials and verifies the target DNS."""
        certificate, private_key, ca = self.read()
        credentials = grpc.ssl_channel_credentials(ca, private_key, certificate)
        return grpc.secure_channel(target, credentials)

    def server_credentials(self) -> grpc.ServerCredentials:
        """gRPC asks for renewed material before each new connection's handshake."""

        def configuration():
            certificate, private_key, ca = self.read()
            return grpc.ssl_server_certificate_configuration(
                [(private_key, certificate)], ca
            )

        return grpc.dynamic_ssl_server_credentials(
            configuration(), configuration, require_client_authentication=True
        )
