"""Generate ONE bank's local keys; exchange only its public artifacts.

  uv run python -m privacy_node.keys .secrets A --key-version k1

Run independently at each bank. Provision peers' authenticated public files in
<root>/<bank>/trusted/{x25519,signing}/<peer>/<version>.pem. Never copy private/.
The first run also writes a single-version <root>/<bank>/active.json.
"""

import argparse
import json
from pathlib import Path

from security import (
    AgreementFiles,
    generate_agreement_key,
    generate_signing_key,
    load_signing_key,
    load_verification_keys,
    private_key_pem,
    public_key_pem,
)

BANKS = ("A", "B", "C")


def make_keys(root, bank, key_version="k1"):
    """Create fresh local material without replacing an existing version."""
    root = Path(root) / bank
    for purpose, key in (
        ("x25519", generate_agreement_key()),
        ("signing", generate_signing_key()),
    ):
        for visibility, value in (
            ("private", private_key_pem(key)),
            ("public", public_key_pem(key.public_key())),
        ):
            path = root / visibility / purpose / f"{key_version}.pem"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as target:
                target.write(value)
    manifest = root / "active.json"
    if not manifest.exists():
        peers = {peer: [key_version] for peer in BANKS if peer != bank}
        manifest.write_text(
            json.dumps(
                {
                    "active_versions": [key_version],
                    "query_version": key_version,
                    "signing_key_version": key_version,
                    "peer_versions": peers,
                }
            )
        )
    return root


def pair_paths(root, bank, key_version="k1"):
    """Paths to this bank's private key and already provisioned peer publics."""
    root = Path(root) / bank
    return AgreementFiles(
        root / "private" / "x25519" / f"{key_version}.pem",
        {
            peer.name: peer / f"{key_version}.pem"
            for peer in (root / "trusted" / "x25519").glob("*")
            if peer.name != bank and (peer / f"{key_version}.pem").is_file()
        },
    )


def public_keys(root, bank):
    """Re-read trusted verification files, including overlapping versions."""
    trusted = Path(root) / bank / "trusted" / "signing"
    paths = {(p.parent.name, p.stem): p for p in trusted.glob("*/*.pem")}
    own = Path(root) / bank / "public" / "signing"
    paths.update({(bank, p.stem): p for p in own.glob("*.pem")})
    return load_verification_keys(paths)


def own_key(root, bank, key_version="k1"):
    return load_signing_key(
        Path(root) / bank / "private" / "signing" / f"{key_version}.pem"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root")
    parser.add_argument("bank", choices=BANKS)
    parser.add_argument("--key-version", default="k1")
    args = parser.parse_args()
    print(make_keys(args.root, args.bank, args.key_version))


if __name__ == "__main__":
    main()
