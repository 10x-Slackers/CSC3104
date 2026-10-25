"""Generate laboratory keys for three banks into a git-ignored folder.

  uv run python -m apps.privacy_node.keys .secrets

Writes pairs/<X>-<Y>.key (32 random bytes per bank pair), signing/<X>.pem and
public/<X>.pem. Load them into Kubernetes Secrets; never commit them.
"""

import os
import sys
from itertools import combinations
from pathlib import Path

from apps.security.security import generate_signing_key, private_key_pem, public_key_pem

BANKS = ("A", "B", "C")


def make_keys(root):
    root = Path(root)
    for sub in ("pairs", "signing", "public"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    for a, b in combinations(BANKS, 2):
        (root / "pairs" / f"{a}-{b}.key").write_bytes(os.urandom(32))
    for bank in BANKS:
        key = generate_signing_key()
        (root / "signing" / f"{bank}.pem").write_bytes(private_key_pem(key))
        (root / "public" / f"{bank}.pem").write_bytes(public_key_pem(key.public_key()))
    return root


def pair_paths(root, bank):
    """{(bank, peer): path} for load_pairwise_keys / TokenIndex."""
    root = Path(root)
    return {
        (bank, peer): root / "pairs" / "{}-{}.key".format(*sorted((bank, peer)))
        for peer in BANKS
        if peer != bank
    }


if __name__ == "__main__":
    print(make_keys(sys.argv[1] if len(sys.argv) > 1 else ".secrets"))
