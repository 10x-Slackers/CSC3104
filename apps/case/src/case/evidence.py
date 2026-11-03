"""Trusted signing keys and the shared result check used by both receivers."""

from pathlib import Path

from privacy_node.claims import check_result
from security import load_verification_keys

__all__ = ["check_result", "load_public_keys"]


def load_public_keys(folder):
    """Re-read trusted <folder>/<bank>/<signing_version>.pem files."""
    return load_verification_keys(
        {(p.parent.name, p.stem): p for p in Path(folder).glob("*/*.pem")}
    )
