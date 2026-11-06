import json
from pathlib import Path
from shutil import copyfile

import pytest

from privacy_node.keys import make_keys, own_key, pair_paths, public_keys
from security import load_pairwise_keys, private_key_pem, public_key_pem, sign, verify


def trust_public(root, recipient, peer, version):
    for purpose in ("x25519", "signing"):
        source = root / peer / "public" / purpose / f"{version}.pem"
        target = root / recipient / "trusted" / purpose / peer / source.name
        target.parent.mkdir(parents=True, exist_ok=True)
        copyfile(source, target)


def test_generator_is_local_and_only_public_files_are_shared(tmp_path):
    make_keys(tmp_path, "A")
    assert {p.name for p in tmp_path.iterdir()} == {"A"}
    assert {
        p.relative_to(tmp_path / "A").as_posix() for p in tmp_path.rglob("*.pem")
    } == {
        "private/x25519/k1.pem",
        "private/signing/k1.pem",
        "public/x25519/k1.pem",
        "public/signing/k1.pem",
    }
    make_keys(tmp_path, "B")
    trust_public(tmp_path, "A", "B", "k1")
    trust_public(tmp_path, "B", "A", "k1")
    derived = {}
    for bank in "AB":
        files = pair_paths(tmp_path, bank)
        assert Path(files.private_key).is_relative_to(tmp_path / bank / "private")
        assert all("trusted" in Path(p).parts for p in files.peer_public_keys.values())
        derived[bank] = load_pairwise_keys(
            bank, "k1", files.private_key, files.peer_public_keys
        )
        for public in (tmp_path / bank / "trusted").rglob("*.pem"):
            assert b"PRIVATE KEY" not in public.read_bytes()
    assert derived["A"] == derived["B"]
    original = (tmp_path / "A/private/x25519/k1.pem").read_bytes()
    with pytest.raises(FileExistsError):
        make_keys(tmp_path, "A")
    assert (tmp_path / "A/private/x25519/k1.pem").read_bytes() == original


def test_signing_key_versions_coexist_and_files_reload(tmp_path):
    for version in ("k1", "k2"):
        make_keys(tmp_path, "A", version)
        make_keys(tmp_path, "B", version)
        trust_public(tmp_path, "A", "B", version)
    trusted = public_keys(tmp_path, "A")
    assert set(trusted) == {("A", "k1"), ("A", "k2"), ("B", "k1"), ("B", "k2")}
    message = b"test-only bank claim"
    for version in ("k1", "k2"):
        signature = sign(message, own_key(tmp_path, "B", version))
        assert verify(message, signature, trusted["B", version])
        other = "k2" if version == "k1" else "k1"
        assert not verify(message, signature, trusted["B", other])
    assert private_key_pem(own_key(tmp_path, "A", "k1")) != private_key_pem(
        own_key(tmp_path, "A", "k2")
    )
    # Removing a retired trusted file takes effect on the next explicit reload.
    (tmp_path / "A/trusted/signing/B/k1.pem").unlink()
    reloaded = public_keys(tmp_path, "A")
    assert ("B", "k1") not in reloaded
    assert public_key_pem(reloaded["B", "k2"]) == public_key_pem(trusted["B", "k2"])


def test_first_version_writes_a_manifest_that_later_versions_keep(tmp_path):
    make_keys(tmp_path, "B")
    make_keys(tmp_path, "B", "k2")
    assert json.loads((tmp_path / "B/active.json").read_text()) == {
        "active_versions": ["k1"],
        "query_version": "k1",
        "signing_key_version": "k1",
        "peer_versions": {"A": ["k1"], "C": ["k1"]},
    }
