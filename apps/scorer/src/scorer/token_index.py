"""Bank-local raw-byte token references with overlapping key versions."""

from scorer.score import validate
from security import (
    canonical_pair,
    derive_token,
    load_agreement_key,
    load_agreement_public_key,
    load_pairwise_keys,
    public_key_pem,
)


class TokenIndex:
    def __init__(self, bank, key_version, key_paths, max_references=None):
        if bank not in ("A", "B", "C"):
            raise ValueError("Invalid local bank")
        if max_references is not None and max_references < 1:
            raise ValueError("Token reference limit must be positive")
        self.bank = bank
        self.max_references = max_references
        self._reference_count = 0
        self.key_version = key_version
        self._files, self._keys, self._public_material = {}, {}, {}
        self._entries = {}
        self.introduce_version(key_version, key_paths)

    @property
    def active_versions(self):
        return tuple(self._keys)

    def introduce_version(self, version, files):
        """Backfill retained references and enable dual indexing with fresh keys."""
        keys = load_pairwise_keys(
            self.bank, version, files.private_key, files.peer_public_keys
        )
        if not keys:
            raise ValueError("Token indexing requires trusted peer public keys")
        if self._keys and set(keys) != set(next(iter(self._keys.values()))):
            raise ValueError("Active versions must cover the same bank pairs")
        if any(peer not in ("A", "B", "C") for pair in keys for peer in pair):
            raise ValueError("Unknown bank in key configuration")
        material = {
            self.bank: public_key_pem(
                load_agreement_key(files.private_key).public_key()
            ),
            **{
                peer: public_key_pem(load_agreement_public_key(path))
                for peer, path in files.peer_public_keys.items()
            },
        }
        for old_version, old in self._public_material.items():
            if old_version == version:
                if material != old:
                    raise ValueError("Use a new version for changed X25519 material")
            elif any(old.get(bank) == public for bank, public in material.items()):
                raise ValueError("Rotation requires fresh X25519 key pairs")
        additions = {}
        if version not in self._keys:
            for (pair, _, _), references in self._entries.items():
                if pair not in keys:
                    raise ValueError("Missing key for retained transaction bank pair")
                for reference in references:
                    token = derive_token(reference[0], *pair, keys[pair], version)
                    additions.setdefault((pair, version, token), set()).add(reference)
        count = sum(map(len, additions.values()))
        if (
            self.max_references is not None
            and self._reference_count + count > self.max_references
        ):
            raise RuntimeError(
                "Token index reference limit reached; no records were evicted"
            )
        self._files[version] = files
        self._keys[version] = keys
        self._public_material[version] = material
        self._entries.update(additions)
        self._reference_count += count

    def reload_keys(self):
        """Re-open Secret mount paths; no pod restart is required."""
        for version, files in self._files.copy().items():
            self.introduce_version(version, files)

    def set_query_version(self, version, peer_versions):
        """Switch only after trusted configuration confirms both endpoints ready."""
        if version not in self._keys:
            raise ValueError("Query version is not active locally")
        peers = {peer for pair in self._keys[version] for peer in pair} - {self.bank}
        if any(version not in peer_versions.get(peer, ()) for peer in peers):
            raise ValueError("Query version is not ready at every peer")
        self.key_version = version

    def retire_version(self, version):
        """After old entries/in-flight work expire, discard that version only."""
        if version == self.key_version:
            raise ValueError("Switch queries before retiring their key version")
        del self._keys[version]
        del self._files[version]
        del self._public_material[version]
        self._entries = {k: v for k, v in self._entries.items() if k[1] != version}
        self._reference_count = sum(map(len, self._entries.values()))

    def token_for(self, payment_id, bank_pair, version):
        pair = canonical_pair(*bank_pair)
        return derive_token(payment_id, *pair, self._keys[version][pair], version)

    def tokens(self, event):
        e = validate(event, self.bank)
        if e["counterparty_bank"] == self.bank:
            return {}
        pair = canonical_pair(self.bank, e["counterparty_bank"])
        if any(pair not in keys for keys in self._keys.values()):
            raise ValueError("Missing key for transaction bank pair")
        return {
            version: self.token_for(e["payment_id"], pair, version)
            for version in self.active_versions
        }

    def add(self, event):
        tokens = self.tokens(event)
        pair = canonical_pair(self.bank, event["counterparty_bank"]) if tokens else None
        reference = (event["payment_id"], event["direction"], event["account"])
        additions = [
            (pair, version, token)
            for version, token in tokens.items()
            if reference not in self._entries.get((pair, version, token), ())
        ]
        if (
            self.max_references is not None
            and self._reference_count + len(additions) > self.max_references
        ):
            raise RuntimeError(
                "Token index reference limit reached; no records were evicted"
            )
        for scope in additions:
            self._entries.setdefault(scope, set()).add(reference)
        self._reference_count += len(additions)
        return tokens.get(self.key_version)

    def lookup(self, bank_pair, key_version, token):
        pair = canonical_pair(*bank_pair)
        if self.bank not in pair:
            raise ValueError("Lookup pair is unrelated to this bank")
        return [
            {"payment_id": payment_id, "direction": direction, "account": account}
            for payment_id, direction, account in sorted(
                self._entries.get((pair, key_version, token), ())
            )
        ]
