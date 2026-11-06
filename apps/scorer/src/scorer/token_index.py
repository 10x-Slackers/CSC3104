"""Bank-local raw-byte token references with overlapping key versions."""

from security import canonical_pair, derive_token, load_pairwise_keys


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
        self._keys = {}
        self._entries = {}
        self.introduce_version(key_version, key_paths)

    @property
    def active_versions(self):
        return tuple(self._keys)

    def matches_versions(self, keys, query_version):
        """Whether prepared configuration leaves the current token index unchanged."""
        return self._keys == keys and self.key_version == query_version

    def introduce_version(self, version, files):
        """Load one version and backfill retained references for overlapping rotation."""
        keys = load_pairwise_keys(
            self.bank, version, files.private_key, files.peer_public_keys
        )
        self.apply_version(version, keys)

    def apply_version(self, version, keys):
        """Apply prepared pair keys without filesystem access."""
        self.check_version(version, keys)
        additions = {}
        if version not in self._keys:
            for (pair, _, _), references in self._entries.items():
                for reference in references:
                    token = derive_token(reference[0], *pair, keys[pair], version)
                    additions.setdefault((pair, version, token), set()).add(reference)
        count = sum(map(len, additions.values()))
        self._check_capacity(count)
        self._keys[version] = keys
        self._entries.update(additions)
        self._reference_count += count

    def check_version(self, version, keys):
        """Cheap validation before copying or backfilling an index."""
        if not keys:
            raise ValueError("Token indexing requires trusted peer public keys")
        if self._keys and set(keys) != set(next(iter(self._keys.values()))):
            raise ValueError("Active versions must cover the same bank pairs")
        if any(peer not in ("A", "B", "C") for pair in keys for peer in pair):
            raise ValueError("Unknown bank in key configuration")
        if version in self._keys and self._keys[version] != keys:
            raise ValueError("Use a new version for changed X25519 material")

    def _check_capacity(self, count):
        if (
            self.max_references is not None
            and self._reference_count + count > self.max_references
        ):
            raise RuntimeError(
                "Token index reference limit reached; no records were evicted"
            )

    def check_query_version(self, version, peer_versions, versions=None):
        versions = self._keys if versions is None else versions
        if version not in versions:
            raise ValueError("Query version is not active locally")
        peers = {peer for pair in versions[version] for peer in pair} - {self.bank}
        if any(version not in peer_versions.get(peer, ()) for peer in peers):
            raise ValueError("Query version is not ready at every peer")

    def set_query_version(self, version, peer_versions):
        """Switch only after trusted configuration confirms both endpoints ready."""
        self.check_query_version(version, peer_versions)
        self.key_version = version

    def retire_version(self, version):
        """After old entries/in-flight work expire, discard that version only."""
        if version == self.key_version:
            raise ValueError("Switch queries before retiring their key version")
        del self._keys[version]
        self._entries = {k: v for k, v in self._entries.items() if k[1] != version}
        self._reference_count = sum(map(len, self._entries.values()))

    def token_for(self, payment_id, bank_pair, version):
        pair = canonical_pair(*bank_pair)
        return derive_token(payment_id, *pair, self._keys[version][pair], version)

    def tokens(self, e):
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
        self._check_capacity(len(additions))
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
