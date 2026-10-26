"""Bank-local token references. Construct a new index and replay on key rotation."""

from apps.scorer.score import validate
from apps.security.security import canonical_pair, derive_token, load_pairwise_keys


class TokenIndex:
    def __init__(self, bank, key_version, key_paths, max_references=None):
        if bank not in ("A", "B", "C"):
            raise ValueError("Invalid local bank")
        self.bank = bank
        if max_references is not None and max_references < 1:
            raise ValueError("Token reference limit must be positive")
        self.max_references = max_references
        self._reference_count = 0
        self.key_version = key_version
        self._keys = load_pairwise_keys(bank, key_paths)
        if not self._keys:
            raise ValueError("Token indexing requires pairwise key files")
        for pair, key in self._keys.items():
            if any(peer not in ("A", "B", "C") for peer in pair):
                raise ValueError("Unknown bank in pairwise key configuration")
            derive_token("", *pair, key, key_version)
        self._entries = {}

    def add(self, event):
        e = validate(event, self.bank)
        if e["counterparty_bank"] == self.bank:
            return None
        pair = canonical_pair(self.bank, e["counterparty_bank"])
        if pair not in self._keys:
            raise ValueError("Missing key for transaction bank pair")
        token = derive_token(e["payment_id"], *pair, self._keys[pair], self.key_version)
        scope = (pair, self.key_version, token)
        reference = (e["payment_id"], e["direction"], e["account"])
        if reference in self._entries.get(scope, ()):
            return token
        if (
            self.max_references is not None
            and self._reference_count >= self.max_references
        ):
            raise RuntimeError(
                "Token index reference limit reached; no records were evicted"
            )
        self._entries.setdefault(scope, set()).add(reference)
        self._reference_count += 1
        return token

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
