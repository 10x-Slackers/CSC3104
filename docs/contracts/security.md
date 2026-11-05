# Security and privacy contract

Owner: Caleb · Partners: Zane (messages, signed claims), Cindy (tokens), Jia Rui (report inputs), Isaac (Secrets, certificates, NetworkPolicy) · Code: `apps/security/` · Status: **draft**, needs review by Zane and Cindy

> [!NOTE]
> **Pending work (decided 30 Oct 2026).** The sections below describe what exists today; these items change it:
> - **16-byte tokens:** `derive_token` returns the HMAC-SHA256 truncated to 128 bits as `bytes`, with no `ppmc:v1:k1:` prefix (the version is in `key_version`). About 4.7× smaller; NIST SP 800-107 Rev 1 §5.3.3 allows truncation. A new frozen test vector.
> - **Key agreement, not a dealer:** each bank generates its own X25519 key pair and publishes the public key; `pair_key(A,B,k) = HKDF(X25519(a_priv, B_pub), info="csc3104 pair-key A-B k")`. This replaces `privacy_node/keys.py`, which generates every bank's keys on one machine. No secret travels (MAS TRM §10.2.5); X25519 stays separate from Ed25519 (TRM §10.2.6).
> - **Rotation with overlapping versions:** introduce `k2` at both banks and index new payments under both keys; switch queries once both have it; retire `k1` after its index entries expire. A new key pair, not a relabel (TRM §10.2.9). Keys are re-read on a timer, and public keys are looked up by (bank, key version). Demonstrated during a live replay.
> - **Schema test instead of the runtime walk:** one CI test reads the protobuf descriptor and asserts every field is allowed and none is in `FORBIDDEN_FIELDS`; the node rejects unknown fields. `check_peer_payload` stays for untyped JSON.
> - **`check_result`** in `claims.py`: all six chain conditions, shared by the node's `Deliver` and the case service's `/results`.
> - **Own-bank data in cases:** an analyst sees their own bank's legs (payment ID, account, amount); other banks' legs stay tokens ([Disclosure boundary](#disclosure-boundary)).
> - **mTLS** with a SPIFFE-style URI SAN naming the bank, and certificates reloaded on renewal; NetworkPolicy paths for Valkey, replay and the summary service.

Scope: pairwise HMAC tokens, Ed25519 claim signing, the disclosure allowlist, certificate identity and mTLS, NetworkPolicy intent, and security test evidence.
Query rate limiting and a signed audit log are extensions, added only once the core works.

## Threat model

This is a laboratory model; see also [overview.md](../overview.md#trust-and-failure-model).

- Banks keep raw records locally and are assumed to apply the agreed rule honestly.
- Key provisioning, the certificate authority and each bank's runtime are trusted. If one bank is compromised, its records, its signing key and its two pair keys are exposed.
- **Protected against:** unauthenticated peers (mTLS), altered or wrongly signed claims (Ed25519), and forbidden fields leaving a bank (allowlist).
- **Not protected against:** a participating bank lying about its own records, or abusing lookups it is authorised to make. Namespace separation doesn't protect data from the cluster administrator.

## Disclosure boundary

**Never leaves a bank:** customer identifiers, account numbers, raw amounts, raw `payment_id`, full payment records and histories, the local token index, and secrets. The evaluation labels (`chain_id`, `hop`, `scenario`, `altman_laundering`) are never used in scoring or messages ([data.md](data.md)).

| Surface | May contain (exact fields need agreement) | Agree with |
| --- | --- | --- |
| Peer query | Pair-scoped token, authenticated bank identity, query ID, versions, protocol hop count and deadline | Zane |
| Peer claim / assembled result | Token references, signer identity, signature, minimal local-rule assertion | Zane, Jia Rui |
| Application logs | Request IDs, bank identity, status, timing, error category. No payload dumps; tokens only if needed | All owners |
| Case service store and UI (**pending work**) | The bank's **own** legs (payment ID, account, amount), snapshotted at case creation; other banks' legs as tokens only. The boundary is between banks, not inside one | Jia Rui, Zane |
| Summary service input (**pending work**) | The same bank-local case evidence, sent inside the bank to a function URL with `AWS_IAM`; template only, no model | Jia Rui |

The protocol's hop count isn't the same field as the evaluation `hop` label. The protocol field needs a different name.

`check_peer_payload(payload, allowed_fields)` walks dicts and lists recursively and raises `ValueError` for any key that is in `FORBIDDEN_FIELDS` or missing from `allowed_fields`, and `TypeError` for any value that isn't a dict, list, tuple or JSON scalar (convert protobuf messages with `MessageToDict(message, preserving_proto_field_name=True)` first). It checks **field names only**: it can't catch an account number hidden inside an allowed value. Zane's serialiser must also enforce the protobuf schema.

## Pairwise HMAC tokens

Every bank pair shares one independent random 32-byte key. Arbitrary per-bank keys would never produce matching tokens.

| Pair key | Held by | Not given to |
| --- | --- | --- |
| A–B | A, B | C |
| A–C | A, C | B |
| B–C | B, C | A |

`derive_token(payment_id, bank_a, bank_b, shared_key, key_version) -> str`:

1. Bank IDs are distinct, exact, case-sensitive strings, sorted so that `(A, B)` and `(B, A)` give the same pair. Intra-bank transfers have no peer token and are rejected.
2. `payment_id` is a string, used exactly as given by [data.md](data.md), with no trimming or case folding.
3. `key_version` must match `[A-Za-z0-9_-]+`, e.g. `k1`.
4. The HMAC input is the ASCII JSON from `canonical_token_input`, written with no spaces:

   ```text
   ["csc3104.payment-token","v1",["A","B"],"k1","p01"]
   ```

5. Output: `ppmc:v1:<key_version>:<hex of HMAC-SHA256(shared_key, input)>`.

Frozen test vector (the key is test-only):

```text
key   = b"A" * 32
input = ["csc3104.payment-token","v1",["A","B"],"k1","p01"]
token = ppmc:v1:k1:86ef4e3f811c077dcde10d9cef4d807c1b039afda3d794d72174c5d4a92cb444
```

The pair is bound into the HMAC input but isn't shown in the token. Lookups must therefore be scoped by `(pair, key_version, token)`. Tokens from different pairs or versions are never compared.

**Key rotation** isn't automated (**pending work**: overlapping versions, above). A new key gets a new version label at both endpoints, and then the affected services restart and rebuild their index (Cindy). Changing the encoding requires a new protocol version (`v2`).

## Ed25519 claim signing

- Each bank has its own signing key and is the only holder of its private key.
- Verifiers look up the public key from trusted configuration keyed by `(bank, signing key version)`. They never trust a key carried inside the claim.
- `sign(message_bytes, private_key)` signs exactly the bytes given. `verify(message_bytes, signature, public_key)` returns `False` for any bad or non-bytes signature, and the caller must then reject the claim without creating confirmed evidence.
- Zane defines the canonical claim bytes. They must cover every security-relevant field: domain, signer, recipient/context, query ID, tokens, and key versions.
- A valid signature proves which bank sent the claim and that it wasn't altered. It does **not** prove the records are true, that the claim is fresh, or that the chain is complete. Replay protection and deadlines belong to the protocol.

Key helpers: `generate_signing_key`, `private_key_pem` (unencrypted PKCS8), `public_key_pem` (SubjectPublicKeyInfo), and `load_signing_key` / `load_verification_key`, which reject keys that aren't Ed25519.

## Secrets

- Keys are generated outside git and loaded as read-only Kubernetes Secret volume files. Bank A mounts the A–B and A–C pair keys and its own signing key; Banks B and C follow the same pattern.
- Public keys are distributed to every verifier as configuration.
- Never put keys in manifests, Helm values or git. The local `.secrets/` folder is git-ignored.
- `load_pairwise_keys(bank_id, {pair: path})` reads the raw bytes without stripping or base64 decoding. It rejects pairs that don't include this bank, pairs supplied twice, keys shorter than 32 bytes, and the same key reused across pairs.
- Keys are read once at startup, so rotation needs a restart. **pending work:** mounted Secrets update in place, so services re-read keys on a timer.

From another app that lists `security` as a workspace dependency (as `scorer` does):

```python
from security import derive_token, load_pairwise_keys

keys = load_pairwise_keys("A", {("A", "B"): "/run/secrets/pairs/ab-k1.key"})
token = derive_token(payment_id, "A", "B", keys["A", "B"], "k1")
```

The mount paths above are examples. Secret names and paths: TODO, with Isaac.

## mTLS and NetworkPolicy

**mTLS** between privacy nodes, with a laboratory CA (e.g. cert-manager):

- Both sides verify the certificate chain. The client checks the server's SAN.
- The server maps the client certificate's SAN to a bank using an explicit SAN-to-bank map. Any certificate signed by the CA isn't enough, and a bank ID written in the message never replaces the certificate.
- The authenticated sender must match the sender in the message envelope. TLS keys are separate from signing keys.
- **pending work** with Zane and Isaac: the DNS SAN plus a SPIFFE-style URI SAN naming the bank, the trust bundle and Secret mounts; the node re-reads certificates when cert-manager renews them. One shared lab CA is a trusted third party, which is a stated limitation.

**NetworkPolicy** intent: every bank namespace denies all ingress and egress by default, then allows only the following:

| From | To | Allowed |
| --- | --- | --- |
| Privacy node | Other banks' privacy nodes | The protocol port only (namespace **and** pod selector) |
| Ingestor, scorer | Own bank's Kafka | Broker ports |
| Scorer, privacy node | Own bank's Valkey | Valkey port (**pending work**); the case service never connects |
| Privacy node, case service | Own bank's PostgreSQL | Database port |
| Replay Job (`replay` namespace, k3d) | Each bank's ingestor | Its HTTP port (**pending work**) |
| Case service | Summary service | Its HTTP port in the namespace; on EKS, AWS on 443 with `AWS_IAM` (**pending work**) |
| Bank workloads | Cluster DNS | DNS |
| Operator-managed pods (e.g. CloudNativePG) | Kubernetes API server | TCP 443/6443 to the API server addresses only |
| KEDA operator | Own bank's Kafka | Broker ports, to read consumer lag |

Isaac owns the final policies in [deployment.md](deployment.md). The cluster's CNI must actually enforce them.

## Residual disclosure

Tokens are pseudonymous, not anonymous:

- Within one pair and key version, a token is stable, so repeated references to the same payment can be linked.
- A pair-key holder can test guessed payment IDs. This matters for predictable IDs such as the fixture's `p01`.
- Bank identities, timing, query frequency and claim outcomes are visible metadata.
- Intermediate banks link their own incoming and outgoing transfers.
- An assembled chain reveals the participating banks and their order.

Report these limits alongside the evaluation results. Never claim zero disclosure.

## Tests

```sh
uv run pytest apps/security
```

Implemented:

- the A–B fixture tokens match at both banks
- the frozen vector
- changing the ID, key, pair or version changes the token
- invalid pairs, versions and short keys are rejected
- key mounts are scoped to their bank; reused keys and duplicate pairs are rejected
- PEM round trip; non-Ed25519 keys rejected
- tampered message, wrong signer and malformed signature all fail verification
- nested allowlist checks; forbidden fields rejected even when the caller allows them

Integration evidence still needed, owned with Isaac and evaluated in [evaluation.md](../evaluation.md):

- [ ] Put sentinel accounts, IDs and amounts in each bank's data. Capture real queries, claims, logs and report inputs for confirmed, partial, timed-out and invalid searches, and check that none of the sentinels appear in any of them, including as numeric values.
- [ ] Unauthenticated, expired or wrongly named certificates are rejected.
- [ ] Tampered or wrongly signed claims never become confirmed evidence.
- [ ] Cross-bank access to Kafka and PostgreSQL is denied on k3d and EKS.

## Interfaces

Agreed contract first ([conventions](../README.md#conventions)): the producer adds one sample message here, and both sides' tests load it.

| Interface | From → to | Pins down | Status | Agree by |
| --- | --- | --- | --- | --- |
| Token and key API ([security.md](security.md)) | Caleb → Cindy, Zane | `derive_token` signature, 16 bytes out, key file layout and `key_version`, one test vector | **draft**, sample TODO | Mon 2 Nov |
| `check_result` ([security.md](security.md)) | Caleb → Zane, Jia Rui | Signature, the six conditions, how a failure is reported | **draft** | Wed 4 Nov |

Also part of: [`.proto` v1](protocol.md#interfaces).

## Open

- [ ] Review Zane's protobuf field allowlist: `ALLOWED_FIELDS` in `apps/privacy_node/src/privacy_node/claims.py`, per message in [protocol.md](protocol.md#messages). `chain_id` was renamed `chain_digest` and `hop` is `hops_remaining`, so neither collides with `FORBIDDEN_FIELDS` (Caleb).
- [ ] Review Zane's canonical claim bytes: [protocol.md](protocol.md#signed-claim-bytes), `claims.claim_bytes` (Caleb).
- [ ] Secret names, mount paths and the SAN-to-bank map (Caleb, with Isaac).
- [x] Destination and inputs for the report: the summary service, template only, bank-local case evidence.
