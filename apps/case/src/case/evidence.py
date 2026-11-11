"""Check one node payload (docs/contracts/protocol.md) and turn it into a Message.

Chains pass the shared check_result, the same check as the node's Deliver. The other
kinds and the own legs come only from this bank's node, so they are trusted.
"""

from google.protobuf import json_format

from case.case_store import Claim, Leg, Message
from privacy_node import privacy_node_pb2 as pb
from privacy_node.claims import check_result
from privacy_node.keys import public_keys as load_public_keys
from security import PublicKeys

__all__ = ["check_result", "load_public_keys", "parse", "tier"]


def tier(result: pb.ChainResult, bank: str) -> int:
    """2 (alert) if two banks' rules link the chain, 1 if only this bank's did, else 0.

    A rule-backed claim has both refs: that bank's own rule linked them. Partial
    chains count too, since a chain longer than the hop limit can never confirm.
    """
    backed = {c.signer for c in result.claims if c.in_ref.token and c.out_ref.token}
    return 2 if len(backed) >= 2 else int(bank in backed)


def parse(payload: dict, public_keys: PublicKeys, bank: str) -> Message:
    """Raise KeyError, TypeError or ValueError for a payload that isn't evidence."""
    key, kind = payload["key"], payload["kind"]
    legs = [
        Leg(
            **{f: leg[f] for f in Leg.__annotations__}
            | {"amount": float(leg["amount"])}
        )
        for leg in payload["legs"]
    ]
    if kind == "chain":
        try:
            result = json_format.ParseDict(payload["result"], pb.ChainResult())
        except json_format.ParseError as exc:
            raise ValueError(str(exc)) from exc
        result = check_result(result, public_keys, bank)
        if result.chain_digest != key:
            raise ValueError("key is not the chain digest")
        state = "confirmed" if result.state == pb.CONFIRMED else "partial"
        claims = [Claim.from_pb(c) for c in result.claims]
        return Message(
            key, kind, tier(result, bank), state, legs, claims, result.sender
        )
    detail = {"pending": ", ".join(payload.get("waiting_for", [])), "local": ""}
    if kind == "hit":
        detail["hit"] = payload["rule"]
    if kind not in detail:
        raise ValueError(f"unknown kind: {kind}")
    return Message(key, kind, 1, kind, legs, detail=detail[kind])
