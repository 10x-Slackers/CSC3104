"""The internal case boundary: raw details belong only to this bank's own claims."""

FIELDS = {"bank", "claim_index", "direction", "payment_id", "account", "amount"}


def own_legs(result, legs, bank):
    """Bind each local-only detail to an existing signed, own-bank transfer."""
    rows = []
    for leg in legs:
        if set(leg) != FIELDS or leg["bank"] != bank:
            raise ValueError("Only this bank's own leg details are permitted")
        index, direction = leg["claim_index"], leg["direction"]
        if not 0 <= index < len(result.claims) or direction not in ("in", "out"):
            raise ValueError("Invalid local leg reference")
        claim = result.claims[index]
        ref = claim.in_ref if direction == "in" else claim.out_ref
        if claim.signer != bank or not ref.token:
            raise ValueError("Local details must reference this bank's signed transfer")
        rows.append(
            (
                bank,
                claim.key_version,
                ref.pair,
                ref.token,
                direction,
                leg["payment_id"],
                leg["account"],
                leg["amount"],
            )
        )
    return rows
