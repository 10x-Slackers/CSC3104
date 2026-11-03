"""Internal case enrichment; never pass these details to a peer RPC."""


def own_case_legs(result, local):
    """Snapshot only this node's own records for an internal multipart delivery."""
    legs = []
    for index, claim in enumerate(result.claims):
        if claim.signer != local.bank:
            continue
        for direction, ref in (("in", claim.in_ref), ("out", claim.out_ref)):
            if not ref.token:
                continue
            payment = local.lookup(ref.pair, claim.key_version, ref.token, direction)
            record = local.records[payment, direction]
            legs.append(
                {"bank": local.bank, "claim_index": index, "direction": direction}
                | record
            )
    return legs
