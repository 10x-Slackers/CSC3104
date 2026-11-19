"""Key agreement with no dealer, tokens and rotation."""

import os

from mulechain import crypto


def keys(count=1):
    return [crypto.PrivateKey(os.urandom(64)) for _ in range(count)]


def public(private):
    return [k.public for k in private]


def test_two_banks_derive_the_same_token_and_a_third_cannot():
    a, b, c = keys(), keys(), keys()
    ab = crypto.pair_keys("A", "B", a, public(b))
    ba = crypto.pair_keys("B", "A", b, public(a))
    ac = crypto.pair_keys("A", "C", a, public(c))
    cb = crypto.pair_keys("C", "B", c, public(b))
    token = crypto.token(ab[0], "p1")
    assert token == crypto.token(ba[0], "p1") and len(token) == 16
    assert token not in (crypto.token(ac[0], "p1"), crypto.token(cb[0], "p1"))


def test_a_rotation_never_leaves_a_token_the_peer_has_not_indexed():
    """B adds a new key, then removes the old one. A does nothing, and its view of B's
    keys may be one step behind. Either bank can always look up the other's token."""
    a, old, new = keys(), keys(), keys()
    steps = [old, old + new, new]
    views = [(steps[0], steps[0]), (steps[1], steps[0]), (steps[1], steps[1])]
    views += [(steps[2], steps[1]), (steps[2], steps[2])]
    for b, seen_by_a in views:  # B's real keys, and the keys A last fetched
        at_a = [crypto.token(k, "p1") for k in crypto.pair_keys("A", "B", a, public(seen_by_a))]
        at_b = [crypto.token(k, "p1") for k in crypto.pair_keys("B", "A", b, public(a))]
        assert at_a[0] in at_b and at_b[0] in at_a  # each asks with its first token


def test_a_signature_verifies_under_any_published_key_only():
    old, new, other = keys()[0], keys()[0], keys()[0]
    signature = crypto.sign(new, b"claim")
    assert crypto.verify([old.public, new.public], b"claim", signature)
    assert not crypto.verify([old.public, new.public], b"other claim", signature)
    assert not crypto.verify([other.public], b"claim", signature)
