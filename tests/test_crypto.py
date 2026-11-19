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


def test_rotation_keeps_a_shared_key_at_every_step():
    """B adds a new key, then drops the old one; A never has to do anything."""
    a, old, new = keys(), keys(), keys()
    steps = [old, old + new, new]
    for b in steps:
        ab = crypto.pair_keys("A", "B", a, public(b))
        ba = crypto.pair_keys("B", "A", b, public(a))
        assert ab[0] == ba[0]  # both query with the oldest pair
    before = crypto.pair_keys("A", "B", a, public(steps[0]))
    during = crypto.pair_keys("A", "B", a, public(steps[1]))
    after = crypto.pair_keys("A", "B", a, public(steps[2]))
    assert before[0] in during and after[0] in during  # indexed under both while rotating


def test_a_signature_verifies_under_any_published_key_only():
    old, new, other = keys()[0], keys()[0], keys()[0]
    signature = crypto.sign(new, b"claim")
    assert crypto.verify([old.public, new.public], b"claim", signature)
    assert not crypto.verify([old.public, new.public], b"other claim", signature)
    assert not crypto.verify([other.public], b"claim", signature)
