"""Joining the node's messages into cases, and tiering them."""

from mulechain.case.store import CaseStore


def leg(payment, direction="out", amount=100.0, token=""):
    token = token or f"t-{payment}"
    return {
        "payment_id": payment,
        "direction": direction,
        "account": "A-1",
        "amount": amount,
        "peer": "B",
        "token": token,
    }


def hit(key, *legs, rule="new_payee"):
    return {"key": key, "kind": "hit", "rule": rule, "legs": list(legs)}


def link(signer, in_token="", out_token=""):
    return {
        "signer": signer,
        "in_from": "X" if in_token else "",
        "in_token": in_token,
        "out_to": "Y" if out_token else "",
        "out_token": out_token,
    }


def chain(key, links, *legs, state="confirmed"):
    return {"key": key, "kind": "chain", "state": state, "links": links, "legs": list(legs)}


def test_tier_2_needs_two_banks_rules_and_tier_1_this_banks_own():
    store = CaseStore(":memory:", "A")
    both = [link("A", "t1", "t2"), link("B", "t2", "t3"), link("C", "t3")]
    only_a = [link("A", "t4", "t5"), link("B", "t5")]
    only_b = [link("A", out_token="t6"), link("B", "t6", "t7"), link("C", "t7")]
    assert store.record(chain("c1", both, leg("p1", token="t1")))["tier"] == 2
    assert store.record(chain("c2", only_a, leg("p2", token="t4")))["tier"] == 1
    assert store.record(chain("c3", only_b, leg("p3", token="t6")))["tier"] == 0
    assert [c["id"] for c in store.cases()] == ["c1", "c2"]  # tier 0 is kept but not listed


def test_messages_sharing_a_transfer_join_one_case_and_alert_once():
    store = CaseStore(":memory:", "A")
    store.record(hit("h1", leg("p1", "in"), leg("p2"), rule="chain"))
    pending = {"key": "s1", "kind": "pending", "waiting_for": ["C"]}
    store.record(pending | {"legs": [leg("p1", "in"), leg("p2")]})
    (case,) = store.cases()
    assert (case["tier"], case["state"], case["waiting"]) == (1, "pending", "C")
    store.toggle(case["id"], "reviewed")

    links = [link("A", "t-p1", "t-p2"), link("B", "t-p2", "t9")]
    alerted = store.record(chain("c1", links, leg("p1", "in"), leg("p2"), state="partial"))
    assert (alerted["id"], alerted["tier"], alerted["state"]) == (case["id"], 2, "partial")
    assert alerted["reviewed"] == 0  # it became an alert: look again
    again = store.record(chain("c2", [*links, link("C", "t9")], leg("p2")))
    assert again["alerted_at"] == alerted["alerted_at"] and again["state"] == "confirmed"
    assert len(store.cases()) == 1


def test_a_message_bridging_two_cases_merges_them_into_the_older():
    store = CaseStore(":memory:", "A")
    first = store.record(hit("h1", leg("p1")))
    second = store.record(hit("h2", leg("p2")))
    store.toggle(second["id"], "bookmarked")
    merged = store.record(hit("h3", leg("p1"), leg("p2"), rule="chain"))
    assert merged["id"] == first["id"] and merged["bookmarked"] == 1
    assert [c["id"] for c in store.cases()] == [first["id"]]
    assert store.get(second["id"])["merged_into"] == first["id"]
    assert store.payments() == {first["id"]: ["p1", "p2"]}


def test_a_repeated_message_changes_nothing():
    store = CaseStore(":memory:", "A")
    store.record(hit("h1", leg("p1")))
    store.record(hit("h1", leg("p1")))
    assert len(store.get("h1")["messages"]) == 1


def test_cases_are_listed_alerts_first_then_by_rules_and_amount():
    store = CaseStore(":memory:", "A")
    store.record(hit("small", leg("p1", amount=10)))
    store.record(hit("big", leg("p2", amount=5000)))
    store.record(hit("two-rules", leg("p3", amount=1)))
    store.record(hit("two-rules-b", leg("p3", amount=1), rule="quick_repeat"))
    links = [link("A", "t1", "t2"), link("B", "t2", "t3")]
    store.record(chain("alert", links, leg("p4", amount=1, token="t1")))
    assert [c["id"] for c in store.cases()] == ["alert", "two-rules", "big", "small"]
