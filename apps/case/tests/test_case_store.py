import threading

import pytest

from case.case_store import CaseStore, Claim, transfers
from case.evidence import parse
from sample_chains import (
    SAMPLE,
    as_partial,
    bridged,
    chain,
    foreign,
    leaks,
    leg,
    m1,
    message,
    o1,
    public_keys,
    unchecked,
    v1,
)

# DEPENDS ON Role 3 (privacy node): every test here uses real ChainResults from its
# nodes (sample_chains.py), so a change to privacy_node.proto affects them all.


def alerts(store):
    return store.db.execute("SELECT COUNT(*) AS n FROM alerts").fetchone()["n"]


def test_transfer_keys_normalize_role3_bytes_to_hex():
    start, link, _ = m1().claims
    assert transfers(Claim.from_pb(link)) == [
        (ref.pair, ref.token.hex()) for ref in (link.in_ref, link.out_ref)
    ]
    assert transfers(Claim.from_pb(start)) == [
        (start.out_ref.pair, start.out_ref.token.hex())
    ]  # chain starts at A


def test_case_id_is_role3_chain_digest(store):
    result = o1()
    assert store.record(message(result)) == result.chain_digest
    case = store.get(result.chain_digest)
    assert [c.signer for c in case["claims"]] == ["A", "B", "C", "A"]
    assert (case["tier"], case["state"]) == (2, "confirmed")
    assert case["raised_at"] is not None


@pytest.mark.parametrize(
    "result,bank,tier",
    [
        (m1, "B", 1),  # only B's rule links p06 -> p07: local suspicion at B
        (m1, "A", 0),  # ... and nothing at A or C
        (m1, "C", 0),
        (o1, "A", 2),  # B's and C's rules link the loop: an alert everywhere
        (lambda: v1()[0], "C", 2),  # partial, but two banks' rules link it
    ],
)
def test_tier_counts_rule_backed_banks(store, result, bank, tier):
    case = store.get(store.record(message(result(), bank)))
    assert case["tier"] == tier
    assert (case["raised_at"] is not None) == (tier == 2)
    assert [c["case_id"] for c in store.cases()] == ([case["case_id"]] if tier else [])


def test_own_legs_are_snapshotted_once(store):
    case = store.get(store.record(message(m1(), "B")))
    assert [(g["payment_id"], g["direction"], g["account"]) for g in case["legs"]] == [
        ("p06", "in", "B-m1"),
        ("p07", "out", "B-m1"),
    ]
    assert leaks(str(case), foreign("B")) == set()  # A's and C's legs stay tokens


def test_every_sample_message_kind_is_stored(store):
    """The shared sample (docs/contracts/protocol.md), with a freshly signed chain."""
    pending, hit, local = (parse(m, public_keys(), "B") for m in SAMPLE[1:])
    fresh = chain(m1(), "B")
    assert fresh.keys() == SAMPLE[0].keys()
    assert fresh["result"].keys() == SAMPLE[0]["result"].keys()
    waiting = store.record(pending)
    assert store.get(waiting)["state"] == "pending"
    assert store.get(waiting)["messages"][0]["detail"] == "C"
    # The hit on p07 and then the confirmed chain join the pending case.
    assert store.record(hit) == waiting
    assert store.record(message(m1(), "B")) == waiting
    case = store.get(waiting)
    assert (case["tier"], case["state"], case["raised_at"]) == (1, "confirmed", None)
    internal = store.get(store.record(local))
    assert (internal["tier"], internal["state"]) == (1, "local")
    assert {g["account"] for g in internal["legs"]} == {"B-bob", "B-carol"}
    assert len(store.cases()) == 2


def test_tier_1_case_is_promoted_by_a_tier_2_chain(store):
    hit = SAMPLE[2] | {"key": "new-payee-p11", "legs": [leg("B", "p11", "out")]}
    case_id = store.record(parse(hit, public_keys(), "B"))
    store.flag(case_id, "reviewed")
    assert store.cases()[0]["tier"] == 1 and alerts(store) == 0
    assert store.record(message(o1(), "B")) == case_id  # o1 includes p11
    assert store.get(case_id)["tier"] == 2 and store.get(case_id)["raised_at"]
    assert not store.get(case_id)["reviewed"]  # the alert needs a fresh look
    assert [n["kind"] for n in store.notifications()] == ["alert"]


def test_overlapping_partials_join_one_case(store):
    # Over-limit v1: five partial results from three banks, all sharing transfers.
    results = v1()
    assert len(results) == 5
    ids = {store.record(message(r, r.claims[0].signer)) for r in results}
    assert len(ids) == 1
    [case] = store.cases()
    assert (case["state"], case["tier"]) == ("partial", 2)
    assert alerts(store) == 1


def test_partial_upgrades_to_confirmed_without_a_second_alert(store):
    case_id = store.record(message(as_partial(o1())))
    raised = store.get(case_id)["raised_at"]
    assert store.get(case_id)["state"] == "partial" and raised is not None
    assert store.record(message(o1())) == case_id
    assert store.get(case_id)["state"] == "confirmed"
    assert store.get(case_id)["raised_at"] == raised
    assert len(store.get(case_id)["messages"]) == 1  # one row per key, upgraded


def test_redelivery_is_stored_once(store):
    case_id = store.record(message(o1()))
    store.record(message(o1()))
    case = store.get(case_id)
    assert len(case["claims"]) == 4 and len(case["messages"]) == 1
    assert alerts(store) == 1


def test_hit_and_internal_chain_can_share_a_key(store):
    # One hit ID covers the same payments, so both messages land in one case.
    hit = parse(SAMPLE[2], public_keys(), "B")
    local = parse(
        SAMPLE[3] | {"key": hit.key, "legs": SAMPLE[2]["legs"]}, public_keys(), "B"
    )
    assert store.record(hit) == store.record(local) == hit.key
    messages = store.get(hit.key)["messages"]
    assert sorted((m["kind"], m["detail"]) for m in messages) == [
        ("hit", "new_payee"),
        ("local", ""),
    ]


def test_cases_rank_by_rules_fired_before_age(store):
    hit, local = (parse(m, public_keys(), "B") for m in SAMPLE[2:])
    older = store.record(local)  # an internal chain: no hit rule, no backed chain
    newer = store.record(hit)  # the new_payee rule fired
    assert [c["case_id"] for c in store.cases()] == [newer, older]
    assert [c["rules"] for c in store.cases()] == [1, 0]


def test_late_partial_redelivery_does_not_downgrade(store):
    case_id = store.record(message(o1()))
    store.record(message(as_partial(o1())))
    assert store.get(case_id)["state"] == "confirmed"


def test_bridging_result_merges_into_oldest_case(store):
    first, second, whole = (unchecked(r, "A") for r in bridged())
    old, new = store.record(first), store.record(second)
    assert old != new
    store.flag(new, "bookmarked")
    store.flag(old, "reviewed")
    assert store.record(whole) == old
    assert [c["case_id"] for c in store.cases()] == [old]
    assert store.get(new)["merged_into"] == old
    assert store.get(old)["bookmarked"]  # flags move with the evidence
    assert not store.get(old)["reviewed"]  # ... but `new` was never reviewed
    assert len(store.get(old)["claims"]) == 6
    assert alerts(store) == 1


def test_concurrent_writers_create_one_case(url):
    CaseStore(url)
    ids, messages = [], [message(o1("B")), message(o1("C"))] * 8
    threads = [
        threading.Thread(target=lambda m=m: ids.append(CaseStore(url).record(m)))
        for m in messages
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    store = CaseStore(url)
    assert len(set(ids)) == 1 and len(store.cases()) == 1
    assert alerts(store) == 1


def test_close_and_reopen_keeps_cases_and_alerts(url):
    store = CaseStore(url)
    confirmed = store.record(message(o1()))
    store.flag(confirmed, "reviewed")
    before = store.cases()
    store.db.conn.close()

    reopened = CaseStore(url)
    assert reopened.cases() == before and before[0]["reviewed"]
    assert reopened.record(message(o1())) == confirmed  # still one alert
    assert [n["kind"] for n in reopened.notifications()] == ["alert"]


def test_leak_check_catches_planted_values():
    assert leaks('{"account": "A-m0", "payment": "p06", "amount": 120.0}') == {
        "A-m0",
        "p06",
        "120.0",
    }
    assert leaks("created_at 1793000120.05, pair A-B, Wired") == set()


def test_no_other_banks_data_in_any_stored_table(store):
    for result in [m1(), o1(), *v1()]:
        store.record(message(result, "B"))
    dump = "\n".join(
        str(value)
        for table in ("cases", "transfers", "messages", "claims", "own_legs")
        for row in store.db.execute(f"SELECT * FROM {table}")
        for value in row.values()
    )
    assert "B-m1" in dump  # B's own legs are stored
    assert leaks(dump, foreign("B")) == set()


def test_reconnects_after_postgresql_restarts(url):
    if not url.startswith("postgres"):
        pytest.skip("needs CASE_TEST_DATABASE_URL")
    import psycopg

    store = CaseStore(url)
    pid = store.db.execute("SELECT pg_backend_pid() AS pid").fetchone()["pid"]
    with psycopg.connect(url, autocommit=True) as admin:  # what a failover does to it
        admin.execute("SELECT pg_terminate_backend(%s)", (pid,))
    with pytest.raises(psycopg.OperationalError):
        store.cases()  # the request in flight fails, and the node resends it
    assert store.cases() == []  # the next one reconnects
