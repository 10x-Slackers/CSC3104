"""In-memory stand-in for a bank's Valkey, with the entries of streaming.md's Valkey layout."""

from collections import defaultdict


class MemoryState:
    def __init__(self):
        # win:<account>: (payment_id, direction) -> (ts, event)
        self.win = defaultdict(dict)
        self.pay = {}  # pay:<payment_id>:<direction>
        self.link = defaultdict(set)  # link:in:<payment_id> and link:out:<payment_id>
        self.hit = {}  # hit:<hit_id>, in the order written (the hits stream)
        self.paid = {}  # last:<account>:<payee> -> (ts, flagged)

    # written by the scorer

    def window(self, account):
        return self.win[account]

    def add_event(self, account, key, ts, event):
        self.win[account][key] = (ts, event)

    def trim(self, account, before):
        for key, (ts, _) in list(self.win[account].items()):
            if ts < before:
                del self.win[account][key]

    def put_payment(self, payment_id, direction, entry):
        self.pay[payment_id, direction] = entry

    def add_link(self, incoming, outgoing):
        self.link["in", incoming].add(outgoing)
        self.link["out", outgoing].add(incoming)

    def put_hit(self, hit):
        self.hit.setdefault(hit["hit_id"], hit)

    def last_paid(self, account, payee):
        return self.paid.get((account, payee))

    def set_last_paid(self, account, payee, ts, flagged):
        # a late, older payment must not replace a newer one
        if ts >= self.paid.get((account, payee), (ts,))[0]:
            self.paid[account, payee] = (ts, flagged)

    # read by the node and the offline evaluator

    def links(self, payment_id, direction):
        return sorted(self.link.get((direction, payment_id), ()))

    def payment(self, payment_id, direction):
        return self.pay.get((payment_id, direction))

    def hits(self):
        return list(self.hit.values())
