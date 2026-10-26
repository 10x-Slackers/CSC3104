"""Privacy node: peer-to-peer chain tracing between banks (protocol.md).

Run one per bank, e.g.:
  uv run python -m apps.privacy_node.node --bank B --port 50052 \
    --workload apps/dataset/fixtures/tiny --key-version k1 \
    --pairwise-key A .secrets/pairs/A-B.key --pairwise-key C .secrets/pairs/B-C.key \
    --signing-key .secrets/signing/B.pem \
    --public-key A .secrets/public/A.pem --public-key B .secrets/public/B.pem \
    --public-key C .secrets/public/C.pem \
    --peer A localhost:50051 --peer C localhost:50053 --db .secrets/B.db --search
"""

import argparse
import hashlib
import threading
import time
import traceback
import uuid
from concurrent import futures
from datetime import UTC, datetime, timedelta

import grpc

from apps.privacy_node import privacy_node_pb2 as pb
from apps.privacy_node import privacy_node_pb2_grpc as pb_grpc
from apps.privacy_node.claims import (
    bound,
    chain_digest,
    check_payload,
    claim_bytes,
    complete,
    key,
    refs,
    valid_chain,
)
from apps.privacy_node.local_state import LocalBank
from apps.privacy_node.store import Store
from apps.security.security import load_signing_key, load_verification_key, sign

HOP_LIMIT = 4  # transfers are hops 0..HOP_LIMIT, so a chain holds at most 5
BRANCH_CAP = 1  # linear chains only; extra local links make the result PARTIAL
SEARCH_SECONDS = 10  # whole-search deadline, carried unchanged by every hop
# Worst state wins when two results are combined.
SEVERITY = [
    *(pb.CONFIRMED, pb.PARTIAL, pb.NO_MATCH, pb.PENDING, pb.TIMED_OUT),
    *(pb.FAILED, pb.INVALID_CLAIM),
]


def now():
    return datetime.now(UTC)


def worst(*states):
    return max(states, key=SEVERITY.index)


def failure_state(error):
    if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return pb.TIMED_OUT
    if error.code() == grpc.StatusCode.UNAVAILABLE:
        return pb.PENDING  # unreachable bank: retried after recovery
    return pb.FAILED  # the peer rejected or failed the query: retrying won't help


class Node(pb_grpc.PrivacyNodeServicer):
    def __init__(self, local, store, signing_key, public_keys, peers, key_version):
        self.bank = local.bank
        self.local, self.store = local, store
        self.signing_key, self.public_keys = signing_key, public_keys
        self.peers, self.key_version = peers, key_version

    # ---- outgoing calls -------------------------------------------------

    def call(self, bank, method, request, deadline):
        """Call a peer once; housekeeping retries searches and deliveries that fail."""
        check_payload(request)
        with grpc.insecure_channel(self.peers[bank]) as channel:  # M3: mTLS
            stub = pb_grpc.PrivacyNodeStub(channel)
            timeout = (deadline - now()).total_seconds()
            return getattr(stub, method)(request, timeout=timeout)

    def request(self, query_id, ref, direction, hops_remaining, deadline, visited):
        return pb.TraceRequest(
            query_id=query_id,
            sender=self.bank,
            key_version=self.key_version,
            ref=ref,
            direction=direction,
            hops_remaining=hops_remaining,
            deadline=deadline,
            visited=visited,
        )

    def claim(self, recipient, query_id, in_ref, out_ref):
        c = pb.LinkClaim(
            signer=self.bank,
            recipient=recipient,
            query_id=query_id,
            key_version=self.key_version,
            in_ref=in_ref,
            out_ref=out_ref,
        )
        c.signature = sign(claim_bytes(c), self.signing_key)
        return c

    # ---- answering a peer -----------------------------------------------

    def Trace(self, request, context):
        slot = f"{request.sender}|{key(request.ref)}"
        stored = self.store.get_reply(request.query_id, slot)
        if stored:
            return stored  # retried query: same answer, no repeated work
        reply = self.follow(request)
        check_payload(reply)
        if reply.state not in (pb.PENDING, pb.TIMED_OUT):  # only cache final answers
            self.store.put_reply(request.query_id, slot, reply)
        return reply

    def follow(self, request):
        # M3: take the sender from the mTLS certificate, not from the message.
        if now() > datetime.fromisoformat(request.deadline):
            return pb.TraceReply(state=pb.TIMED_OUT)
        down = request.direction == pb.DOWNSTREAM
        here = "in" if down else "out"
        payment = request.sender in request.ref.pair.split("-") and self.local.lookup(
            request.ref.pair, request.key_version, request.ref.token, here
        )
        if not payment:
            return pb.TraceReply(state=pb.NO_MATCH)
        return self.extend(request, payment, down)

    def extend(self, request, payment, down):
        """Sign this bank's link for the transfer and follow it one more hop."""
        here, there = ("in", "out") if down else ("out", "in")
        this_ref = request.ref

        def claim(other):
            in_ref, out_ref = (this_ref, other) if down else (other, this_ref)
            return self.claim(request.sender, request.query_id, in_ref, out_ref)

        nexts = self.local.links(payment, here)
        if not nexts:  # the chain starts or ends at this bank
            return pb.TraceReply(state=pb.CONFIRMED, claims=[claim(pb.TokenRef())])
        state = pb.PARTIAL if len(nexts) > BRANCH_CAP else pb.CONFIRMED
        next_ref = self.local.refs[nexts[0], there]
        if key(next_ref) in {key(r) for r in request.visited}:  # loop closed
            return pb.TraceReply(state=state, claims=[claim(next_ref)])
        if request.hops_remaining <= 0:  # hop limit: next_ref is left unfollowed
            return pb.TraceReply(state=pb.PARTIAL, claims=[claim(next_ref)])
        forward = self.request(
            request.query_id,
            next_ref,
            request.direction,
            request.hops_remaining - 1,
            request.deadline,
            [*request.visited, next_ref],
        )
        peer = self.local.counterparty[nexts[0], there]
        try:
            reply = self.call(
                peer, "Trace", forward, datetime.fromisoformat(request.deadline)
            )
        except grpc.RpcError as error:
            return pb.TraceReply(state=failure_state(error), claims=[claim(next_ref)])
        return pb.TraceReply(
            state=worst(state, reply.state), claims=[claim(next_ref), *reply.claims]
        )

    def Deliver(self, request, context):
        """Store a chain result pushed by the bank that assembled it."""
        claims, sender = list(request.claims), request.sender
        own = [i for i, c in enumerate(claims) if c.signer == c.recipient == sender]
        up, down = (claims[: own[0]][::-1], claims[own[0] + 1 :]) if own else ([], [])
        if (
            not own
            or request.state not in (pb.CONFIRMED, pb.PARTIAL)
            or self.bank not in {c.signer for c in claims}
            or chain_digest(claims) != request.chain_digest
            or not valid_chain(claims, self.public_keys)
            or not all(bound(leg, sender, leg[0].query_id) for leg in (up, down) if leg)
        ):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid chain result")
        # A cut chain is never confirmed, whatever the sender says.
        if not complete(claims):
            request.state = pb.PARTIAL
        self.store.save_chain(request)
        return pb.Ack(chain_digest=request.chain_digest)

    # ---- starting a search ----------------------------------------------

    def leg(self, direction, payment, ref, budget, deadline, visited):
        """Query the next bank in one direction and check the claims that come back."""
        query_id = str(uuid.uuid4())
        side = "out" if direction == pb.DOWNSTREAM else "in"
        request = self.request(
            query_id, ref, direction, budget, deadline.isoformat(), visited
        )
        try:
            reply = self.call(
                self.local.counterparty[payment, side], "Trace", request, deadline
            )
        except grpc.RpcError as error:
            return failure_state(error), []
        claims = list(reply.claims)
        fresh = bound(claims, self.bank, query_id)
        return (reply.state if fresh else pb.INVALID_CLAIM), claims

    def search(self, incoming, outgoing):
        """Trace both ways from a local detection, assemble and store for delivery."""
        search_id = hashlib.sha256(f"{self.bank}:{incoming}:{outgoing}".encode())
        search_id = search_id.hexdigest()
        deadline = now() + timedelta(seconds=SEARCH_SECONDS)
        in_ref = self.local.refs[incoming, "in"]
        out_ref = self.local.refs[outgoing, "out"]
        own = self.claim(self.bank, search_id, in_ref, out_ref)
        visited = [in_ref, out_ref]
        budget = HOP_LIMIT + 1 - len(visited)
        down_state, down = self.leg(
            pb.DOWNSTREAM, outgoing, out_ref, budget, deadline, visited
        )
        found = refs(down)
        new = {key(r) for r in found} - {key(r) for r in visited}
        visited += found
        up_state, up = self.leg(
            pb.UPSTREAM, incoming, in_ref, budget - len(new), deadline, visited
        )
        claims = [*reversed(up), own, *down]
        valid = valid_chain(claims, self.public_keys)
        cut = pb.CONFIRMED if complete(claims) else pb.PARTIAL
        state = worst(down_state, up_state, cut) if valid else pb.INVALID_CLAIM
        result = pb.ChainResult(
            chain_digest=chain_digest(claims) if valid else "",
            sender=self.bank,
            state=state,
            claims=claims,
        )
        if state in (pb.CONFIRMED, pb.PARTIAL):
            others = {c.signer for c in claims} - {self.bank}
            self.store.save_chain(result, deliver_to=sorted(others))
        self.store.put_search(search_id, incoming, outgoing, state, result.chain_digest)
        return result

    def search_detections(self):
        results = [
            self.search(d["incoming_payment_id"], d["outgoing_payment_id"])
            for d in self.local.detections
        ]
        self.deliver_pending()
        return results

    # ---- housekeeping ---------------------------------------------------

    def deliver_pending(self):
        for bank, result in self.store.pending_deliveries():
            try:
                self.call(bank, "Deliver", result, now() + timedelta(seconds=5))
                self.store.ack(result.chain_digest, bank)
            except grpc.RpcError:
                pass  # stays in the outbox until the bank is back

    def housekeeping(self):
        """Resume searches that needed an unavailable bank; redeliver unacknowledged results."""
        for incoming, outgoing in self.store.unfinished_searches():
            self.search(incoming, outgoing)
        self.deliver_pending()


def serve(node, port=0):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_PrivacyNodeServicer_to_server(node, server)
    port = server.add_insecure_port(f"[::]:{port}")  # M3: mTLS
    server.start()
    return server, port


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bank", required=True, choices="ABC")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--key-version", default="k1")
    parser.add_argument("--pairwise-key", nargs=2, action="append", default=[])
    parser.add_argument("--signing-key", required=True)
    parser.add_argument("--public-key", nargs=2, action="append", default=[])
    parser.add_argument("--peer", nargs=2, action="append", default=[])
    parser.add_argument("--db", default=":memory:")
    parser.add_argument("--search", action="store_true", help="trace local detections")
    parser.add_argument("--housekeeping-seconds", type=float, default=10)
    args = parser.parse_args()
    keys = {(args.bank, peer): path for peer, path in args.pairwise_key}
    local = LocalBank.from_workload(args.workload, args.bank, args.key_version, keys)
    node = Node(
        local,
        Store(args.db),
        load_signing_key(args.signing_key),
        {bank: load_verification_key(path) for bank, path in args.public_key},
        dict(args.peer),
        args.key_version,
    )
    server, port = serve(node, args.port)
    print(f"bank {args.bank} privacy node on port {port}", flush=True)
    if args.search:
        for result in node.search_detections():
            print(pb.State.Name(result.state), result.chain_digest[:12], flush=True)

    def loop():
        while True:
            time.sleep(args.housekeeping_seconds)
            try:
                node.housekeeping()
            except Exception:  # noqa: BLE001 - log and retry on the next pass
                traceback.print_exc()

    threading.Thread(target=loop, daemon=True).start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
