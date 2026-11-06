"""Privacy node: peer-to-peer chain tracing between banks (protocol.md).

Run one per bank, e.g.:
  uv run python -m privacy_node.node --bank B --port 50052 \
    --workload apps/dataset/fixtures/tiny --key-directory .secrets \
    --tls-cert /run/tls/tls.crt --tls-key /run/tls/tls.key --tls-ca /run/tls/ca.crt \
    --peer A privacy-node.bank-a.svc.cluster.local:50051 \
    --peer C privacy-node.bank-c.svc.cluster.local:50051 --search

For live Kafka state, replace --workload with --bootstrap-servers <brokers>
and use a fresh --group to rebuild state. --search starts live candidates.
"""

import argparse
import hashlib
import json
import logging
import signal
import threading
import traceback
import uuid
from collections.abc import Callable, Iterable, Mapping
from concurrent import futures
from copy import copy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from google.protobuf.message import Message

from privacy_node import privacy_node_pb2 as pb
from privacy_node import privacy_node_pb2_grpc as pb_grpc
from privacy_node.claims import (
    bound,
    chain_digest,
    check_result,
    claim_bytes,
    complete,
    frame,
    has_unknown_fields,
    key,
    refs,
    valid_chain,
)
from privacy_node.keys import own_key, pair_paths
from privacy_node.keys import public_keys as trusted_public_keys
from privacy_node.local_state import LocalBank
from privacy_node.store import Store
from privacy_node.transport import TlsFiles, authenticated_bank
from scorer.stream import consume
from security import PublicKeys, load_pairwise_keys, sign

log = logging.getLogger(__name__)

HOP_LIMIT = 4  # transfers are hops 0..HOP_LIMIT, so a chain holds at most 5
BRANCH_CAP = 1  # linear chains only; extra local links make the result PARTIAL
SEARCH_SECONDS = 10  # whole-search deadline, carried unchanged by every hop
# Worst state wins when two results are combined.
SEVERITY = [
    *(pb.CONFIRMED, pb.PARTIAL, pb.NO_MATCH, pb.PENDING, pb.TIMED_OUT),
    *(pb.FAILED, pb.INVALID_CLAIM),
]


def now() -> datetime:
    return datetime.now(UTC)


UNREADABLE_TRACE = pb.TraceReply(state=pb.INVALID_CLAIM)


def worst(*states: pb.State) -> pb.State:
    return max(states, key=SEVERITY.index)


def unreadable(reply: Message) -> bool:
    """A peer reply carrying a field or a state our .proto doesn't define.

    proto3 keeps an unknown enum number as a plain int in a known field, so
    has_unknown_fields can't see state=99; worst() would crash on it instead.
    """
    return (
        has_unknown_fields(reply)
        or getattr(reply, "state", pb.NO_MATCH) not in SEVERITY
    )


# The same request may succeed later (https://grpc.io/docs/guides/status-codes/).
TRANSIENT = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.ABORTED,
    }
)


def failure_state(error: grpc.RpcError) -> pb.State:
    if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return pb.TIMED_OUT
    if error.code() in TRANSIENT:
        return pb.PENDING  # the bank may answer later: retried by housekeeping
    return pb.FAILED  # the peer rejected or failed the query: retrying won't help


class Node(pb_grpc.PrivacyNodeServicer):
    def __init__(
        self,
        local: LocalBank,
        store: Store,
        signing_key: Ed25519PrivateKey,
        public_keys: PublicKeys,
        peers: Mapping[str, str],
        ingestion_grace_seconds: float = 0,
        signing_key_version: str = "k1",
        tls: TlsFiles | None = None,
    ) -> None:
        self.bank = local.bank
        self.local, self.store = local, store
        self.signing_key, self.public_keys = signing_key, public_keys
        self.peers = peers
        self.signing_key_version, self.tls = signing_key_version, tls
        self.reload_keys: Callable[[], None] | None = None
        self._key_reload = None
        self._started_detections = set()
        if ingestion_grace_seconds < 0:
            raise ValueError("Ingestion grace must be nonnegative")
        self.ingestion_grace_seconds = ingestion_grace_seconds
        self._ingestion_deadlines = {}

    # ---- outgoing calls -------------------------------------------------

    @property
    def key_version(self) -> str:
        return self.local.index.key_version

    def call[R: Message](
        self, bank: str, method: str, request: Message, deadline: datetime, fallback: R
    ) -> R:
        """Open one channel per call so Service balancing can select a replica.

        An unreadable reply is logged and replaced by fallback.
        """
        target = self.peers[bank]
        with (
            self.tls.channel(target) if self.tls else grpc.insecure_channel(target)
        ) as channel:
            stub = pb_grpc.PrivacyNodeStub(channel)
            timeout = (deadline - now()).total_seconds()
            reply = getattr(stub, method)(request, timeout=timeout)
        if not unreadable(reply):
            return reply
        log.warning("unreadable %s reply from bank=%s", method, bank)
        return copy(fallback)  # callers may change their reply

    def request(
        self,
        query_id: str,
        ref: pb.TokenRef,
        direction: pb.Direction,
        hops_remaining: int,
        deadline: str,
        visited: Iterable[pb.TokenRef],
        key_version: str,
    ) -> pb.TraceRequest:
        return pb.TraceRequest(
            query_id=query_id,
            sender=self.bank,
            key_version=key_version,
            ref=ref,
            direction=direction,
            hops_remaining=hops_remaining,
            deadline=deadline,
            visited=visited,
        )

    def claim(
        self,
        recipient: str,
        query_id: str,
        in_ref: pb.TokenRef,
        out_ref: pb.TokenRef,
        key_version: str,
        truncated: bool = False,
    ) -> pb.LinkClaim:
        with self.local.lock:
            signing_key, signing_version = self.signing_key, self.signing_key_version
        c = pb.LinkClaim(
            signer=self.bank,
            recipient=recipient,
            query_id=query_id,
            key_version=key_version,
            signing_key_version=signing_version,
            in_ref=in_ref,
            out_ref=out_ref,
            truncated=truncated,
        )
        c.signature = sign(claim_bytes(c), signing_key)
        return c

    def reload_key_directory(self, root: str | Path, config: dict) -> None:
        """Apply a trusted mounted active-version manifest without restarting."""
        active = config["active_versions"]
        keys = {}
        for version in active:
            files = pair_paths(root, self.bank, version)
            keys[version] = load_pairwise_keys(
                self.bank, version, files.private_key, files.peer_public_keys
            )
        signing_version = config["signing_key_version"]
        signing_key = own_key(root, self.bank, signing_version)
        public_keys = trusted_public_keys(root, self.bank)
        with self.local.lock:
            index = self.local.index
            for version, material in keys.items():
                index.check_version(version, material)
            index.check_query_version(
                config["query_version"], config["peer_versions"], keys
            )
            candidate = index
            if not index.matches_versions(keys, config["query_version"]):
                candidate = copy(index)
                candidate._keys = index._keys.copy()
                candidate._entries = index._entries.copy()
                for version, material in keys.items():
                    candidate.apply_version(version, material)
                candidate.set_query_version(
                    config["query_version"], config["peer_versions"]
                )
                for version in set(candidate.active_versions) - set(active):
                    candidate.retire_version(version)
            self.local.index = candidate
            self.signing_key = signing_key
            self.signing_key_version = signing_version
            self.public_keys = public_keys

    # ---- answering a peer -----------------------------------------------

    def Trace(
        self, request: pb.TraceRequest, context: grpc.ServicerContext
    ) -> pb.TraceReply:
        if self.tls:
            authenticated_bank(context, request.sender)
        if has_unknown_fields(request):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unknown fields")
        slot = frame(
            [
                request.sender.encode(),
                request.key_version.encode(),
                request.ref.SerializeToString(deterministic=True),
            ]
        )
        stored = self.store.get_reply(request.query_id, slot)
        if stored:
            return stored  # retried query: same answer, no repeated work
        reply = self.follow(request)
        if reply.state not in (pb.PENDING, pb.TIMED_OUT):  # only cache final answers
            self.store.put_reply(request.query_id, slot, reply)
        return reply

    def follow(self, request: pb.TraceRequest) -> pb.TraceReply:
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

    def extend(
        self, request: pb.TraceRequest, payment: str, down: bool
    ) -> pb.TraceReply:
        """Sign this bank's link for the transfer and follow it one more hop."""
        here, there = ("in", "out") if down else ("out", "in")
        this_ref = request.ref
        truncated = False

        def claim(other):
            in_ref, out_ref = (this_ref, other) if down else (other, this_ref)
            return self.claim(
                request.sender,
                request.query_id,
                in_ref,
                out_ref,
                request.key_version,
                truncated,
            )

        nexts = self.local.links(payment, here)
        if not nexts:  # the chain starts or ends at this bank
            return pb.TraceReply(state=pb.CONFIRMED, claims=[claim(pb.TokenRef())])
        truncated = len(nexts) > BRANCH_CAP
        state = pb.PARTIAL if truncated else pb.CONFIRMED
        next_ref = self.local.ref(nexts[0], there, request.key_version)
        if key(next_ref) in {key(r) for r in request.visited}:  # loop closed
            return pb.TraceReply(state=state, claims=[claim(next_ref)])
        if request.hops_remaining <= 0:  # hop limit: next_ref is left unfollowed
            truncated = True
            return pb.TraceReply(state=pb.PARTIAL, claims=[claim(next_ref)])
        forward = self.request(
            request.query_id,
            next_ref,
            request.direction,
            request.hops_remaining - 1,
            request.deadline,
            [*request.visited, next_ref],
            request.key_version,
        )
        peer = self.local.counterparty[nexts[0], there]
        try:
            deadline = datetime.fromisoformat(request.deadline)
            reply = self.call(peer, "Trace", forward, deadline, UNREADABLE_TRACE)
        except grpc.RpcError as error:
            return pb.TraceReply(state=failure_state(error), claims=[claim(next_ref)])
        return pb.TraceReply(
            state=worst(state, reply.state), claims=[claim(next_ref), *reply.claims]
        )

    def Deliver(self, request: pb.ChainResult, context: grpc.ServicerContext) -> pb.Ack:
        """Store a chain result pushed by the bank that assembled it."""
        if self.tls:
            authenticated_bank(context, request.sender)
        try:
            checked = check_result(request, self.public_keys, self.bank)
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        self.store.save_chain(checked)
        return pb.Ack(chain_digest=checked.chain_digest)

    # ---- starting a search ----------------------------------------------

    def leg(
        self,
        direction: pb.Direction,
        payment: str,
        ref: pb.TokenRef,
        budget: int,
        deadline: datetime,
        visited: Iterable[pb.TokenRef],
        key_version: str,
    ) -> tuple[pb.State, list[pb.LinkClaim]]:
        """Query the next bank in one direction and check the claims that come back."""
        query_id = str(uuid.uuid4())
        side = "out" if direction == pb.DOWNSTREAM else "in"
        request = self.request(
            query_id, ref, direction, budget, deadline.isoformat(), visited, key_version
        )
        try:
            reply = self.call(
                self.local.counterparty[payment, side],
                "Trace",
                request,
                deadline,
                UNREADABLE_TRACE,
            )
        except grpc.RpcError as error:
            return failure_state(error), []
        claims = list(reply.claims)
        fresh = bound(claims, self.bank, query_id)
        return (reply.state if fresh else pb.INVALID_CLAIM), claims

    def search(self, incoming: str, outgoing: str) -> pb.ChainResult:
        """Trace both ways from a local detection, assemble and store for delivery."""
        search_id = hashlib.sha256(f"{self.bank}:{incoming}:{outgoing}".encode())
        search_id = search_id.hexdigest()
        retry_until = self._ingestion_deadlines.setdefault(
            search_id, now() + timedelta(seconds=self.ingestion_grace_seconds)
        )
        deadline = now() + timedelta(seconds=SEARCH_SECONDS)
        with self.local.lock:
            key_version = self.key_version
            in_ref = self.local.ref(incoming, "in", key_version)
            out_ref = self.local.ref(outgoing, "out", key_version)
            own = self.claim(self.bank, search_id, in_ref, out_ref, key_version)
        visited = [in_ref, out_ref]
        budget = HOP_LIMIT + 1 - len(visited)
        down_state, down = self.leg(
            pb.DOWNSTREAM, outgoing, out_ref, budget, deadline, visited, key_version
        )
        found = refs(down)
        new = {key(r) for r in found} - {key(r) for r in visited}
        visited += found
        up_state, up = self.leg(
            pb.UPSTREAM,
            incoming,
            in_ref,
            budget - len(new),
            deadline,
            visited,
            key_version,
        )
        claims = [*reversed(up), own, *down]
        valid = valid_chain(claims, self.public_keys)
        cut = pb.CONFIRMED if complete(claims) else pb.PARTIAL
        state = worst(down_state, up_state, cut) if valid else pb.INVALID_CLAIM
        if state == pb.NO_MATCH and now() < retry_until:
            # Fresh query IDs let delayed ingestion be checked again.
            state = pb.PENDING
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

    def search_detections(self) -> list[pb.ChainResult]:
        results = [
            self.search(d["incoming_payment_id"], d["outgoing_payment_id"])
            for d in self.local.detections
        ]
        self.deliver_pending()
        return results

    def search_new_detections(
        self, stop: threading.Event | None = None
    ) -> list[pb.ChainResult]:
        """Start each live candidate once; housekeeping owns pending retries."""
        results = []
        for detection in self.local.detections:
            if stop is not None and stop.is_set():
                break
            identity = detection["detection_id"]
            if identity not in self._started_detections:
                results.append(
                    self.search(
                        detection["incoming_payment_id"],
                        detection["outgoing_payment_id"],
                    )
                )
                self._started_detections.add(identity)
        self.deliver_pending(stop=stop)
        return results

    # ---- housekeeping ---------------------------------------------------

    def deliver_pending(self, stop: threading.Event | None = None) -> None:
        for bank, result in self.store.pending_deliveries():
            if stop is not None and stop.is_set():
                break
            try:
                self.call(
                    bank, "Deliver", result, now() + timedelta(seconds=5), pb.Ack()
                )
                self.store.ack(result.chain_digest, bank)
            except grpc.RpcError as error:
                if error.code() != grpc.StatusCode.INVALID_ARGUMENT:
                    continue  # stays in the outbox until the bank is back
                # The receiver read it and said no, so resending can't help.
                self.store.dead_letter(result.chain_digest, bank)
                log.warning(
                    "dead-lettered chain_digest=%s bank=%s reason=%s",
                    result.chain_digest,
                    bank,
                    error.details(),
                )

    def housekeeping(self, stop: threading.Event | None = None) -> None:
        """Resume searches that needed an unavailable bank; redeliver unacknowledged results."""
        reload_keys = self.reload_keys
        if reload_keys and (
            self._key_reload is None or not self._key_reload.is_alive()
        ):

            def reload():
                try:
                    reload_keys()
                except Exception:  # noqa: BLE001 - retain working keys and retry later
                    traceback.print_exc()

            self._key_reload = threading.Thread(target=reload, daemon=True)
            self._key_reload.start()
        for incoming, outgoing in self.store.unfinished_searches():
            if stop is not None and stop.is_set():
                break
            self.search(incoming, outgoing)
        self.deliver_pending(stop=stop)


def serve(node: Node, port: int = 0) -> tuple[grpc.Server, int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_PrivacyNodeServicer_to_server(node, server)
    port = (
        server.add_secure_port(f"[::]:{port}", node.tls.server_credentials())
        if node.tls
        else server.add_insecure_port(f"[::]:{port}")
    )
    server.start()
    return server, port


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bank", required=True, choices="ABC")
    parser.add_argument("--port", type=int, default=50051)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--workload", help="standalone CSV initialization")
    source.add_argument("--bootstrap-servers", help="integrated bank-local Kafka mode")
    parser.add_argument(
        "--group", help="Kafka group; use a fresh group to rebuild state"
    )
    parser.add_argument("--min-ratio", default="0.90")
    parser.add_argument("--max-ratio", default="0.98")
    parser.add_argument("--min-delay", type=float, default=60)
    parser.add_argument("--window", type=float, default=1800)
    parser.add_argument("--max-token-references", type=int)
    parser.add_argument(
        "--key-directory", required=True, help="this bank's mounted key tree"
    )
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    parser.add_argument("--tls-ca")
    parser.add_argument("--insecure-demo", action="store_true")
    parser.add_argument("--peer", nargs=2, action="append", default=[])
    parser.add_argument("--db", default=":memory:")
    parser.add_argument("--search", action="store_true", help="trace local detections")
    parser.add_argument("--housekeeping-seconds", type=float, default=10)
    parser.add_argument(
        "--ingestion-grace-seconds",
        type=float,
        default=0,
        help="retry NO_MATCH for this bounded period; default: disabled",
    )
    args = parser.parse_args()
    if args.housekeeping_seconds <= 0 or args.ingestion_grace_seconds < 0:
        parser.error(
            "Housekeeping interval must be positive and ingestion grace nonnegative"
        )
    if not args.insecure_demo and not all((args.tls_cert, args.tls_key, args.tls_ca)):
        parser.error(
            "mTLS requires --tls-cert, --tls-key and --tls-ca (local tests: --insecure-demo)"
        )
    config_path = Path(args.key_directory) / args.bank / "active.json"

    def read_config():
        return json.loads(config_path.read_text())

    config = read_config()
    query_version = config["query_version"]
    if query_version not in config["active_versions"]:
        parser.error("Query version must be active")
    keys = pair_paths(args.key_directory, args.bank, query_version)
    rule = {
        name: getattr(args, name)
        for name in ("min_ratio", "max_ratio", "min_delay", "window")
    }
    rule["max_token_references"] = args.max_token_references
    if args.bootstrap_servers:
        local = LocalBank(args.bank, query_version, keys, **rule)
    else:
        local = LocalBank.from_workload(
            args.workload, args.bank, query_version, keys, **rule
        )
    for version in config["active_versions"]:
        if version != query_version:
            local.index.introduce_version(
                version, pair_paths(args.key_directory, args.bank, version)
            )
    local.index.set_query_version(query_version, config["peer_versions"])
    node = Node(
        local,
        Store(args.db),
        own_key(args.key_directory, args.bank, config["signing_key_version"]),
        trusted_public_keys(args.key_directory, args.bank),
        dict(args.peer),
        ingestion_grace_seconds=args.ingestion_grace_seconds,
        signing_key_version=config["signing_key_version"],
        tls=None
        if args.insecure_demo
        else TlsFiles(args.tls_cert, args.tls_key, args.tls_ca),
    )
    node.reload_keys = lambda: node.reload_key_directory(
        args.key_directory, read_config()
    )
    server, port = serve(node, args.port)
    print(f"bank {args.bank} privacy node on port {port}", flush=True)
    if args.search and args.workload:
        for result in node.search_detections():
            print(pb.State.Name(result.state), result.chain_digest[:12], flush=True)

    stop = threading.Event()
    handlers = {
        sig: signal.signal(sig, lambda *_: stop.set())
        for sig in (signal.SIGINT, signal.SIGTERM)
    }

    def loop():
        while not stop.wait(args.housekeeping_seconds):
            try:
                if args.search and args.bootstrap_servers:
                    node.search_new_detections(stop=stop)
                node.housekeeping(stop=stop)
            except Exception:  # noqa: BLE001 - log and retry on the next pass
                traceback.print_exc()

    worker = threading.Thread(target=loop, daemon=True)
    worker.start()
    try:
        if args.bootstrap_servers:
            from confluent_kafka import Consumer

            consumer = Consumer(
                {
                    "bootstrap.servers": args.bootstrap_servers,
                    "group.id": args.group or f"privacy-node-{args.bank}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            consume(
                consumer,
                local.scorer,
                process_event=local.process,
                stop_event=stop,
                require_replay=True,
            )
        else:
            while not stop.is_set():
                if not server.wait_for_termination(timeout=1):
                    break
    finally:
        stop.set()
        server.stop(0)
        worker.join(timeout=SEARCH_SECONDS + 5)
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    main()
