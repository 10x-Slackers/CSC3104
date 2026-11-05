"""Privacy node: peer-to-peer chain tracing between banks (protocol.md).

Run one per bank, e.g.:
  uv run python -m privacy_node.node --bank B --port 50052 \
    --workload apps/dataset/fixtures/tiny --key-version k1 \
    --x25519-private-key .secrets/B/private/x25519/k1.pem \
    --peer-x25519-public-key A .secrets/B/trusted/x25519/A/k1.pem \
    --peer-x25519-public-key C .secrets/B/trusted/x25519/C/k1.pem \
    --signing-key .secrets/B/private/signing/k1.pem --signing-key-version k1 \
    --public-key A k1 .secrets/B/trusted/signing/A/k1.pem \
    --public-key B k1 .secrets/B/trusted/signing/B/k1.pem \
    --public-key C k1 .secrets/B/trusted/signing/C/k1.pem \
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
from concurrent import futures
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import grpc

from privacy_node import privacy_node_pb2 as pb
from privacy_node import privacy_node_pb2_grpc as pb_grpc
from privacy_node.claims import (
    bound,
    chain_digest,
    check_result,
    claim_bytes,
    complete,
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
from security import (
    AgreementFiles,
    load_pairwise_keys,
    load_signing_key,
    load_verification_key,
    sign,
)

log = logging.getLogger(__name__)

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


def unreadable(reply):
    """A peer reply carrying a field or a state our .proto doesn't define.

    proto3 keeps an unknown enum number as a plain int in a known field, so
    has_unknown_fields can't see state=99; worst() would crash on it instead.
    """
    if has_unknown_fields(reply):
        return True
    return "state" in reply.DESCRIPTOR.fields_by_name and reply.state not in SEVERITY


# The same request may succeed later (https://grpc.io/docs/guides/status-codes/).
TRANSIENT = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.ABORTED,
    }
)


def failure_state(error):
    if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return pb.TIMED_OUT
    if error.code() in TRANSIENT:
        return pb.PENDING  # the bank may answer later: retried by housekeeping
    return pb.FAILED  # the peer rejected or failed the query: retrying won't help


class Node(pb_grpc.PrivacyNodeServicer):
    def __init__(
        self,
        local,
        store,
        signing_key,
        public_keys,
        peers,
        key_version,
        ingestion_grace_seconds=0,
        signing_key_version="k1",
        tls=None,
        reload_keys=None,
    ):
        self.bank = local.bank
        self.local, self.store = local, store
        self.signing_key, self.public_keys = signing_key, public_keys
        self.peers, self.key_version = peers, key_version
        self.signing_key_version, self.tls = signing_key_version, tls
        self.reload_keys = reload_keys
        self._key_reload = None
        self._channels = {}
        self._retired_channels = set()
        self._active_calls = {}
        self._channel_lock = threading.Lock()
        self._started_detections = set()
        if ingestion_grace_seconds < 0:
            raise ValueError("Ingestion grace must be nonnegative")
        self.ingestion_grace_seconds = ingestion_grace_seconds
        self._ingestion_deadlines = {}

    # ---- outgoing calls -------------------------------------------------

    def call(self, bank, method, request, deadline):
        """Reuse each peer channel until its mounted TLS credentials change."""
        target = self.peers[bank]
        material = self.tls.read() if self.tls else None
        with self._channel_lock:
            previous, channel = self._channels.get(target, (None, None))
            if channel is None or previous != material:
                if channel is not None:
                    self._retire_channel(channel)
                channel = (
                    self.tls.channel(target, material)
                    if self.tls
                    else grpc.insecure_channel(target)
                )
                self._channels[target] = material, channel
            self._active_calls[channel] = self._active_calls.get(channel, 0) + 1
        try:
            stub = pb_grpc.PrivacyNodeStub(channel)
            timeout = (deadline - now()).total_seconds()
            reply = getattr(stub, method)(request, timeout=timeout)
            if not unreadable(reply):
                return reply
            # Never relay or use what we cannot read. An Ack is only a receipt.
            log.warning("unreadable %s reply from bank=%s", method, bank)
            return (
                pb.TraceReply(state=pb.INVALID_CLAIM) if method == "Trace" else pb.Ack()
            )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.UNAVAILABLE:
                with self._channel_lock:
                    if self._channels.get(target, (None, None))[1] is channel:
                        del self._channels[target]
                        self._retire_channel(channel)
            raise
        finally:
            with self._channel_lock:
                self._active_calls[channel] -= 1
                if not self._active_calls[channel]:
                    del self._active_calls[channel]
                    if channel in self._retired_channels:
                        self._retire_channel(channel)

    def _retire_channel(self, channel):
        """Caller holds the channel lock; let any existing RPCs finish first."""
        if self._active_calls.get(channel):
            self._retired_channels.add(channel)
        else:
            self._retired_channels.discard(channel)
            channel.close()

    def close(self):
        """Release current and retired channels after the node has stopped."""
        channels = [channel for _, channel in self._channels.values()]
        for channel in channels + list(self._retired_channels):
            channel.close()
        self._channels.clear()
        self._retired_channels.clear()

    def request(
        self,
        query_id,
        ref,
        direction,
        hops_remaining,
        deadline,
        visited,
        key_version=None,
    ):
        return pb.TraceRequest(
            query_id=query_id,
            sender=self.bank,
            key_version=key_version or self.key_version,
            ref=ref,
            direction=direction,
            hops_remaining=hops_remaining,
            deadline=deadline,
            visited=visited,
        )

    def claim(
        self, recipient, query_id, in_ref, out_ref, key_version=None, truncated=False
    ):
        with self.local.lock:
            signing_key, signing_version = self.signing_key, self.signing_key_version
        c = pb.LinkClaim(
            signer=self.bank,
            recipient=recipient,
            query_id=query_id,
            key_version=key_version or self.key_version,
            signing_key_version=signing_version,
            in_ref=in_ref,
            out_ref=out_ref,
            truncated=truncated,
        )
        c.signature = sign(claim_bytes(c), signing_key)
        return c

    def set_query_version(self, version, peer_versions):
        with self.local.lock:
            self.local.index.set_query_version(version, peer_versions)
            self.key_version = version

    def reload_key_directory(self, root, config):
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
            candidate = self.local.index
            if not candidate.matches_versions(keys, config["query_version"]):
                candidate = deepcopy(candidate)
                for version, material in keys.items():
                    candidate.apply_version(version, material)
            candidate.set_query_version(
                config["query_version"], config["peer_versions"]
            )
            for version in set(candidate.active_versions) - set(active):
                candidate.retire_version(version)
            self.local.index = candidate
            self.key_version = config["query_version"]
            self.signing_key = signing_key
            self.signing_key_version = signing_version
            self.public_keys = public_keys

    # ---- answering a peer -----------------------------------------------

    def Trace(self, request, context):
        if self.tls:
            authenticated_bank(context, request.sender)
        if has_unknown_fields(request):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unknown fields")
        slot = (
            request.sender.encode()
            + b"\0"
            + request.key_version.encode()
            + b"\0"
            + request.ref.SerializeToString(deterministic=True)
        )
        stored = self.store.get_reply(request.query_id, slot)
        if stored:
            return stored  # retried query: same answer, no repeated work
        reply = self.follow(request)
        if reply.state not in (pb.PENDING, pb.TIMED_OUT):  # only cache final answers
            self.store.put_reply(request.query_id, slot, reply)
        return reply

    def follow(self, request):
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
        if self.tls:
            authenticated_bank(context, request.sender)
        try:
            checked = check_result(request, self.public_keys, self.bank)
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        self.store.save_chain(checked)
        return pb.Ack(chain_digest=checked.chain_digest)

    # ---- starting a search ----------------------------------------------

    def leg(self, direction, payment, ref, budget, deadline, visited, key_version):
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

    def search_detections(self):
        results = [
            self.search(d["incoming_payment_id"], d["outgoing_payment_id"])
            for d in self.local.detections
        ]
        self.deliver_pending()
        return results

    def search_new_detections(self, stop=None):
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

    def deliver_pending(self, stop=None):
        for bank, result in self.store.pending_deliveries():
            if stop is not None and stop.is_set():
                break
            try:
                self.call(bank, "Deliver", result, now() + timedelta(seconds=5))
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

    def housekeeping(self, stop=None):
        """Resume searches that needed an unavailable bank; redeliver unacknowledged results."""
        if self.reload_keys and (
            self._key_reload is None or not self._key_reload.is_alive()
        ):

            def reload():
                try:
                    self.reload_keys()
                except Exception:  # noqa: BLE001 - retain working keys and retry later
                    traceback.print_exc()

            self._key_reload = threading.Thread(target=reload, daemon=True)
            self._key_reload.start()
        for incoming, outgoing in self.store.unfinished_searches():
            if stop is not None and stop.is_set():
                break
            self.search(incoming, outgoing)
        self.deliver_pending(stop=stop)


def serve(node, port=0):
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
    parser.add_argument("--max-events", type=int, default=10000)
    parser.add_argument("--max-token-references", type=int)
    parser.add_argument("--key-version", default="k1")
    parser.add_argument(
        "--key-directory", help="per-bank key tree for reloadable active versions"
    )
    parser.add_argument("--key-config", help="mounted JSON active-version manifest")
    parser.add_argument("--x25519-private-key")
    parser.add_argument(
        "--peer-x25519-public-key", nargs=2, action="append", default=[]
    )
    parser.add_argument("--signing-key")
    parser.add_argument("--signing-key-version", default="k1")
    parser.add_argument(
        "--public-key",
        nargs=3,
        action="append",
        default=[],
        metavar=("BANK", "VERSION", "PATH"),
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
    if args.key_directory:
        config_path = Path(
            args.key_config or Path(args.key_directory) / args.bank / "active.json"
        )
        config = json.loads(config_path.read_text())
        args.key_version = config["query_version"]
        args.signing_key_version = config["signing_key_version"]
        keys = pair_paths(args.key_directory, args.bank, args.key_version)
        signing_key = own_key(args.key_directory, args.bank, args.signing_key_version)
        public_keys = trusted_public_keys(args.key_directory, args.bank)
    else:
        if not args.x25519_private_key or not args.signing_key:
            parser.error("provide --key-directory or explicit X25519/signing key files")
        keys = AgreementFiles(
            Path(args.x25519_private_key), dict(args.peer_x25519_public_key)
        )
        signing_key = load_signing_key(args.signing_key)
        public_keys = {
            (bank, version): load_verification_key(path)
            for bank, version, path in args.public_key
        }
    rule = {
        name: getattr(args, name)
        for name in ("min_ratio", "max_ratio", "min_delay", "window", "max_events")
    }
    rule["max_token_references"] = args.max_token_references
    if args.bootstrap_servers:
        local = LocalBank(args.bank, args.key_version, keys, **rule)
    else:
        local = LocalBank.from_workload(
            args.workload, args.bank, args.key_version, keys, **rule
        )
    node = Node(
        local,
        Store(args.db),
        signing_key,
        public_keys,
        dict(args.peer),
        args.key_version,
        ingestion_grace_seconds=args.ingestion_grace_seconds,
        signing_key_version=args.signing_key_version,
        tls=None
        if args.insecure_demo
        else TlsFiles(args.tls_cert, args.tls_key, args.tls_ca),
    )

    def reload_keys():
        if args.key_directory:
            config_path = Path(
                args.key_config or Path(args.key_directory) / args.bank / "active.json"
            )
            node.reload_key_directory(
                args.key_directory, json.loads(config_path.read_text())
            )
            return
        pair_keys = load_pairwise_keys(
            args.bank, args.key_version, keys.private_key, keys.peer_public_keys
        )
        signing_key = load_signing_key(args.signing_key)
        public_keys = {
            (bank, version): load_verification_key(path)
            for bank, version, path in args.public_key
        }
        with local.lock:
            if not local.index.matches_versions(
                {args.key_version: pair_keys}, args.key_version
            ):
                candidate = deepcopy(local.index)
                candidate.apply_version(args.key_version, pair_keys)
                local.index = candidate
            node.signing_key, node.public_keys = signing_key, public_keys

    node.reload_keys = reload_keys
    if args.key_directory:
        reload_keys()
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
        node.close()
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    main()
