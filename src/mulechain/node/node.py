"""Privacy node: the only part of a bank that talks to other banks.

    BANK=B python -m mulechain.node.node

When this bank's rules link an incoming transfer to an outgoing one, the node asks the
banks on either side, peer to peer, whether their rules move the same money on. Each
bank on the path answers with a signed claim and asks the next bank. Nobody sees
another bank's accounts, amounts or payment IDs: a transfer is named only by its token.

The patterns this file shows (docs/patterns.md):

- Zero trust. A caller is whoever its mTLS certificate says it is, never a field in
  its message, and a bank answers only about transfers that caller is party to.
- Deadline propagation. The bank that starts a search gives it 10 seconds. Every bank
  on the path passes on what is left of that, minus a margin to send its own reply.
- Retry with backoff. A search that got no answer is a row in the store, tried again
  later; a search is never lost because a bank was down.
- Transactional outbox. Whatever a chain owes (a copy for each bank on it, a message
  for this bank's case service) is stored with it and delivered until acknowledged.
- Advisory lock. Replicas share the store, and a PostgreSQL advisory lock lets one
  of them at a time do the background work.
"""

import json
import os
import signal
import threading
import time
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
from valkey import Valkey

from mulechain import BANKS, crypto, log
from mulechain.node import protocol_pb2 as pb
from mulechain.node import protocol_pb2_grpc as pb_grpc
from mulechain.node.bank import Bank
from mulechain.node.claims import claim_bytes, complete, digest, tokens, valid_chain
from mulechain.node.store import CASE, Store

HOP_LIMIT = 4  # a search follows at most HOP_LIMIT + 1 transfers
SEARCH_SECONDS = 10  # the whole search's deadline, set by the bank that starts it
REPLY_MARGIN = 0.25  # seconds each bank keeps back to send its own reply
CALL_SECONDS = 5  # for Deliver and Keys, which no search is waiting on
KEY_SECONDS = 10  # how often peers' public keys are fetched again
KEY_SETTLE_SECONDS = 120  # how long a change to this bank's keys waits before peers see it


def identity(bank: str) -> str:
    """The name in a bank's privacy-node certificate (its URI SAN)."""
    return f"spiffe://csc3104/bank/{bank}/privacy-node"


def caller(context: grpc.ServicerContext) -> str:
    """The bank on the other end of this call, read from its client certificate."""
    names = context.auth_context().get("x509_subject_alternative_name", ())
    banks = [bank for bank in BANKS if identity(bank).encode() in names]
    if len(banks) != 1:
        context.abort(grpc.StatusCode.UNAUTHENTICATED, "a bank's certificate is required")
    return banks[0]


def time_left(context: grpc.ServicerContext) -> float:
    """What remains of the caller's timeout. gRPC sends it as a duration, not a
    clock time, so banks need not agree on what time it is."""
    left = context.time_remaining()
    return SEARCH_SECONDS if left is None else min(left, SEARCH_SECONDS)


class Unanswered(Exception):
    """A bank on the path gave no answer in time."""

    def __init__(self, peer: str) -> None:
        self.peer = peer  # the neighbour this node asked; the silent bank may be further on


class Node(pb_grpc.PrivacyNodeServicer):
    def __init__(
        self,
        bank: Bank,
        store: Store,
        key_dir: str | Path,
        call: Callable,  # call(peer, method, request, timeout) -> reply; raises grpc.RpcError
        send_cases: Callable[[list[dict]], None],  # to this bank's case service
        grace: float = 0,
    ) -> None:
        self.name, self.bank, self.store = bank.name, bank, store
        self.key_dir, self.call, self.send_cases = key_dir, call, send_cases
        # Seconds the bank's scorers get to catch up before a link counts as inside it.
        self.grace = grace
        self.peers = [b for b in BANKS if b != self.name]
        self.own = crypto.load_keys(key_dir)
        self.published = self.latest = [k.public for k in self.own]  # what Keys answers
        self.latest_at = 0.0
        self.keys: dict[str, list[crypto.PublicKey]] = {}  # every bank's public keys
        self.keys_at = -KEY_SECONDS
        self.pool = ThreadPoolExecutor(max_workers=8)  # searches run side by side

    # ---- keys -------------------------------------------------------------------

    def Keys(self, request: pb.Empty, context: grpc.ServicerContext) -> pb.PublicKeys:
        return pb.PublicKeys(
            keys=[pb.PublicKey(agreement=k.agreement, signing=k.signing) for k in self.published]
        )

    def refresh_keys(self) -> set[str]:
        """Re-read this bank's keys and ask each peer for its public keys.

        They go to the store (so every replica has them after a restart) and to Valkey
        (where this bank's scorer derives its pair keys from them). Returns the peers
        that didn't answer.

        A change to this bank's own keys is published only once it has lasted
        KEY_SETTLE_SECONDS, because the bank's pods see a changed Secret up to a minute
        apart. By then every scorer here indexes under a new key before a peer names a
        transfer with it, and has stopped using a removed key before peers forget it.
        """
        self.own = crypto.load_keys(self.key_dir)
        current = [k.public for k in self.own]
        if crypto.encode(current) != crypto.encode(self.latest):
            self.latest, self.latest_at = current, time.monotonic()
        if time.monotonic() - self.latest_at >= KEY_SETTLE_SECONDS:
            self.published = self.latest
        silent = set()
        for peer in self.peers:
            try:
                reply = self.call(peer, "Keys", pb.Empty(), CALL_SECONDS)
            except grpc.RpcError:
                silent.add(peer)  # keep the keys it published last time
                continue
            if reply.keys:
                keys = [crypto.PublicKey(k.agreement, k.signing) for k in reply.keys]
                self.store.put_peer_keys(peer, crypto.encode(keys))
        known = self.store.peer_keys()
        if known:
            self.bank.publish_keys(known)
        peer_keys = {bank: crypto.decode(text) for bank, text in known.items()}
        self.keys = peer_keys | {self.name: current}
        self.keys_at = time.monotonic()
        return silent

    def valid(self, claims: list[pb.LinkClaim]) -> bool:
        """Check a chain's signatures; if that fails, fetch the keys again and recheck,
        because a peer may have just started or rotated its keys.

        Raises Unanswered if a signer's keys couldn't be fetched: the chain may be fine.
        """
        if valid_chain(claims, self.keys):
            return True
        silent = self.refresh_keys() & {c.signer for c in claims}
        if valid_chain(claims, self.keys):
            return True
        if silent:
            raise Unanswered(min(silent))
        return False

    def sign(
        self, in_from: str, in_token: bytes, out_to: str, out_token: bytes, cut: bool = False
    ) -> pb.LinkClaim:
        """This bank's claim: "my rule links the transfer in_token to the transfer out_token"."""
        claim = pb.LinkClaim(
            signer=self.name,
            in_from=in_from,
            in_token=in_token,
            out_to=out_to,
            out_token=out_token,
            cut=cut,
        )
        claim.signature = crypto.sign(self.own[0], claim_bytes(claim))
        return claim

    # ---- answering a peer -------------------------------------------------------

    def Trace(self, request: pb.TraceRequest, context: grpc.ServicerContext) -> pb.TraceReply:
        asker = caller(context)
        here, there = ("in", "out") if request.direction == pb.DOWNSTREAM else ("out", "in")
        payment = self.bank.lookup(request.token, here, asker)
        if not payment:
            return pb.TraceReply()  # not held here, or not the caller's transfer to ask about

        def claim(peer: str = "", token: bytes = b"", cut: bool = False) -> pb.LinkClaim:
            side = {here: (asker, request.token), there: (peer, token)}
            return self.sign(*side["in"], *side["out"], cut)

        nexts = self.bank.links(payment, here)
        if not nexts:  # the chain starts or ends at this bank
            return pb.TraceReply(claims=[claim()])
        cut = len(nexts) > 1  # a branch: follow one path and say that there is more
        peer, token = self.bank.peer(nexts[0], there), self.bank.token(nexts[0], there)
        if token in request.visited:  # the chain has come back round: a loop
            return pb.TraceReply(claims=[claim(peer, token, cut)])
        if request.hops_remaining <= 0:  # hop limit: name the next transfer, don't follow it
            return pb.TraceReply(claims=[claim(peer, token, cut=True)])
        onward = pb.TraceRequest(
            token=token,
            direction=request.direction,
            hops_remaining=request.hops_remaining - 1,
            visited=[*request.visited, token],
        )
        try:
            reply = self.call(peer, "Trace", onward, time_left(context) - REPLY_MARGIN)
        except grpc.RpcError as error:
            # The bank that started the search will try all of it again later.
            context.abort(grpc.StatusCode.UNAVAILABLE, f"bank {peer}: {error.code().name}")
        if not reply.claims:
            return pb.TraceReply()  # the next bank doesn't hold it (yet): no chain
        return pb.TraceReply(claims=[claim(peer, token, cut), *reply.claims])

    def Deliver(self, request: pb.Chain, context: grpc.ServicerContext) -> pb.Ack:
        """Store a chain another bank assembled. A repeat changes nothing (idempotent)."""
        sender, claims = caller(context), list(request.claims)
        signers = {c.signer for c in claims}
        try:
            valid = {self.name, sender} <= signers and self.valid(claims)
        except Unanswered as silent:  # can't check it yet: the sender will try again
            context.abort(grpc.StatusCode.UNAVAILABLE, f"no keys for bank {silent.peer}")
        if not valid:
            # The sender reads this code as "never send it again" (a dead letter).
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "not a valid chain of ours")
        self.save(claims, deliver_to=[])
        return pb.Ack()

    # ---- starting a search ------------------------------------------------------

    def ask(
        self,
        peer: str,
        token: bytes,
        direction: pb.Direction,
        hops: int,
        visited: list[bytes],
        deadline: float,
    ) -> list[pb.LinkClaim]:
        request = pb.TraceRequest(
            token=token, direction=direction, hops_remaining=hops, visited=visited
        )
        try:
            return list(self.call(peer, "Trace", request, deadline - time.monotonic()).claims)
        except grpc.RpcError as error:
            raise Unanswered(peer) from error

    def search(self, incoming: str, outgoing: str) -> list[pb.LinkClaim]:
        """Trace the money both ways from one of this bank's links.

        Returns the chain's claims in money-flow order, or [] if a neighbour doesn't
        hold its side of the transfer.
        """
        deadline = time.monotonic() + SEARCH_SECONDS
        in_from, in_token = self.bank.peer(incoming, "in"), self.bank.token(incoming, "in")
        out_to, out_token = self.bank.peer(outgoing, "out"), self.bank.token(outgoing, "out")
        own = self.sign(in_from, in_token, out_to, out_token)
        visited = [in_token, out_token]
        budget = HOP_LIMIT + 1 - len(visited)  # both ways share the hop limit
        down = self.ask(out_to, out_token, pb.DOWNSTREAM, budget, visited, deadline)
        if not down:
            return []
        new = set(tokens(down)) - set(visited)
        visited += sorted(new)
        up = self.ask(in_from, in_token, pb.UPSTREAM, budget - len(new), visited, deadline)
        if not up:
            return []
        return [*reversed(up), own, *down]

    def attempt(self, search: dict) -> str:
        """Try one stored search and return its new state.

        pending    try again later: a bank didn't answer, or doesn't hold its side of
                   the transfer yet
        moved      the link is inside this bank; another search took over from the
                   cross-bank transfers around it
        local      the money starts or stays inside this bank: a case for this bank only
        confirmed  a complete chain, signed by every bank on it
        partial    a chain that some bank cut short (hop limit or a branch)
        failed     the answers didn't verify
        """
        incoming, outgoing = search["incoming"], search["outgoing"]
        starts, ends = self.bank.ends(incoming, "in"), self.bank.ends(outgoing, "out")
        if not (starts and ends):
            if time.time() - search["started"] < self.grace:
                # an intra-bank link on another account may not be scored yet
                return "pending"
            legs = self.bank.internal_legs(incoming, outgoing)
            self.store.queue_case({"key": search["id"], "kind": "local", "legs": legs})
            return "local"
        if (starts[0], ends[0]) != (incoming, outgoing):
            self.store.add_search(starts[0], ends[0])  # one path through the bank
            return "moved"
        try:
            claims = self.search(incoming, outgoing)
            if claims and not self.valid(claims):
                return "failed"
        except Unanswered as silent:
            legs = [self.bank.leg(incoming, "in"), self.bank.leg(outgoing, "out")]
            waiting = {"key": search["id"], "kind": "pending", "waiting_for": silent.peer}
            self.store.queue_case(waiting | {"legs": legs})
            return "pending"
        if not claims:  # a bank on the path hasn't ingested its side yet
            return "pending"
        return self.save(claims, deliver_to=sorted({c.signer for c in claims} - {self.name}))

    def save(self, claims: list[pb.LinkClaim], deliver_to: list[str]) -> str:
        """Store a verified chain with the messages it owes (the outbox pattern).

        Returns its state: `confirmed`, or `partial` if a bank cut it short.
        """
        own_legs = []
        for claim in (c for c in claims if c.signer == self.name):
            for token, direction in ((claim.in_token, "in"), (claim.out_token, "out")):
                if token:
                    payment, _ = self.bank.held(token)
                    own_legs.append(self.bank.leg(payment, direction) | {"token": token.hex()})
        links = [
            {
                "signer": c.signer,
                "in_from": c.in_from,
                "in_token": c.in_token.hex(),
                "out_to": c.out_to,
                "out_token": c.out_token.hex(),
            }
            for c in claims
        ]
        key, state = digest(claims), "confirmed" if complete(claims) else "partial"
        case = {"key": key, "kind": "chain", "state": state, "legs": own_legs, "links": links}
        chain = pb.Chain(claims=claims).SerializeToString()
        self.store.save_chain(key, chain, deliver_to, case)
        return state

    # ---- background work --------------------------------------------------------

    def take_hits(self, block_ms: int | None = None) -> int:
        """Read the scorer's new rule hits from Valkey.

        Each hit becomes a case message, and a chain hit also a search. They are stored
        in the same transaction as the stream position, so a crash repeats nothing and
        skips nothing.
        """
        read = self.store.cursor()
        hits = self.bank.hits(read, block_ms)
        if not hits:
            return 0
        with self.store.transaction():
            for _, hit in hits:
                try:
                    legs = [self.bank.leg(hit["incoming"], "in")] if hit["incoming"] else []
                    legs.append(self.bank.leg(hit["outgoing"], "out"))
                except KeyError:  # its payments have expired from Valkey: nothing to show
                    continue
                self.store.queue_case(
                    {"key": hit["id"], "kind": "hit", "rule": hit["rule"], "legs": legs}
                )
                if hit["rule"] == "chain":
                    self.store.add_search(hit["incoming"], hit["outgoing"])
            self.store.set_cursor(hits[-1][0])
        # Earlier batches are stored. This one stays until the next pass, in case
        # PostgreSQL fails over to a replica that doesn't have it yet.
        self.bank.drop_hits(read)
        return len(hits)

    def run_searches(self) -> int:
        due = self.store.due_searches()

        def run(search: dict) -> None:
            start = time.monotonic()
            try:
                state = self.attempt(search)
            except KeyError:
                state = "pending"  # its payments aren't in Valkey: wait for the rebuild
            self.store.finish_search(search, state)
            log(
                "node",
                bank=self.name,
                event="search",
                search=search["id"],
                state=state,
                attempt=search["attempts"] + 1,
                ms=round((time.monotonic() - start) * 1000, 1),
            )

        list(self.pool.map(run, due))
        return len(due)

    def deliver(self) -> int:
        """Send what the outbox owes; anything not acknowledged stays for a later pass."""
        due = self.store.due_messages()
        self.deliver_chains([m for m in due if m["dest"] != CASE])
        self.deliver_cases([m for m in due if m["dest"] == CASE])
        return len(due)

    def deliver_chains(self, messages: list[dict]) -> None:
        down = set()  # a bank that failed in this pass isn't called again in it
        for message in messages:
            peer, start, state = message["dest"], time.monotonic(), "queued"
            if peer not in down:
                try:
                    chain = pb.Chain.FromString(bytes(message["body"]))
                    self.call(peer, "Deliver", chain, CALL_SECONDS)
                    outcome, state = "OK", "sent"
                except grpc.RpcError as error:
                    outcome = error.code().name
                    if outcome == "INVALID_ARGUMENT":
                        state = "dead"  # the bank read it and refused: sending again can't help
                    else:
                        down.add(peer)
                log(
                    "node",
                    bank=self.name,
                    event="deliver",
                    chain=message["key"],
                    peer=peer,
                    outcome=outcome,
                    ms=round((time.monotonic() - start) * 1000, 1),
                )
            self.store.settle([message], state)

    def deliver_cases(self, messages: list[dict]) -> None:
        if messages:
            try:
                self.send_cases([json.loads(bytes(m["body"])) for m in messages])  # one batch
                self.store.settle(messages, "sent")
            except OSError:
                self.store.settle(messages, "queued")

    def work_once(self, block_ms: int | None = None) -> int | None:
        """One pass of the background work.

        Returns how much there was to do, or None if another replica holds the lock.
        """
        if time.monotonic() - self.keys_at >= KEY_SECONDS:
            self.refresh_keys()  # every replica, since every replica checks signatures
        with self.store.housekeeping() as mine:
            if not mine:
                return None
            return self.take_hits(block_ms) + self.run_searches() + self.deliver()

    def work(self, stop: threading.Event) -> None:
        done = 0
        while not stop.is_set():
            try:
                # Wait for new hits only when the last pass found nothing to do.
                done = self.work_once(block_ms=None if done else 1000)
                if done is None:
                    stop.wait(1)
            except Exception as error:  # noqa: BLE001 - e.g. Valkey or PostgreSQL restarting
                log("node", bank=self.name, error=repr(error))
                stop.wait(1)


class Mtls:
    """Mutual TLS between banks, from the files cert-manager mounts and renews.

    Both sides present a certificate signed by the lab CA. The files are read again
    for every new connection, so a renewed certificate is used without a restart.
    """

    def __init__(self, directory: str, peers: dict[str, str]) -> None:
        self.directory, self.peers = Path(directory), peers  # bank -> host:port

    def read(self) -> list[bytes]:
        return [(self.directory / name).read_bytes() for name in ("ca.crt", "tls.key", "tls.crt")]

    def call(self, peer: str, method: str, request, timeout: float):
        """One channel per call, so the peer's Service spreads calls over its replicas."""
        credentials = grpc.ssl_channel_credentials(*self.read())
        with grpc.secure_channel(self.peers[peer], credentials) as channel:
            return getattr(pb_grpc.PrivacyNodeStub(channel), method)(request, timeout=timeout)

    def server_credentials(self) -> grpc.ServerCredentials:
        def configuration() -> grpc.ServerCertificateConfiguration:
            ca, key, certificate = self.read()
            return grpc.ssl_server_certificate_configuration([(key, certificate)], ca)

        return grpc.dynamic_ssl_server_credentials(
            configuration(), configuration, require_client_authentication=True
        )


def serve(node: Node, tls: Mtls, port: int = 50051) -> grpc.Server:
    server = grpc.server(ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_PrivacyNodeServicer_to_server(node, server)
    server.add_secure_port(f"[::]:{port}", tls.server_credentials())
    server.start()
    return server


def post_to(url: str) -> Callable[[list[dict]], None]:
    def send(messages: list[dict]) -> None:
        body, headers = json.dumps(messages).encode(), {"Content-Type": "application/json"}
        urllib.request.urlopen(urllib.request.Request(url, body, headers), timeout=10).close()

    return send


def main() -> None:
    name = os.environ["BANK"]
    # PEERS="B=privacy-node.bank-b.svc.cluster.local:50051,C=..."
    tls = Mtls(os.environ["TLS_DIR"], dict(p.split("=") for p in os.environ["PEERS"].split(",")))
    node = Node(
        Bank(name, Valkey.from_url(os.environ["VALKEY_URL"], decode_responses=True)),
        Store(os.environ["DATABASE_URL"]),
        os.environ["KEY_DIR"],
        tls.call,
        post_to(os.environ["CASE_URL"] + "/messages"),
        grace=float(os.environ.get("INGESTION_GRACE_SECONDS", "60")),
    )
    server = serve(node, tls)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    node.work(stop)
    server.stop(CALL_SECONDS).wait()


if __name__ == "__main__":
    main()
