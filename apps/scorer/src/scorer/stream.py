"""The live scorer: consume `bank-<X>-transactions`, keep state in the bank's Valkey.

    SCORER_BANK=B VALKEY_URL=valkey://valkey:6379 python -m scorer.stream

Settings come from the environment (deployment.md, Scorer row); a CLI flag overrides
each one. Offsets are stored after each event's Valkey write and committed in the
background, so a restart carries on where it left off. consume() below is the older
in-memory path the privacy node still runs in-process.
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TextIO

from confluent_kafka import Consumer, KafkaException, TopicPartition
from valkey import Valkey

from ingestor import Event
from scorer.score import Link, Scorer
from scorer.state import ValkeyState
from scorer.token_index import TokenIndex
from security import AgreementFiles, canonical_pair, load_pairwise_keys

RELOAD_SECONDS = 10  # how often active.json is checked, as the node does


def topic(bank: str) -> str:
    return f"bank-{bank}-transactions"


def require_full_replay(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """Reject offsets or assignments that cannot rebuild this in-memory bank."""
    if len(partitions) != 1 or partitions[0].partition != 0:
        raise RuntimeError(
            "Integrated mode requires one partition and one consumer per bank"
        )
    partition = partitions[0]
    metadata = consumer.list_topics(topic=partition.topic, timeout=10)
    if len(metadata.topics[partition.topic].partitions) != 1:
        raise RuntimeError("Integrated mode requires one partition per bank")
    low, _ = consumer.get_watermark_offsets(partition, timeout=10)
    committed = consumer.committed(partitions, timeout=10)[0]
    if committed.error:
        raise KafkaException(committed.error)
    if low != 0 or committed.offset > 0:
        raise RuntimeError(
            "Full replay required: use a fresh group with records retained from offset zero"
        )


def consume(
    consumer: Consumer,
    scorer: Scorer,
    output: TextIO = sys.stdout,
    token_index: TokenIndex | None = None,
    process_event: Callable[[Event], Iterable[Link]] | None = None,
    stop_event: threading.Event | None = None,
    require_replay: bool = False,
) -> None:
    try:
        if process_event is not None and token_index is not None:
            raise ValueError(
                "Use the shared state's processor or a separate token index"
            )
        if token_index is not None and token_index.bank != scorer.bank:
            raise ValueError("Token index belongs to another bank")
        if require_replay:
            consumer.subscribe([topic(scorer.bank)], on_assign=require_full_replay)
        else:
            consumer.subscribe([topic(scorer.bank)])
        while stop_event is None or not stop_event.is_set():
            message = consumer.poll(1)
            if message is None:
                continue
            error = message.error()
            if error:
                # librdkafka retries transient errors (e.g. topic not created yet) itself.
                if error.fatal():
                    raise KafkaException(error)
                print(error, file=sys.stderr)
                continue
            # Fail without committing wrongly routed records.
            event = json.loads(message.value() or b"")
            if (
                message.topic() != topic(scorer.bank)
                or message.key() != event["account"].encode()
            ):
                raise ValueError("wrong topic or account key")
            if process_event is None:
                if token_index is not None:
                    token_index.add(event)
                results = scorer.process(event)
            else:
                results = process_event(event)
            for result in results:
                print(json.dumps(result, sort_keys=True), file=output)
            output.flush()
            committed = consumer.commit(message=message, asynchronous=False)
            for partition in committed:
                if partition.error:
                    raise KafkaException(partition.error)
    finally:
        consumer.close()


def log(output: TextIO, bank: str, **fields: object) -> None:
    line = {"ts": time.time(), "service": "scorer", "bank": bank} | fields
    print(json.dumps(line), file=output, flush=True)


def serve(
    consumer: Consumer,
    scorer: Scorer,
    stop: threading.Event,
    output: TextIO = sys.stdout,
    reload: Callable[[], None] | None = None,
    reload_every: float = RELOAD_SECONDS,
) -> None:
    """Score each event, then store its offset. Stops between events when stop is set.

    reload() runs between events every reload_every seconds. If a key file or
    active.json is missing or bad, the scorer logs it and carries on with the keys it has.
    """
    next_reload = time.monotonic() + reload_every
    try:
        consumer.subscribe([topic(scorer.bank)])
        while not stop.is_set():
            if reload and time.monotonic() >= next_reload:
                next_reload = time.monotonic() + reload_every
                try:
                    reload()
                except (OSError, ValueError, KeyError) as error:
                    log(
                        output,
                        scorer.bank,
                        level="error",
                        error=f"key reload: {error!r}",
                    )
            message = consumer.poll(1)
            if message is None:
                continue
            error = message.error()
            if error:
                if error.fatal():
                    raise KafkaException(error)
                # librdkafka retries transient errors (e.g. topic not created yet) itself
                log(output, scorer.bank, level="error", error=str(error))
                continue
            event = json.loads(message.value() or b"")
            if (
                message.topic() != topic(scorer.bank)
                or message.key() != event["account"].encode()
            ):
                raise ValueError("wrong topic or account key")
            links = scorer.process(event)
            if links or scorer.fired:
                log(
                    output,
                    scorer.bank,
                    payment_id=event["payment_id"],
                    kafka_ts=message.timestamp()[1]
                    / 1000,  # when the ingestor wrote it
                    links=[
                        [x["incoming_payment_id"], x["outgoing_payment_id"]]
                        for x in links
                    ],
                    hits=scorer.fired,
                )
            consumer.store_offsets(message=message)
    finally:
        consumer.close()  # also commits the stored offsets


def key_files(root: Path, bank: str, version: str) -> AgreementFiles:
    """One version's X25519 files in a bank's key directory (security.md layout)."""
    trusted = root / "trusted" / "x25519"
    return AgreementFiles(
        root / "private" / "x25519" / f"{version}.pem",
        {
            peer.name: peer / f"{version}.pem"
            for peer in trusted.glob("*")
            # the k3d keys also trust this bank's own public key; it isn't a peer
            if peer.name != bank and (peer / f"{version}.pem").is_file()
        },
    )


def key_index(bank: str, key_dir: str | Path) -> tuple[TokenIndex, dict]:
    """Token keys for every version in <key_dir>/<bank>/active.json, and that config."""
    root = Path(key_dir) / bank
    config = json.loads((root / "active.json").read_text())
    query = config["query_version"]
    index = TokenIndex(bank, query, key_files(root, bank, query))
    for version in config["active_versions"]:
        if version != query:
            index.introduce_version(version, key_files(root, bank, version))
    return index, config


def reload_keys(scorer: Scorer, key_dir: str | Path, applied: dict) -> dict:
    """Apply a changed active.json between events: the rotation steps in security.md.

    New versions are introduced and backfilled from the pay: entries, the query
    version switches once every peer has it, and versions no longer listed retire;
    their tok: keys expire with the TTL. Anything invalid raises before the index
    changes, so the old keys stay in use. Returns the config now applied.
    """
    root = Path(key_dir) / scorer.bank
    config = json.loads((root / "active.json").read_text())
    if config == applied:
        return applied
    index = scorer.token_index
    keys = {}
    for version in config["active_versions"]:
        files = key_files(root, scorer.bank, version)
        keys[version] = load_pairwise_keys(
            scorer.bank, version, files.private_key, files.peer_public_keys
        )
        index.check_version(version, keys[version])
    index.check_query_version(config["query_version"], config["peer_versions"], keys)

    # compared with what was applied, not with the index, so a retry after a
    # backfill that stopped part-way does the whole backfill again
    added = [
        v for v in config["active_versions"] if v not in applied["active_versions"]
    ]
    switched = config["query_version"] != applied["query_version"]
    for version in added:
        index.apply_version(version, keys[version])
    index.set_query_version(config["query_version"], config["peer_versions"])
    for version in set(index.active_versions) - set(config["active_versions"]):
        index.retire_version(version)
    if added or switched:  # retiring alone has nothing to write
        backfill(scorer, added)
    return config


def backfill(scorer: Scorer, versions: list[str]) -> None:
    """Write tok: for new versions, and point pay: at the query version's token."""
    index, state = scorer.token_index, scorer.state
    for n, (payment_id, direction, entry) in enumerate(state.payments(), 1):
        if entry["counterparty_bank"] == scorer.bank:
            continue  # intra-bank: no token
        pair = canonical_pair(scorer.bank, entry["counterparty_bank"])
        ref = {"payment_id": payment_id, "direction": direction}
        ref |= {"account": entry["account"], "amount": entry["amount"]}
        for version in versions:
            token = index.token_for(payment_id, pair, version).hex()
            state.put_token("-".join(pair), version, token, ref)
        token = index.token_for(payment_id, pair, index.key_version).hex()
        if entry.get("token") != token:
            state.put_payment(payment_id, direction, entry | {"token": token})
        if n % 1000 == 0:
            state.commit()  # keep each MULTI/EXEC small
    state.commit()


def resume_rotation(scorer: Scorer, applied: dict) -> None:
    """A pod that restarts mid-rotation may have stopped during a backfill: redo it.

    Only while more than one version is active, so a normal start costs nothing.
    """
    if len(applied.get("active_versions", ())) > 1:
        backfill(scorer, applied["active_versions"])


def settings(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bank", nargs="?", choices=("A", "B", "C"), default=env.get("SCORER_BANK")
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--group",
        default=env.get("KAFKA_GROUP"),
        help="default: scorer-<bank>; change it only to rebuild Valkey",
    )
    parser.add_argument(
        "--valkey-url", default=env.get("VALKEY_URL", "valkey://localhost:6379")
    )
    parser.add_argument(
        "--key-dir",
        help="default: KEY_DIR. X25519 files and <bank>/active.json; "
        "without keys no tokens are written",
    )
    # one key version from explicit files (the chart uses KEY_DIR)
    parser.add_argument("--key-version")
    parser.add_argument("--x25519-private", help="this bank's X25519 private PEM")
    parser.add_argument(
        "--peer-public-key",
        nargs=2,
        action="append",
        default=[],
        metavar=("PEER", "PATH"),
        help="trusted X25519 peer public PEM; repeat for each peer",
    )
    args = parser.parse_args(argv)
    if not args.bank:
        parser.error("give the bank as an argument or SCORER_BANK")
    files = (args.key_version, args.x25519_private, args.peer_public_key)
    if any(files) and not all(files):
        parser.error(
            "--key-version, --x25519-private and --peer-public-key go together"
        )
    if args.key_dir and any(files):
        parser.error("use --key-dir or the key file flags, not both")
    if not any(files):  # key file flags override KEY_DIR, like every other setting
        args.key_dir = args.key_dir or env.get("KEY_DIR")
    args.group = args.group or f"scorer-{args.bank}"
    return args


def main() -> None:
    args = settings()
    token_index, reload, applied = None, None, {}
    if args.key_dir:
        token_index, applied = key_index(args.bank, args.key_dir)

        def reload() -> None:
            nonlocal applied
            applied = reload_keys(scorer, args.key_dir, applied)

    elif args.key_version:
        token_index = TokenIndex(
            args.bank,
            args.key_version,
            AgreementFiles(args.x25519_private, dict(args.peer_public_key)),
        )
    db = Valkey.from_url(args.valkey_url, decode_responses=True)
    scorer = Scorer(args.bank, state=ValkeyState(db), token_index=token_index)
    resume_rotation(scorer, applied)
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,  # store_offsets() after the Valkey write
            "queued.max.messages.kbytes": 16384,  # 16 MiB prefetch, not the 64 MiB default
        }
    )
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    try:
        serve(consumer, scorer, stop, reload=reload)
    except Exception as error:
        log(sys.stdout, args.bank, level="error", error=repr(error))
        raise


if __name__ == "__main__":
    main()
