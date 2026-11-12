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
from security import AgreementFiles


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
) -> None:
    """Score each event, then store its offset. Stops between events when stop is set."""
    try:
        consumer.subscribe([topic(scorer.bank)])
        while not stop.is_set():
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


def key_index(bank: str, key_dir: str | Path) -> TokenIndex:
    """Token keys for every version in <key_dir>/<bank>/active.json (security.md layout)."""
    root = Path(key_dir) / bank
    active = json.loads((root / "active.json").read_text())

    def files(version: str) -> AgreementFiles:
        trusted = root / "trusted" / "x25519"
        return AgreementFiles(
            root / "private" / "x25519" / f"{version}.pem",
            {
                peer.name: peer / f"{version}.pem"
                for peer in trusted.glob("*")
                if (peer / f"{version}.pem").is_file()
            },
        )

    index = TokenIndex(bank, active["query_version"], files(active["query_version"]))
    for version in active["active_versions"]:
        if version != active["query_version"]:
            index.introduce_version(version, files(version))
    return index


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
        default=env.get("KEY_DIR"),
        help="X25519 files and <bank>/active.json; without keys no tokens are written",
    )
    # one key version from explicit files, as the chart passes them today
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
    args.group = args.group or f"scorer-{args.bank}"
    return args


def main() -> None:
    args = settings()
    token_index = None
    if args.key_dir:
        token_index = key_index(args.bank, args.key_dir)
    elif args.key_version:
        token_index = TokenIndex(
            args.bank,
            args.key_version,
            AgreementFiles(args.x25519_private, dict(args.peer_public_key)),
        )
    db = Valkey.from_url(args.valkey_url, decode_responses=True)
    scorer = Scorer(args.bank, state=ValkeyState(db), token_index=token_index)
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
        serve(consumer, scorer, stop)
    except Exception as error:
        log(sys.stdout, args.bank, level="error", error=repr(error))
        raise


if __name__ == "__main__":
    main()
