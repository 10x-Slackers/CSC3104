"""Pipe replay into `stream.py ingest B`; run `stream.py consume B` separately.

Both accept --bootstrap-servers (default localhost:9092). Consumers use one
bank-specific group; use a fresh --group to replay history after a restart.
Output is JSONL local candidate links, not confirmed cross-bank alerts.
"""

import argparse
import json
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer

if __package__:
    from .baseline import make_scorer, thresholds
    from .score import validate
else:
    from baseline import make_scorer, thresholds
    from score import validate


def topic(bank):
    return f"bank-{bank}-transactions"


def ingest(lines, bank, producer):
    errors = []

    def delivered(error, message):
        if error:
            errors.append(str(error))

    try:
        for line in lines:
            event = validate(json.loads(line), bank)
            deadline = time.monotonic() + 30
            while True:
                if errors:
                    raise RuntimeError(errors[0])
                try:
                    producer.produce(
                        topic(bank),
                        key=event["account"],
                        value=json.dumps(event),
                        on_delivery=delivered,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            "Kafka producer queue remained full for 30 seconds"
                        )
            producer.poll(0)
            if errors:
                raise RuntimeError(errors[0])
    finally:
        remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"Kafka delivery failed: {remaining} pending, {errors}")


def consume(consumer, scorer, output=sys.stdout, token_index=None):
    try:
        if token_index is not None and token_index.bank != scorer.bank:
            raise ValueError("Token index belongs to another bank")
        consumer.subscribe([topic(scorer.bank)])
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            # Fail without committing malformed or wrongly routed records.
            event = validate(json.loads(message.value()), scorer.bank)
            if (
                message.topic() != topic(scorer.bank)
                or message.key() != event["account"].encode()
            ):
                raise ValueError("wrong topic or account key")
            if token_index is not None:
                token_index.add(event)
            for result in scorer.process(event):
                print(json.dumps(result, sort_keys=True), file=output)
            output.flush()
            committed = consumer.commit(message=message, asynchronous=False)
            for partition in committed:
                if partition.error:
                    raise KafkaException(partition.error)
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("ingest", "consume"))
    thresholds(parser)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--group", help="default: scorer-<bank>")
    parser.add_argument("--key-version", help="enable token indexing with this version")
    parser.add_argument(
        "--pairwise-key",
        nargs=2,
        action="append",
        default=[],
        metavar=("PEER", "PATH"),
        help="raw pairwise key file; repeat for each peer",
    )
    args = parser.parse_args()
    token_index = None
    if args.key_version is not None or args.pairwise_key:
        if args.mode != "consume" or not args.key_version or not args.pairwise_key:
            parser.error("Indexing requires consume, --key-version and --pairwise-key")
        # Direct script execution puts apps/scorer, rather than the repo root, on sys.path.
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
        from apps.scorer.token_index import TokenIndex

        paths = {}
        for peer, path in args.pairwise_key:
            pair = (args.bank, peer)
            if pair in paths:
                parser.error("Duplicate peer key configuration")
            paths[pair] = path
        token_index = TokenIndex(args.bank, args.key_version, paths)
    config = {"bootstrap.servers": args.bootstrap_servers}
    if args.mode == "ingest":
        ingest(sys.stdin, args.bank, Producer(config | {"enable.idempotence": True}))
    else:
        scorer = make_scorer(args)
        consumer = Consumer(
            config
            | {
                "group.id": args.group or f"scorer-{args.bank}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        try:
            consume(consumer, scorer, token_index=token_index)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
