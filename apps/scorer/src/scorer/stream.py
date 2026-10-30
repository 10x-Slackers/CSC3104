"""Pipe replay into `python -m scorer.stream ingest B`; run `... consume B` separately.

Both accept --bootstrap-servers (default localhost:9092). Consumers use one
bank-specific group; use a fresh --group to replay history after a restart.
Output is JSONL local candidate links, not confirmed cross-bank alerts.
"""

import argparse
import json
import sys
import time

from confluent_kafka import Consumer, KafkaException, Producer

from scorer.baseline import make_scorer, thresholds
from scorer.score import validate
from scorer.token_index import TokenIndex


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
                        ) from None
            producer.poll(0)
            if errors:
                raise RuntimeError(errors[0])
    finally:
        remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"Kafka delivery failed: {remaining} pending, {errors}")


def require_full_replay(consumer, partitions):
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
    consumer,
    scorer,
    output=sys.stdout,
    token_index=None,
    process_event=None,
    stop_event=None,
    require_replay=False,
):
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
            if message.error():
                # librdkafka retries transient errors (e.g. topic not created yet) itself.
                if message.error().fatal():
                    raise KafkaException(message.error())
                print(message.error(), file=sys.stderr)
                continue
            # Fail without committing malformed or wrongly routed records.
            event = validate(json.loads(message.value()), scorer.bank)
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("ingest", "consume"))
    thresholds(parser)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--group", help="default: scorer-<bank>")
    parser.add_argument("--key-version", help="enable token indexing with this version")
    parser.add_argument(
        "--max-token-references",
        type=int,
        help="stop when the index is full; never evict",
    )
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
    if (
        args.key_version is not None
        or args.pairwise_key
        or args.max_token_references is not None
    ):
        if args.mode != "consume" or not args.key_version or not args.pairwise_key:
            parser.error("Indexing requires consume, --key-version and --pairwise-key")
        paths = {}
        for peer, path in args.pairwise_key:
            pair = (args.bank, peer)
            if pair in paths:
                parser.error("Duplicate peer key configuration")
            paths[pair] = path
        token_index = TokenIndex(
            args.bank, args.key_version, paths, args.max_token_references
        )
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
