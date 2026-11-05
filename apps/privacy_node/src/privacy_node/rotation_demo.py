"""Replay k1, dual-index fresh k2, switch and retire without losing a link.

  uv run python -m privacy_node.rotation_demo

Uses temporary local keys and the explicitly insecure localhost demo transport.
Use the separate mTLS tests and k3d procedure for authenticated transport evidence.
"""

import csv
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from privacy_node import privacy_node_pb2 as pb
from privacy_node.demo import TINY, prepare_demo_keys, start_banks
from privacy_node.keys import pair_paths, public_keys


def main():
    with tempfile.TemporaryDirectory() as key_dir:
        nodes, servers = start_banks(TINY, key_dir)
        try:
            result = nodes["B"].search("p06", "p07")
            assert result.state == pb.CONFIRMED
            print("k1 replay: p06 -> p07 CONFIRMED")
            prepare_demo_keys(key_dir, "k2")
            for bank, node in nodes.items():
                node.local.index.introduce_version(
                    "k2", pair_paths(key_dir, bank, "k2")
                )
                node.public_keys = public_keys(key_dir, bank)
            for bank, node in nodes.items():
                with (Path(TINY) / f"bank_{bank}.csv").open() as source:
                    for event in csv.DictReader(source):
                        event["payment_id"] += "-overlap"
                        event["account"] += "-overlap"
                        event["event_time"] = (
                            datetime.fromisoformat(event["event_time"])
                            + timedelta(days=1)
                        ).isoformat()
                        node.local.process(event)
            index = nodes["B"].local.index
            for version in ("k1", "k2"):
                token = index.token_for("p06-overlap", ("A", "B"), version)
                assert isinstance(token, bytes) and len(token) == 16
                assert index.lookup(("A", "B"), version, token)
                print(
                    f"overlap {version}: {len(token)} bytes, display hex={token.hex()}"
                )
            overlap = nodes["B"].search("p06-overlap", "p07-overlap")
            assert overlap.state == pb.CONFIRMED
            ready = {bank: ("k1", "k2") for bank in nodes}
            for node in nodes.values():
                node.set_query_version("k2", ready)
            assert nodes["B"].search("p06", "p07").state == pb.CONFIRMED
            print("k2 backfill: pre-rotation p06 -> p07 also CONFIRMED")
            rotated = nodes["B"].search("p06-overlap", "p07-overlap")
            assert rotated.state == pb.CONFIRMED
            assert [c.signer for c in rotated.claims] == [
                c.signer for c in overlap.claims
            ]
            print("k2 query: same A -> B -> C link CONFIRMED")
            for node in nodes.values():
                node.local.index.retire_version("k1")
            assert nodes["B"].search("p06-overlap", "p07-overlap").state == pb.CONFIRMED
            print(
                "k1 retired after demo work drained: k2 still CONFIRMED; no missed link"
            )
        finally:
            for node in nodes.values():
                node.close()
            for server in servers.values():
                server.stop(0)


if __name__ == "__main__":
    main()
