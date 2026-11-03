"""Three privacy nodes on localhost, tracing every local detection in a workload.

  uv run python -m privacy_node.demo [apps/dataset/fixtures/tiny]

Prints each search's state and each bank's stored chains. Keys are generated
into a temporary folder for the run.
"""

import sys
import tempfile
from pathlib import Path
from shutil import copyfile

from privacy_node import privacy_node_pb2 as pb
from privacy_node.keys import BANKS, make_keys, pair_paths, public_keys
from privacy_node.local_state import LocalBank
from privacy_node.node import Node, serve
from privacy_node.store import Store
from security import load_signing_key

TINY = "apps/dataset/fixtures/tiny"


def prepare_demo_keys(root, version="k1"):
    """Lab only: each bank generates locally, then explicitly publishes public files."""
    root = Path(root)
    for bank in BANKS:
        make_keys(root, bank, version)
    for bank in BANKS:
        for peer in BANKS:
            for purpose in ("x25519", "signing"):
                target = root / bank / "trusted" / purpose / peer / f"{version}.pem"
                target.parent.mkdir(parents=True, exist_ok=True)
                copyfile(root / peer / "public" / purpose / f"{version}.pem", target)


def start_banks(workload, key_dir):
    """Explicitly insecure localhost demo, never the deployment CLI transport."""
    prepare_demo_keys(key_dir)
    nodes, servers, ports = {}, {}, {}
    for bank in BANKS:
        local = LocalBank.from_workload(workload, bank, "k1", pair_paths(key_dir, bank))
        signing = load_signing_key(Path(key_dir) / bank / "private/signing/k1.pem")
        nodes[bank] = Node(
            local, Store(), signing, public_keys(key_dir, bank), {}, "k1"
        )
        servers[bank], ports[bank] = serve(nodes[bank])
    for bank, node in nodes.items():
        node.peers.update({b: f"localhost:{p}" for b, p in ports.items() if b != bank})
    return nodes, servers


def main():
    workload = sys.argv[1] if len(sys.argv) > 1 else TINY
    with tempfile.TemporaryDirectory() as key_dir:
        nodes, servers = start_banks(workload, key_dir)
        for bank, node in nodes.items():
            for d, r in zip(
                node.local.detections, node.search_detections(), strict=True
            ):
                route = " -> ".join(c.signer for c in r.claims)
                print(
                    f"search from {bank} on {d['incoming_payment_id']}->"
                    f"{d['outgoing_payment_id']}: {pb.State.Name(r.state):9} "
                    f"chain {r.chain_digest[:12]} via {route}"
                )
        for bank, node in nodes.items():
            chains = [
                f"{c.chain_digest[:12]} {pb.State.Name(c.state)}"
                for c in node.store.chains()
            ]
            print(f"bank {bank} stores {len(chains)} chains: {', '.join(chains)}")
        for server in servers.values():
            server.stop(0)


if __name__ == "__main__":
    main()
