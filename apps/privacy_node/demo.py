"""Three privacy nodes on localhost, tracing every local detection in a workload.

  uv run python -m apps.privacy_node.demo [apps/dataset/fixtures/tiny]

Prints each search's state and each bank's stored chains. Keys are generated
into a temporary folder for the run.
"""

import sys
import tempfile

from apps.privacy_node import privacy_node_pb2 as pb
from apps.privacy_node.keys import BANKS, make_keys, pair_paths
from apps.privacy_node.local_state import LocalBank
from apps.privacy_node.node import Node, serve
from apps.privacy_node.store import Store
from apps.security.security import load_signing_key, load_verification_key

TINY = "apps/dataset/fixtures/tiny"


def start_banks(workload, key_dir):
    """Start one node per bank on free ports; returns ({bank: node}, {bank: server})."""
    make_keys(key_dir)
    public = {b: load_verification_key(f"{key_dir}/public/{b}.pem") for b in BANKS}
    nodes, servers, ports = {}, {}, {}
    for bank in BANKS:
        local = LocalBank.from_workload(workload, bank, "k1", pair_paths(key_dir, bank))
        signing = load_signing_key(f"{key_dir}/signing/{bank}.pem")
        nodes[bank] = Node(local, Store(), signing, public, {}, "k1")
        servers[bank], ports[bank] = serve(nodes[bank])
    for bank, node in nodes.items():
        node.peers.update({b: f"localhost:{p}" for b, p in ports.items() if b != bank})
    return nodes, servers


def main():
    workload = sys.argv[1] if len(sys.argv) > 1 else TINY
    with tempfile.TemporaryDirectory() as key_dir:
        nodes, servers = start_banks(workload, key_dir)
        for bank, node in nodes.items():
            for d, r in zip(node.local.detections, node.search_detections()):
                route = " -> ".join(f"{c.signer}" for c in r.claims)
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
