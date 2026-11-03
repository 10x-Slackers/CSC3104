"""Check actual Helm output; live CNI enforcement is deploy/check-netpol.sh's job."""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[3]


@pytest.fixture(scope="module")
def rendered():
    helm = os.environ.get("HELM") or shutil.which("helm")
    if not helm:
        pytest.skip("Helm required: install Helm or set HELM to its executable path")
    output = subprocess.run(
        [helm, "template", "bank-a", str(ROOT / "deploy/charts/bank"), "-n", "bank-a"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return list(yaml.safe_load_all(output))


def matches(selector, labels):
    return all(
        labels.get(k) == v for k, v in selector.get("matchLabels", {}).items()
    ) and all(
        labels.get(e["key"]) in e["values"]
        for e in selector.get("matchExpressions", [])
    )


def peer_matches(peer, owner_namespace, other):
    namespace, labels = other
    if "ipBlock" in peer:
        return False  # the tested endpoints below are pods, not external CIDRs
    namespace_matches = (
        matches(peer["namespaceSelector"], {"kubernetes.io/metadata.name": namespace})
        if "namespaceSelector" in peer
        else namespace == owner_namespace
    )
    return namespace_matches and matches(peer.get("podSelector", {}), labels)


def allows(policies, direction, own, other, port):
    namespace, labels = own
    selected = [
        p
        for p in policies
        if namespace.startswith("bank-")
        and direction.title() in p["policyTypes"]
        and matches(p["podSelector"], labels)
    ]
    # Kubernetes policies are additive; BOTH source egress and destination ingress apply.
    return not selected or any(
        any(p["port"] == port for p in rule.get("ports", []))
        and any(
            peer_matches(peer, namespace, other)
            for peer in rule.get("to" if direction == "egress" else "from", [])
        )
        for policy in selected
        for rule in policy.get(direction, [])
    )


def pod(namespace, app=None, **labels):
    return namespace, {**labels, **({"app": app} if app else {})}


KAFKA = pod("bank-a", **{"strimzi.io/name": "kafka-kafka"})
POSTGRES = pod("bank-a", **{"cnpg.io/cluster": "postgres"})
ROUTES = [
    (pod("bank-a", "privacy-node"), pod("bank-b", "privacy-node"), 50051, True),
    (pod("bank-a", "scorer"), pod("bank-b", "privacy-node"), 50051, False),
    (pod("default", "privacy-node"), pod("bank-b", "privacy-node"), 50051, False),
]
for target, port, clients in [
    (KAFKA, 9092, ["ingestor", "scorer", "privacy-node"]),
    (POSTGRES, 5432, ["privacy-node", "case"]),
    (pod("bank-a", "valkey"), 6379, ["privacy-node", "scorer"]),
    (pod("bank-a", "case"), 8000, ["privacy-node"]),
    (pod("bank-a", "intake"), 8001, ["case"]),
]:
    ROUTES += [
        (pod("bank-a", source), target, port, source in clients)
        for source in ["ingestor", "scorer", "privacy-node", "case", "unknown"]
    ]
    ROUTES += [
        (pod(namespace, clients[0]), target, port, False)
        for namespace in ["bank-b", "default"]
    ]
ROUTES += [
    (pod("strimzi", name="strimzi-cluster-operator"), KAFKA, 9091, True),
    (pod("strimzi", "unknown"), KAFKA, 9091, False),
    (
        pod("cnpg-system", **{"app.kubernetes.io/name": "cloudnative-pg"}),
        POSTGRES,
        8000,
        True,
    ),
    (pod("cnpg-system", "unknown"), POSTGRES, 5432, False),
    (pod("keda", "keda-operator"), KAFKA, 9092, True),
    (pod("keda", "unknown"), KAFKA, 9092, False),
]


@pytest.mark.parametrize("source,destination,port,expected", ROUTES)
def test_network_policy_paths(rendered, source, destination, port, expected):
    policies = [d["spec"] for d in rendered if d["kind"] == "NetworkPolicy"]
    actual = allows(policies, "egress", source, destination, port) and allows(
        policies, "ingress", destination, source, port
    )
    assert actual == expected


def test_bank_certificate_and_default_deny(rendered):
    certificate = next(d["spec"] for d in rendered if d["kind"] == "Certificate")
    assert certificate["uris"] == ["spiffe://csc3104/bank/A/privacy-node"]
    assert certificate["usages"] == ["server auth", "client auth"]
    assert certificate["privateKey"]["rotationPolicy"] == "Always"
    policies = [d for d in rendered if d["kind"] == "NetworkPolicy"]
    baseline = next(
        d["spec"] for d in policies if d["metadata"]["name"] == "bank-baseline"
    )
    assert baseline["podSelector"] == {} and baseline["ingress"] == []
    assert baseline["policyTypes"] == ["Ingress", "Egress"]
    [dns] = baseline["egress"]
    assert dns["to"] == [
        {
            "namespaceSelector": {
                "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
            },
            "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
        }
    ]
    assert {(p["protocol"], p["port"]) for p in dns["ports"]} == {
        ("TCP", 53),
        ("UDP", 53),
    }


def test_deploy_has_no_public_function_url_or_static_aws_keys():
    # No Lambda deployment exists yet. These guards also reject an insecure later manifest.
    for path in (ROOT / "deploy").rglob("*"):
        if path.suffix not in {".yaml", ".yml", ".json", ".tf"}:
            continue
        text = path.read_text()
        assert not re.search(r"\b(?:AuthType|auth_type)\s*[:=]\s*[\"']?NONE\b", text), (
            path
        )
        assert not re.search(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b", text), path
