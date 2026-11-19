"""Checks against a running cluster, k3d or EKS:

    make replay WORKLOAD=tiny && make cluster-check

They test what the charts promise, from inside the pods:

- End to end: the tiny workload gives every bank the cases the offline evaluator finds.
- NetworkPolicy: each path a service needs is open, and everything else is closed.
  Every "blocked" target is also shown open from a pod that may reach it, so a
  missing service can't pass as a blocked one.
- mTLS: which callers a privacy node accepts, what a caller may ask, and that a
  renewed certificate is served without a restart. (This renews bank A's certificate.)
"""

import base64
import json
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from cryptography import x509

from mulechain import evaluate
from mulechain.node.node import identity

BANKS = ("a", "b", "c")
OPEN, BLOCKED = "open", "blocked"


def kubectl(*args: str, stdin: str | None = None) -> str:
    done = subprocess.run(
        ["kubectl", *args], input=stdin, capture_output=True, text=True, check=False
    )
    return done.stdout.strip()


def python_in(namespace: str, pod: str, code: str, *args: str) -> str:
    """Run Python inside a running pod: every service image has it."""
    return kubectl("exec", "-n", namespace, pod, "--", "python", "-c", code, *args)


# On AWS a bank has no Valkey, ingestor or summary pods: ElastiCache and Lambda do that.
ON_EKS = not kubectl("get", "service", "valkey", "-n", "bank-a", "--ignore-not-found")

# ---- end to end


def test_the_tiny_workload_gives_every_bank_the_cases_the_offline_evaluator_finds():
    offline, _ = evaluate.run(Path(__file__).parents[1] / "workloads/tiny")
    expected = sorted((c.bank, c.tier, " ".join(sorted(c.ids))) for c in offline)
    fetch = "import urllib.request as u; print(u.urlopen('http://localhost:8000/api/cases').read().decode())"
    found = []
    for bank in BANKS:
        cases = json.loads(python_in(f"bank-{bank}", "deploy/case", fetch))
        found += [(bank.upper(), case["tier"], " ".join(case["payments"])) for case in cases]
    assert sorted(found) == expected


# ---- NetworkPolicy

CONNECT = """import socket, sys
try: socket.create_connection((sys.argv[1], int(sys.argv[2])), 3); print("open")
except OSError: print("blocked")"""
RESTRICTED = {  # what the bank namespaces' Pod Security Standard demands of a pod
    "runAsNonRoot": True,
    "runAsUser": 65532,
    "allowPrivilegeEscalation": False,
    "capabilities": {"drop": ["ALL"]},
    "seccompProfile": {"type": "RuntimeDefault"},
}


def reach(source: str, target: str) -> str:
    """Whether `target` (host:port) can be reached from `source`: "open" or "blocked".

    `source` is "<namespace>/<deployment>" or "<namespace>/pod/<name>" for a running
    pod, or just "<namespace>" for a new pod with no labels.
    """
    host, port = target.rsplit(":", 1)
    namespace, _, pod = source.partition("/")
    if pod:
        return python_in(namespace, pod if "/" in pod else f"deploy/{pod}", CONNECT, host, port)
    patch = [{"op": "add", "path": "/spec/containers/0/securityContext", "value": RESTRICTED}]
    # A new pod's policy takes a few seconds to apply, so wait before trying.
    script = f"sleep 10; nc -z -w 3 {host} {port} && echo open || echo blocked"
    run = ("run", f"probe-{uuid.uuid4().hex[:8]}", "-n", namespace, "--rm", "-i", "--quiet")
    run += ("--restart=Never", "--image=busybox:1.37", "--override-type=json")
    return kubectl(*run, f"--overrides={json.dumps(patch)}", "--", "sh", "-c", script)


def pod_ips(namespace: str, selector: str) -> list[tuple[str, str]]:
    """(name, IP) of the pods a label selector matches."""
    path = '{range .items[*]}{.metadata.name} {.status.podIP}{"\\n"}{end}'
    listed = kubectl("get", "pod", "-n", namespace, "-l", selector, "-o", f"jsonpath={path}")
    return [tuple(line.split()) for line in listed.splitlines()]


def paths() -> list[tuple[str, str, str]]:
    kafka, postgres = "kafka-kafka-bootstrap.bank-{}:9092", "postgres-rw.bank-{}:5432"
    postgres_a = pod_ips("bank-a", "cnpg.io/cluster=postgres,role=primary")[0][1]
    (node_1, _), (_, node_2_ip) = pod_ips("bank-a", "app=privacy-node")[:2]  # bank A runs two
    listed = [
        # The Kubernetes API is open to bank pods; other HTTPS services and the internet are not.
        ("bank-a", "kubernetes.default:443", OPEN),
        ("default", "keda-operator-metrics-apiserver.keda:443", OPEN),
        ("bank-a", "keda-operator-metrics-apiserver.keda:443", BLOCKED),
        ("default", "1.1.1.1:443", OPEN),
        ("bank-a", "1.1.1.1:443", BLOCKED),
        # Kafka: its own bank's scorer (and ingestor), KEDA and the Strimzi operator.
        ("bank-a/scorer", kafka.format("a"), OPEN),
        ("bank-c/scorer", kafka.format("c"), OPEN),
        ("keda", kafka.format("a"), OPEN),
        ("strimzi", "kafka-kafka-bootstrap.bank-a:9091", OPEN),
        ("bank-a", kafka.format("a"), BLOCKED),
        ("bank-a/privacy-node", kafka.format("a"), BLOCKED),
        ("bank-b/scorer", kafka.format("c"), BLOCKED),
        ("default", kafka.format("a"), BLOCKED),
        ("default", "kafka-kafka-bootstrap.bank-a:9091", BLOCKED),
        # PostgreSQL: its own bank's privacy node and case service, and its operator.
        ("bank-a/privacy-node", postgres.format("a"), OPEN),
        ("bank-b/case", postgres.format("b"), OPEN),
        ("cnpg-system", f"{postgres_a}:8000", OPEN),
        ("bank-b", postgres.format("b"), BLOCKED),
        ("bank-a/scorer", postgres.format("a"), BLOCKED),
        ("bank-a/case", postgres.format("b"), BLOCKED),
        ("default", postgres.format("a"), BLOCKED),
        ("default", f"{postgres_a}:8000", BLOCKED),
        # The case service: its own bank's privacy node only.
        ("bank-a/privacy-node", "case.bank-a:8000", OPEN),
        ("bank-a/scorer", "case.bank-a:8000", BLOCKED),
        ("default", "case.bank-a:8000", BLOCKED),
        # Privacy nodes: only each other, only across banks, only on the gRPC port.
        ("bank-a/privacy-node", "privacy-node.bank-b:50051", OPEN),
        ("bank-c/privacy-node", f"{node_2_ip}:50051", OPEN),
        ("bank-a/scorer", "privacy-node.bank-a:50051", BLOCKED),
        (f"bank-a/pod/{node_1}", f"{node_2_ip}:50051", BLOCKED),
        ("bank-a", "privacy-node.bank-b:50051", BLOCKED),
        ("default", "privacy-node.bank-a:50051", BLOCKED),
    ]
    if ON_EKS:
        env = "{.spec.template.spec.containers[0].env[?(@.name=='VALKEY_URL')].value}"
        scorer = kubectl("get", "deploy", "scorer", "-n", "bank-c", "-o", f"jsonpath={env}")
        elasticache_c = scorer.removeprefix("valkey://")
        return listed + [
            # Any pod could take the node's AWS role from its metadata service; only the
            # case service may, to sign its calls to the summary Lambda.
            ("default", "169.254.169.254:80", OPEN),
            ("bank-a/case", "169.254.169.254:80", OPEN),
            ("bank-a", "169.254.169.254:80", BLOCKED),
            ("bank-a/privacy-node", "169.254.169.254:80", BLOCKED),
            ("bank-b/case", "lambda.us-east-1.amazonaws.com:443", OPEN),
            ("bank-b/privacy-node", "lambda.us-east-1.amazonaws.com:443", BLOCKED),
            # ElastiCache: its own bank's scorer and privacy node.
            ("bank-c/scorer", elasticache_c, OPEN),
            ("bank-c/privacy-node", elasticache_c, OPEN),
            ("bank-c/case", elasticache_c, BLOCKED),
            ("bank-a/scorer", elasticache_c, BLOCKED),
        ]
    return listed + [
        # Valkey: its own bank's scorer and privacy node. Summary: its case service.
        ("bank-a/scorer", "valkey.bank-a:6379", OPEN),
        ("bank-a/privacy-node", "valkey.bank-a:6379", OPEN),
        ("bank-a/case", "valkey.bank-a:6379", BLOCKED),
        ("bank-b/scorer", "valkey.bank-a:6379", BLOCKED),
        ("default", "valkey.bank-a:6379", BLOCKED),
        ("bank-a/case", "summary.bank-a:8080", OPEN),
        ("bank-a/privacy-node", "summary.bank-a:8080", BLOCKED),
        ("default", "summary.bank-a:8080", BLOCKED),
        # The ingestor: only the replay Job reaches it, and it reaches only Kafka.
        ("replay", "ingestor.bank-a:8080", OPEN),
        ("replay", "ingestor.bank-c:8080", OPEN),
        ("bank-b/ingestor", kafka.format("b"), OPEN),
        ("bank-a/scorer", "ingestor.bank-a:8080", BLOCKED),
        ("replay", kafka.format("a"), BLOCKED),
        ("default", "ingestor.bank-a:8080", BLOCKED),
    ]


PATHS = paths()


@pytest.fixture(scope="session")
def reached() -> dict:
    """Every path tried once, side by side (each new pod waits ten seconds)."""
    with ThreadPoolExecutor(max_workers=len(PATHS)) as pool:
        return dict(zip(PATHS, pool.map(lambda path: reach(*path[:2]), PATHS), strict=True))


@pytest.mark.parametrize("path", PATHS, ids=lambda p: f"{p[0]} -> {p[1]} is {p[2]}")
def test_network_path(reached, path):
    assert reached[path] == path[2]


# ---- mTLS between privacy nodes

TRACE = """import sys, grpc
from mulechain.node import protocol_pb2 as pb, protocol_pb2_grpc as rpc
target, token, pem = sys.argv[1:]
read = lambda name: open("/tls/" + name, "rb").read()
key, certificate = {"own": (read("tls.key"), read("tls.crt")), "none": (None, None)}.get(pem, (pem.encode(),) * 2)
channel = grpc.secure_channel(target, grpc.ssl_channel_credentials(read("ca.crt"), key, certificate))
request = pb.TraceRequest(token=bytes.fromhex(token), direction=pb.DOWNSTREAM, hops_remaining=1)
try: print("OK", len(rpc.PrivacyNodeStub(channel).Trace(request, timeout=5).claims))
except grpc.RpcError as error: print(error.code().name)"""
SERVED_SERIAL = """import socket, ssl, sys
context = ssl.create_default_context(cafile="/tls/ca.crt")
context.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
context.set_alpn_protocols(["h2"])
with context.wrap_socket(socket.create_connection((sys.argv[1], 50051), 3), server_hostname=sys.argv[2]) as s:
    print(int(s.getpeercert()["serialNumber"], 16))"""
UNKNOWN = bytes(16).hex()  # a token no bank holds: an accepted caller gets an empty answer


def trace(caller: str, callee: str, token: str = UNKNOWN, pem: str = "own") -> str:
    """Call a bank's node from another bank's node pod: "OK <claims>" or a gRPC status.

    `pem` is the client key and certificate to present: the pod's own, "none", or PEM text.
    """
    target = f"privacy-node.bank-{callee}.svc:50051"
    return python_in(f"bank-{caller}", "deploy/privacy-node", TRACE, target, token, pem)


def secret(namespace: str, name: str, key: str) -> bytes:
    data = kubectl("get", "secret", "-n", namespace, name, "-o", "jsonpath={.data}")
    return base64.b64decode(json.loads(data)[key])


def certificate(bank: str) -> x509.Certificate:
    return x509.load_pem_x509_certificate(secret(f"bank-{bank}", "privacy-node-tls", "tls.crt"))


@pytest.mark.parametrize("bank", BANKS)
def test_each_bank_has_a_certificate_from_the_lab_ca_naming_it(bank):
    issued = certificate(bank)
    names = issued.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert names.get_values_for_type(x509.DNSName) == [f"privacy-node.bank-{bank}.svc"]
    assert names.get_values_for_type(x509.UniformResourceIdentifier) == [identity(bank.upper())]
    authority = x509.load_pem_x509_certificate(secret("cert-manager", "bank-ca", "tls.crt"))
    issued.verify_directly_issued_by(authority)


@pytest.mark.parametrize(("caller", "callee"), [("a", "b"), ("b", "c"), ("c", "a")])
def test_a_node_accepts_another_banks_node(caller, callee):
    assert trace(caller, callee) == "OK 0"


def test_a_caller_with_no_certificate_cannot_connect():
    assert trace("a", "b", pem="none") == "UNAVAILABLE"


def test_a_self_signed_certificate_naming_bank_a_cannot_connect():
    name = f"subjectAltName=URI:{identity('A')}"
    make = ["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256"]
    make += ["-nodes", "-subj", "/CN=impostor", "-days", "1", "-addext", name]
    impostor = subprocess.run(
        [*make, "-keyout", "-", "-out", "-"], capture_output=True, text=True, check=False
    )
    assert "PRIVATE KEY" in impostor.stdout
    assert trace("a", "b", pem=impostor.stdout) == "UNAVAILABLE"


def test_a_lab_certificate_that_names_no_bank_is_unauthenticated():
    """Signed by the right CA is not enough: the certificate must say which bank it is."""
    manifest = {
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {"name": "no-bank", "namespace": "bank-a"},
        "spec": {
            "secretName": "no-bank",
            "dnsNames": ["privacy-node.bank-a.svc"],
            "usages": ["server auth", "client auth"],
            "issuerRef": {"name": "bank-ca", "kind": "ClusterIssuer"},
        },
    }
    kubectl("apply", "-f", "-", stdin=json.dumps(manifest))
    try:
        kubectl(
            "wait", "-n", "bank-a", "certificate/no-bank", "--for=condition=Ready", "--timeout=60s"
        )
        pem = secret("bank-a", "no-bank", "tls.key") + secret("bank-a", "no-bank", "tls.crt")
        assert trace("a", "b", pem=pem.decode()) == "UNAUTHENTICATED"
    finally:
        kubectl("delete", "-n", "bank-a", "certificate/no-bank", "secret/no-bank")


def test_a_node_answers_only_the_bank_on_the_other_side_of_a_transfer():
    """In the tiny workload p06 went from A to B. A may ask B about it; C may not,
    even though it presents a valid certificate and knows the token."""
    read = "import os, valkey; print(valkey.Valkey.from_url(os.environ['VALKEY_URL'], decode_responses=True).hget('pay:p06:out', 'token'))"
    token = python_in("bank-a", "deploy/privacy-node", read)
    assert len(token) == 32, "replay the tiny workload first: make replay WORKLOAD=tiny"
    assert trace("a", "b", token) not in ("OK 0", "UNAVAILABLE")
    assert trace("c", "b", token) == "OK 0"


def served_serial(ip: str) -> int | None:
    """The serial number of the certificate a bank A node pod serves, seen from bank C."""
    args = (SERVED_SERIAL, ip, "privacy-node.bank-a.svc")
    answer = python_in("bank-c", "deploy/privacy-node", *args)
    return int(answer) if answer.isdigit() else None


def changed(read, old: int) -> int:
    """Wait up to three minutes for `read()` to give something other than `old`."""
    for _ in range(60):
        if (got := read()) not in (old, None):
            return got
        time.sleep(3)
    return old


def test_a_renewed_certificate_is_served_without_a_restart():
    pods = "{range .items[*]}{.metadata.uid} {.status.containerStatuses[0].restartCount} {end}"
    running = ("get", "pod", "-n", "bank-a", "-l", "app=privacy-node", "-o", f"jsonpath={pods}")
    before, old = kubectl(*running), certificate("a").serial_number
    # What `cmctl renew` does: mark the certificate as being issued again.
    issuing = {
        "type": "Issuing",
        "status": "True",
        "reason": "ManuallyTriggered",
        "message": "test_cluster.py",
        "lastTransitionTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    patch = [{"op": "add", "path": "/status/conditions/-", "value": issuing}]
    renew = ("patch", "certificate", "privacy-node", "-n", "bank-a", "--subresource=status")
    kubectl(*renew, "--type=json", "-p", json.dumps(patch))
    new = changed(lambda: certificate("a").serial_number, old)
    assert new != old, "cert-manager issued no new certificate"
    # The kubelet updates the mounted Secret within a minute; the node reads it per connection.
    for _, ip in pod_ips("bank-a", "app=privacy-node"):
        assert changed(lambda ip=ip: served_serial(ip), old) == new
    assert trace("a", "b") == "OK 0" and trace("c", "a") == "OK 0"
    assert kubectl(*running) == before  # the same pods, never restarted
