#!/usr/bin/env bash
# Checks privacy-node mTLS on a running cluster (security.md § mTLS): issued certificates and mounts, which peers a
# node accepts, and renewal without a restart. Run after k3d-up.sh or eks-up.sh. Each rejected call has a passing
# twin from the real certificate. Renews bank B's and A's certificates; probe calls leave NO_MATCH replies under
# query ids "mtls-check-*" in bank B's and A's protocol stores.
set -uo pipefail

check() { # <label> <expected>, using $got
  [ "$got" = "$2" ] && r=PASS || { r=FAIL; failed=1; }
  echo "$r  $1: ${got:-probe failed} (expected $2)"
}

# Trace from a bank's privacy-node pod to another bank's node. Client key and certificate in PEM on stdin: empty for
# the pod's own /tls, "none" for no client certificate. A pair without the sender makes the node answer NO_MATCH.
TRACE='import re, sys, uuid, grpc
from privacy_node import privacy_node_pb2 as pb, privacy_node_pb2_grpc as rpc
target, sender, name = sys.argv[1:]
pem = sys.stdin.read().encode() or open("/tls/tls.key", "rb").read() + open("/tls/tls.crt", "rb").read()
key = re.search(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", pem, re.S)
creds = grpc.ssl_channel_credentials(open("/tls/ca.crt", "rb").read(), key and key[0], key and pem.replace(key[0], b""))
channel = grpc.secure_channel(target, creds, [("grpc.ssl_target_name_override", name)])
request = pb.TraceRequest(query_id=f"mtls-check-{uuid.uuid4()}", sender=sender, key_version="k1",
    ref=pb.TokenRef(pair="X-Y", token=bytes(16)))
try: print("OK", pb.State.Name(rpc.PrivacyNodeStub(channel).Trace(request, timeout=5).state))
except grpc.RpcError as error: print(error.code().name)'

trace() { # <label> <from bank> <to bank> <sender> <expect> [name the client checks]; stdin: client credentials
  host=privacy-node.bank-$3.svc
  got=$(kubectl exec -i -n "bank-$2" deploy/privacy-node -- python -c "$TRACE" "$host:50051" "$4" \
    "${6:-$host}" 2>/dev/null)
  check "$1: bank-$2 -> bank-$3 as sender $4" "$5"
}

# Serial of the certificate a node presents, seen from bank C's node over a real mTLS handshake.
SERIAL='import socket, ssl, sys
ctx = ssl.create_default_context(cafile="/tls/ca.crt")
ctx.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
ctx.set_alpn_protocols(["h2"])
with ctx.wrap_socket(socket.create_connection((sys.argv[1], 50051), 3), server_hostname=sys.argv[2]) as s:
    print(s.getpeercert()["serialNumber"])'
served() { kubectl exec -n bank-c deploy/privacy-node -- python -c "$SERIAL" "$1" "$2" 2>/dev/null; }

secret() { kubectl get secret -n "bank-$1" "${2:-privacy-node-tls}" -o jsonpath="{.data.${3:-tls\.crt}}" | base64 -d; }
serial() { secret "$1" | openssl x509 -noout -serial | cut -d= -f2; }
pods() { kubectl get pod -n "bank-$1" -l app=privacy-node \
  -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.uid} {.status.containerStatuses[0].restartCount} {.status.podIP}{"\n"}{end}'; }

failed=0
echo "== Issued certificates and mounts"
for b in a b c; do
  B=${b^^}
  got=$(kubectl get certificate -n "bank-$b" privacy-node -o jsonpath='{.spec.issuerRef.kind}/{.spec.issuerRef.name} {.status.conditions[?(@.type=="Ready")].status}')
  check "bank-$b Certificate issuer, Ready" "ClusterIssuer/bank-ca True"
  got=$(secret "$b" | openssl x509 -noout -ext subjectAltName | tail -1 | xargs)
  check "bank-$b SANs" "DNS:privacy-node.bank-$b.svc, URI:spiffe://csc3104/bank/$B/privacy-node"
  got=$(secret "$b" | openssl x509 -noout -ext extendedKeyUsage | tail -1 | xargs)
  check "bank-$b usages" "TLS Web Server Authentication, TLS Web Client Authentication"
  got=$(openssl verify -CAfile <(secret "$b" privacy-node-tls 'ca\.crt') <(secret "$b") 2>&1 | sed 's/.*: //')
  check "bank-$b certificate verifies against the lab CA" "OK"
  got=$(kubectl get deploy -n "bank-$b" privacy-node -o jsonpath='{.spec.template.spec.containers[0].volumeMounts[?(@.name=="tls")]}')
  check "bank-$b /tls mount" '{"mountPath":"/tls","name":"tls"}'
  while read -r pod _; do
    got=$(kubectl exec -n "bank-$b" "$pod" -- sh -c 'ls /tls | xargs; touch /tls/x 2>/dev/null && echo writable' 2>/dev/null)
    check "bank-$b $pod /tls files, read-only" "ca.crt tls.crt tls.key"
  done < <(pods "$b")
done

echo "== Peer calls"
trace "own certificate" a b A "OK NO_MATCH" </dev/null
trace "own certificate" c a C "OK NO_MATCH" </dev/null
trace "wrong-bank sender" a b B PERMISSION_DENIED </dev/null
trace "wrong-bank sender" a b C PERMISSION_DENIED </dev/null
trace "wrong-bank sender" c a B PERMISSION_DENIED </dev/null
trace "no client certificate" a b A UNAVAILABLE <<<none
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=impostor -days 1 \
  -addext subjectAltName=URI:spiffe://csc3104/bank/A/privacy-node -keyout /dev/stdout -out /dev/stdout 2>/dev/null |
  trace "self-signed certificate with bank A's URI SAN" a b A UNAVAILABLE
kubectl apply -n bank-a -f - >/dev/null <<'EOF'
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: mtls-check-no-uri}
spec:
  secretName: mtls-check-no-uri
  dnsNames: [privacy-node.bank-a.svc]
  usages: [server auth, client auth]
  issuerRef: {name: bank-ca, kind: ClusterIssuer}
EOF
kubectl wait -n bank-a certificate/mtls-check-no-uri --for=condition=Ready --timeout=60s >/dev/null
{ secret a mtls-check-no-uri 'tls\.key'; secret a mtls-check-no-uri; } |
  trace "lab-CA certificate without a bank URI SAN" a b A UNAUTHENTICATED
kubectl delete -n bank-a certificate/mtls-check-no-uri >/dev/null
kubectl delete -n bank-a secret/mtls-check-no-uri >/dev/null
trace "client expecting bank C's DNS name" a b A UNAVAILABLE privacy-node.bank-c.svc </dev/null

echo "== Renewal without restart"
for b in b a; do
  pods=$(pods "$b") before=$(pods "$b" | xargs) old=$(serial "$b")
  kubectl delete secret -n "bank-$b" privacy-node-tls >/dev/null  # cert-manager issues a new key and certificate
  for _ in $(seq 60); do new=$(serial "$b" 2>/dev/null); [ -n "$new" ] && [ "$new" != "$old" ] && break; sleep 2; done
  got=$([ -n "$new" ] && [ "$new" != "$old" ] && echo reissued)
  check "bank-$b certificate $old -> ${new:-none}" reissued
  while read -r pod _ _ ip; do  # the kubelet refreshes Secret volumes within about a minute
    for _ in $(seq 60); do
      got=$(served "$ip" "privacy-node.bank-$b.svc"); [ "$got" = "$new" ] && break; sleep 3
    done
    check "bank-$b $pod serves the new certificate" "$new"
  done <<<"$pods"
  trace "after renewal" a b A "OK NO_MATCH" </dev/null
  trace "after renewal" c a C "OK NO_MATCH" </dev/null
  got=$(pods "$b" | xargs)
  check "bank-$b pods, uids, restart counts unchanged" "$before"
done
exit $failed
