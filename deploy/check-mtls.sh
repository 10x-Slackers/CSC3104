#!/usr/bin/env bash
# Checks privacy-node mTLS on a running cluster (security.md § mTLS): issued certificates and mounts, which peers a
# node accepts, and renewal without a restart. Run after k3d-up.sh or eks-up.sh. Each rejected call has a passing
# twin from the real certificate. Renews bank A's certificate; probe calls leave NO_MATCH replies under query ids
# "mtls-check-*" in the banks' protocol stores.
set -uo pipefail

check() { # <label> <expected>, using $got
  [ "$got" = "$2" ] && r=PASS || { r=FAIL; failed=1; }
  echo "$r  $1: ${got:-probe failed} (expected $2)"
}

# Trace from a bank's privacy-node pod to another bank's node, with client key and certificate in one PEM:
# empty for the pod's own /tls, "none" for no client certificate. A pair without the sender makes the node answer NO_MATCH.
TRACE='import sys, uuid, grpc
from privacy_node import privacy_node_pb2 as pb, privacy_node_pb2_grpc as rpc
target, sender, pem = sys.argv[1:]
pem = None if pem == "none" else pem.encode() or open("/tls/tls.key", "rb").read() + open("/tls/tls.crt", "rb").read()
channel = grpc.secure_channel(target, grpc.ssl_channel_credentials(open("/tls/ca.crt", "rb").read(), pem, pem))
request = pb.TraceRequest(query_id=f"mtls-check-{uuid.uuid4()}", sender=sender, key_version="k1",
    ref=pb.TokenRef(pair="X-Y", token=bytes(16)))
try: print("OK", pb.State.Name(rpc.PrivacyNodeStub(channel).Trace(request, timeout=5).state))
except grpc.RpcError as error: print(error.code().name)'

trace() { # <label> <from bank> <to bank> <sender> <expect> [client PEM]
  got=$(kubectl exec -n "bank-$2" deploy/privacy-node -- python -c "$TRACE" "privacy-node.bank-$3.svc:50051" "$4" \
    "${6:-}" 2>/dev/null)
  check "$1: bank-$2 -> bank-$3 as sender $4" "$5"
}

ring() { # every node calls the next bank's node with its own certificate, then claiming to be the bank it calls
  for pair in "a b" "b c" "c a"; do
    read -r from to <<<"$pair"
    trace "own certificate${1:-}" "$from" "$to" "${from^^}" "OK NO_MATCH"
    [ -z "${1:-}" ] && trace "wrong-bank sender" "$from" "$to" "${to^^}" PERMISSION_DENIED
  done
}

# Serial of the certificate a node presents, seen from bank C's node over a real mTLS handshake.
SERIAL='import socket, ssl, sys
ctx = ssl.create_default_context(cafile="/tls/ca.crt")
ctx.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
ctx.set_alpn_protocols(["h2"])
with ctx.wrap_socket(socket.create_connection((sys.argv[1], 50051), 3), server_hostname=sys.argv[2]) as s:
    print(s.getpeercert()["serialNumber"])'

secret() { # <namespace> [secret] [key]
  kubectl get secret -n "$1" "${2:-privacy-node-tls}" -o go-template="{{index .data \"${3:-tls.crt}\" | base64decode}}"; }
serial() { secret "bank-$1" | openssl x509 -noout -serial | cut -d= -f2; }
pods() { kubectl get pod -n "bank-$1" -l app=privacy-node \
  -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.uid} {.status.containerStatuses[0].restartCount} {.status.podIP}{"\n"}{end}'; }

failed=0
echo "== Issued certificates and mounts"
for b in a b c; do
  got=$(kubectl get certificate -n "bank-$b" privacy-node -o jsonpath='{.spec.issuerRef.kind}/{.spec.issuerRef.name} {.status.conditions[?(@.type=="Ready")].status}')
  check "bank-$b Certificate issuer, Ready" "ClusterIssuer/bank-ca True"
  crt=$(secret "bank-$b")
  got=$(openssl x509 -noout -ext subjectAltName <<<"$crt" | tail -1 | xargs)
  check "bank-$b SANs" "DNS:privacy-node.bank-$b.svc, URI:spiffe://csc3104/bank/${b^^}/privacy-node"
  got=$(openssl x509 -noout -ext extendedKeyUsage <<<"$crt" | tail -1 | xargs)
  check "bank-$b usages" "TLS Web Server Authentication, TLS Web Client Authentication"
  got=$(openssl verify -CAfile <(secret cert-manager bank-ca) <(echo "$crt") 2>&1 | sed 's/.*: //')
  check "bank-$b certificate verifies against the lab CA (cert-manager/bank-ca)" "OK"
  got=$(kubectl get deploy -n "bank-$b" privacy-node -o jsonpath='{.spec.template.spec.containers[0].volumeMounts[?(@.name=="tls")]}')
  check "bank-$b /tls mount" '{"mountPath":"/tls","name":"tls"}'
  while read -r pod _; do
    got=$(kubectl exec -n "bank-$b" "$pod" -- sh -c 'ls /tls | xargs; touch /tls/x 2>/dev/null && echo writable' 2>/dev/null)
    check "bank-$b $pod /tls files, read-only" "ca.crt tls.crt tls.key"
  done < <(pods "$b")
done

echo "== Peer calls"
ring
trace "no client certificate" a b A UNAVAILABLE none
trace "self-signed certificate with bank A's URI SAN" a b A UNAVAILABLE "$(openssl req -x509 -newkey ec \
  -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=impostor -days 1 \
  -addext subjectAltName=URI:spiffe://csc3104/bank/A/privacy-node -keyout /dev/stdout -out /dev/stdout 2>/dev/null)"
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
trace "lab-CA certificate without a bank URI SAN" a b A UNAUTHENTICATED \
  "$(secret bank-a mtls-check-no-uri tls.key; secret bank-a mtls-check-no-uri)"
kubectl delete -n bank-a certificate/mtls-check-no-uri secret/mtls-check-no-uri >/dev/null

echo "== Renewal without restart (bank A: both its pods serve it)"
before=$(pods a) old=$(serial a)
kubectl patch certificate privacy-node -n bank-a --subresource=status --type=json -p "[{\"op\": \"add\", \
  \"path\": \"/status/conditions/-\", \"value\": {\"type\": \"Issuing\", \"status\": \"True\", \
  \"reason\": \"ManuallyTriggered\", \"message\": \"check-mtls.sh\", \"lastTransitionTime\": \"$(date -u +%FT%TZ)\"}}]" \
  >/dev/null  # what cmctl renew does: cert-manager issues a new key and certificate into the same Secret
for _ in $(seq 60); do new=$(serial a); [ -n "$new" ] && [ "$new" != "$old" ] && break; sleep 2; done
got=$([ -n "$new" ] && [ "$new" != "$old" ] && echo renewed)
check "bank-a certificate $old -> $new" renewed
while read -r pod _ _ ip; do # the kubelet refreshes Secret volumes within about a minute
  for _ in $(seq 60); do got=$(kubectl exec -n bank-c deploy/privacy-node -- python -c "$SERIAL" "$ip" \
    privacy-node.bank-a.svc 2>/dev/null); [ "$got" = "$new" ] && break; sleep 3; done
  check "bank-a $pod serves the new certificate" "${new:-a new serial}"
done <<<"$before"
ring " after renewal"
got=$(pods a | xargs)
check "bank-a pods, uids, restart counts unchanged" "$(xargs <<<"$before")"
exit $failed
