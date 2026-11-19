#!/usr/bin/env bash
# Checks that the cluster enforces the bank NetworkPolicies. Run after k3d-up.sh or eks-up.sh.
# Each "blocked" target is first shown open from its own namespace, so a missing service can't pass.
# Probes wait 10 s: k3s and the VPC CNI apply a new pod's egress policy a few seconds after it starts.
set -uo pipefail

probe() { # <namespace> <host:port> <expect: open|blocked>, from a pod the bank namespaces' restricted PSS admits
  got=$(kubectl run "probe-$RANDOM" -n "$1" --rm -i --quiet --restart=Never --image=busybox:1.37 --override-type=json \
    --overrides '[{"op": "add", "path": "/spec/containers/0/securityContext", "value": {"runAsNonRoot": true,
      "runAsUser": 65532, "allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]},
      "seccompProfile": {"type": "RuntimeDefault"}}}]' -- \
    sh -c "sleep 10; nc -z -w 3 ${2%:*} ${2#*:} && echo open || echo blocked" 2>/dev/null)
  check "$1" "$2" "$3"
}

app() { # <bank> <deployment> <host:port> <expect>, from that bank's running pod
  got=$(kubectl exec -n "bank-$1" "deploy/$2" -- python -c "import socket
try: socket.create_connection(('${3%:*}', ${3#*:}), 3); print('open')
except OSError: print('blocked')" 2>/dev/null)
  check "bank-$1 $2" "$3" "$4"
}

check() {
  [ "$got" = "$3" ] && r=PASS || { r=FAIL; failed=1; }
  echo "$r  $1 -> $2: ${got:-probe failed} (expected $3)"
}

failed=0
probe bank-a postgres-rw.bank-a:5432 open
probe bank-a kafka-kafka-bootstrap.bank-a:9092 open
probe bank-a kubernetes.default:443 open
probe default keda-operator-metrics-apiserver.keda:443 open  # API-server rule must not open other 443/6443 pods
probe bank-a keda-operator-metrics-apiserver.keda:443 blocked
probe bank-b postgres-rw.bank-b:5432 open
probe bank-c kafka-kafka-bootstrap.bank-c:9092 open
probe default 1.1.1.1:443 open
probe bank-a postgres-rw.bank-b:5432 blocked
probe bank-b kafka-kafka-bootstrap.bank-c:9092 blocked
probe bank-a 1.1.1.1:443 blocked
probe default postgres-rw.bank-a:5432 blocked
probe default kafka-kafka-bootstrap.bank-a:9092 blocked
# Privacy node peers: only privacy-node pods, only to the other banks' privacy nodes.
app a privacy-node privacy-node.bank-b:50051 open
app c privacy-node privacy-node.bank-a:50051 open
probe bank-a privacy-node.bank-b:50051 blocked
probe default privacy-node.bank-a:50051 blocked
app a privacy-node kafka-kafka-bootstrap.bank-b:9092 blocked
# The privacy node posts to its own bank's case service (the baseline keeps other banks out, probed above).
app a privacy-node case.bank-a:8000 open
if [[ $(kubectl config current-context) == arn:aws:eks:* ]]; then
  # Pods could take LabRole from the node's metadata service; only the case service may, to sign summary calls.
  probe default 169.254.169.254:80 open
  probe bank-a 169.254.169.254:80 blocked
  app a case 169.254.169.254:80 open
  app a privacy-node 169.254.169.254:80 blocked
  # The case service reaches HTTPS outside the VPC (function URLs); other bank pods don't.
  app b case lambda.us-east-1.amazonaws.com:443 open
  app b privacy-node lambda.us-east-1.amazonaws.com:443 blocked
  # The scorer and privacy node reach ElastiCache; the case service doesn't.
  valkey=$(kubectl get deploy scorer -n bank-c -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="VALKEY_URL")].value}')
  valkey=${valkey#valkey://}
  app c scorer "$valkey" open
  app c privacy-node "$valkey" open
  app c case "$valkey" blocked
else
  # k3d. Replay: only to each bank's ingestor HTTP port (on EKS replay calls the ingestor Lambdas instead).
  probe replay ingestor.bank-a:8080 open
  probe replay ingestor.bank-c:8080 open
  probe bank-b ingestor.bank-b:8080 open
  probe replay kafka-kafka-bootstrap.bank-a:9092 blocked
  probe default ingestor.bank-b:8080 blocked
fi
exit $failed
