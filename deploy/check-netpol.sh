#!/usr/bin/env bash
# Checks that the cluster enforces the bank NetworkPolicies. Run after k3d-up.sh.
# Each "blocked" target is first shown open from its own namespace, so a missing service can't pass.
# Probes wait 10 s: k3s applies a new pod's egress policy a few seconds after it starts.
set -uo pipefail

probe() { # <namespace> <host:port> <expect: open|blocked>
  got=$(kubectl run "probe-$RANDOM" -n "$1" --rm -i --quiet --restart=Never --image=busybox:1.37 -- \
    sh -c "sleep 10; nc -z -w 3 ${2%:*} ${2#*:} && echo open || echo blocked" 2>/dev/null)
  [ "$got" = "$3" ] && r=PASS || { r=FAIL; failed=1; }
  echo "$r  $1 -> $2: ${got:-probe failed} (expected $3)"
}

failed=0
probe bank-a postgres-rw.bank-a:5432 open
probe bank-a kafka-kafka-bootstrap.bank-a:9092 open
probe bank-a kubernetes.default:443 open
probe bank-b postgres-rw.bank-b:5432 open
probe bank-c kafka-kafka-bootstrap.bank-c:9092 open
probe default 1.1.1.1:443 open
probe bank-a postgres-rw.bank-b:5432 blocked
probe bank-b kafka-kafka-bootstrap.bank-c:9092 blocked
probe bank-a 1.1.1.1:443 blocked
probe default postgres-rw.bank-a:5432 blocked
probe default kafka-kafka-bootstrap.bank-a:9092 blocked
exit $failed
