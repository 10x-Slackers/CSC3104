#!/usr/bin/env bash
# Checks that the cluster enforces the bank NetworkPolicies. Run after k3d-up.sh.
set -uo pipefail

probe() { # <namespace> <host:port> <expect: open|blocked>
  if kubectl run "probe-$RANDOM" -n "$1" --rm -i --quiet --restart=Never --image=busybox:1.37 -- \
    nc -z -w 3 "${2%:*}" "${2#*:}" >/dev/null 2>&1; then got=open; else got=blocked; fi
  [ "$got" = "$3" ] && r=PASS || { r=FAIL; failed=1; }
  echo "$r  $1 -> $2: $got (expected $3)"
}

failed=0
probe bank-a postgres-rw.bank-a:5432 open
probe bank-a kafka-kafka-bootstrap.bank-a:9092 open
probe bank-a postgres-rw.bank-b:5432 blocked
probe bank-b kafka-kafka-bootstrap.bank-c:9092 blocked
probe default postgres-rw.bank-a:5432 blocked
probe default kafka-kafka-bootstrap.bank-a:9092 blocked
exit $failed
