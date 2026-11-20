# Distributed-systems patterns

Three banks run the same services and must agree on chains of transfers while pods crash, banks go offline and messages arrive twice. These ten patterns make that safe. [overview.md](overview.md) shows how the services fit together.

| Pattern | Problem it solves here | Where |
| --- | --- | --- |
| [Log as source of truth](#1-the-log-is-the-source-of-truth) | Detection state can be lost | [scorer.py](../src/mulechain/scorer.py), [kafka.yaml](../deploy/charts/bank/templates/kafka.yaml) |
| [Partition ownership, autoscaling on lag](#2-partition-ownership-and-autoscaling-on-lag) | Many workers, one account's events in order | [ingestor.py](../src/mulechain/ingestor.py), [scorer.yaml](../deploy/charts/bank/templates/scorer.yaml) |
| [Delivery chain, idempotent receivers](#3-the-delivery-chain-and-idempotent-receivers) | Every hop can repeat a message | every service |
| [Deadline propagation](#4-deadline-propagation) | A search crosses several banks | [node.py](../src/mulechain/node/node.py) |
| [Backoff with jitter, expiry](#5-retry-with-capped-backoff-full-jitter-and-expiry) | A bank is offline for a while | [store.py](../src/mulechain/node/store.py) |
| [Transactional outbox, dead letters](#6-transactional-outbox-and-dead-letters) | A crash between "stored" and "sent" | [store.py](../src/mulechain/node/store.py), [node.py](../src/mulechain/node/node.py) |
| [Advisory locks](#7-advisory-locks) | Two replicas share one database | [db.py](../src/mulechain/db.py) |
| [Key agreement with no dealer](#8-key-agreement-with-no-dealer) | Nobody may hold every bank's keys | [crypto.py](../src/mulechain/crypto.py) |
| [Zero trust](#9-zero-trust) | A peer, or a pod, may be hostile | [node.py](../src/mulechain/node/node.py), [networkpolicy.yaml](../deploy/charts/bank/templates/networkpolicy.yaml) |
| [Bulkhead, graceful degradation](#10-bulkhead-and-graceful-degradation) | One slow part must not stop the rest | [app.py](../src/mulechain/case/app.py) |

The unit tests are in [tests/](../tests) and run with `make check`. The cluster checks are in [test_cluster.py](../deploy/test_cluster.py) and run with `make cluster-check`.

## 1. The log is the source of truth

**Problem.** The scorer keeps its working state in Valkey, which runs in memory with no persistence. If that state is lost, detection must not be.

**Here.** Each bank's Kafka topic keeps every event forever (`retention.ms: -1` in [kafka.yaml](../deploy/charts/bank/templates/kafka.yaml)), and everything in Valkey is derived from it. `Scorer.process` in [scorer.py](../src/mulechain/scorer.py) writes an event's state and the partition's next offset to the `offsets` hash in one Valkey transaction (`MULTI`/`EXEC`: all of the writes happen or none), so the two can't disagree. When a worker is given partitions, `resume` (inside `main`) starts each one at the offset Valkey holds, or at the start of the topic if Valkey holds none. The offsets the client commits to Kafka are used only by KEDA to measure lag.

**Source.** Kreps, [The Log](https://www.linkedin.com/blog/engineering/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) (2013), describes a log as the record from which every other store is derived. Storing a consumer's offset together with the results it produced is the Kafka documentation's own recipe for exactly-once consumption ([KafkaConsumer, "Storing Offsets Outside Kafka"](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)). `MULTI`/`EXEC` is a [Valkey transaction](https://valkey.io/topics/transactions/).

**See it.** `make rebuild BANK=a` empties the bank's Valkey and restarts its scorers. They read the topic from the start, and the bank ends with the same keys and the same cases ([deployment.md](deployment.md)). Deleting the Valkey pod does the same unasked: each worker finds the partition's offset gone before it scores its next event, stops, and starts again from the log (`test_a_worker_stops_before_writing_to_an_emptied_valkey` in [test_scorer.py](../tests/test_scorer.py)).

**Limit.** While Valkey rebuilds, the privacy node answers peers from partial state, so run `make rebuild` while the bank is idle ([detection.md](detection.md#rebuilding-valkey-from-the-log)).

## 2. Partition ownership and autoscaling on lag

**Problem.** The rules compare an account's transfers with each other, so one account's events must be scored in order by one worker at a time, while the bank still needs several workers under load.

**Here.** The ingestor keys every event by its account (`key=event["account"]` in [ingestor.py](../src/mulechain/ingestor.py)), so an account lives in one of the topic's 6 partitions. Kafka's consumer group gives each scorer worker some partitions, and a partition has one owner at a time. Workers share no account state, so they need no locks. KEDA's `ScaledObject` in [scorer.yaml](../deploy/charts/bank/templates/scorer.yaml) adds a worker for every 100 events of consumer lag, from 1 up to 6, one per partition.

**Source.** Kafka writes events with the same key to the same partition and keeps each partition's order ([Kafka introduction](https://kafka.apache.org/intro)). Scaling on lag is KEDA's [Apache Kafka scaler](https://keda.sh/docs/2.21/scalers/apache-kafka/), which by default adds no more workers than the topic has partitions.

**See it.** Run `kubectl get hpa -A` during `make replay WORKLOAD=slice-6h ARGS="--speed 0"` (generate the workload first: [data.md](data.md#workloads)): the scorer deployments grow, then shrink again once the lag is gone.

**Limit.** The parts of one chain are scored by different workers and different banks, a little apart in time. A search that reaches a bank before its side of a transfer does therefore stays pending and is tried again, and "the money stays inside this bank" becomes final only after 60 seconds (`INGESTION_GRACE_SECONDS`) ([protocol.md](protocol.md#search-states)).

## 3. The delivery chain and idempotent receivers

**Problem.** A payment passes through seven hops before it is a case. Any hop can crash after doing its work but before saying so. Nothing may be lost, so every sender repeats until acknowledged, and every receiver must make a repeat harmless (idempotent).

| Hop | The sender keeps trying until | What makes a repeat harmless |
| --- | --- | --- |
| Replay → ingestor | HTTP 200. A 400 stops the replay (the batch is wrong); anything else resends the same batch (`send` in [replay.py](../src/mulechain/replay.py)) | The scorer ignores an event it has already seen |
| Ingestor → Kafka | Every event is acknowledged by all in-sync replicas, then 200 (`handle` in [ingestor.py](../src/mulechain/ingestor.py)) | The idempotent producer: Kafka drops the client's own retries |
| Kafka → scorer → Valkey | The offset moves only with the event's writes, in one transaction | A repeated event stops at the window check or rewrites the same keys |
| Scorer → privacy node (the `hits` stream) | The node's position in the stream is stored in the same PostgreSQL transaction as what it read (`take_hits` in [node.py](../src/mulechain/node/node.py)) | A hit's ID is a hash of bank, rule and payments (`hit_identity` in [scorer.py](../src/mulechain/scorer.py)) |
| Hit → search | A search is a row until it has an outcome | Its ID is a hash of its two payments (`add_search` in [store.py](../src/mulechain/node/store.py)) |
| Search → chain, bank → bank (`Deliver`) | The outbox row is acknowledged | A chain's ID is a hash of its set of signed claims (`digest` in [claims.py](../src/mulechain/node/claims.py)), so every bank gives the same chain the same ID |
| Privacy node → case service | The outbox row is acknowledged | The case store keeps the first message per `(key, kind)` ([case/store.py](../src/mulechain/case/store.py)) |

In the HA bank a write needs two of three brokers (`min.insync.replicas` in [kafka.yaml](../deploy/charts/bank/templates/kafka.yaml)), so losing one broker loses no acknowledged event.

**Source.** The [Idempotent Receiver](https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html) pattern in Hohpe and Woolf, *Enterprise Integration Patterns* (2003). IDs chosen by the sender, so that a retry is recognised, follow the AWS Builders' Library article [Making retries safe with idempotent APIs](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/). The producer setting is librdkafka's [idempotent producer](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_INTRODUCTION.html#idempotent-producer).

**See it.** `test_a_batch_is_acknowledged_only_once_kafka_has_it` ([test_ingestor.py](../tests/test_ingestor.py)), `test_a_replayed_event_changes_nothing` ([test_scorer.py](../tests/test_scorer.py)), `test_a_chain_and_what_it_owes_are_stored_once` ([test_node_store.py](../tests/test_node_store.py)), `test_a_forged_chain_is_refused_and_a_repeated_one_is_harmless` ([test_system.py](../tests/test_system.py)) and `test_the_node_posts_batches_and_a_resent_batch_is_harmless` ([test_case_app.py](../tests/test_case_app.py)).

## 4. Deadline propagation

**Problem.** A search asks bank B, which asks bank C, and so on. If each bank waited its own full timeout, the bank that started the search would give up long before the last one stopped working.

**Here.** The bank that starts a search gives it `SEARCH_SECONDS = 10` in total ([node.py](../src/mulechain/node/node.py)). gRPC carries the time left to the next bank as a duration, so banks need not have matching clocks. Each bank reads it with `time_left` and passes it on in `Trace`, minus `REPLY_MARGIN = 0.25` seconds for its own reply. A bank that runs out of time answers `UNAVAILABLE`, and the starting bank records the search as pending and tries all of it again later. `Deliver` and `Keys` are not part of a search and use a fixed `CALL_SECONDS = 5`.

**Source.** gRPC's guide to [deadlines](https://grpc.io/docs/guides/deadlines/), which describes deadline propagation, and Beyer et al., *Site Reliability Engineering* (2016), ch. 22, [Addressing Cascading Failures](https://sre.google/sre-book/addressing-cascading-failures/#xref_cascading-failure_latency-and-deadlines): every server in a request tree passes on the time that is left.

**See it.** Each privacy node logs one JSON line per search attempt with its state and time: `kubectl logs -n bank-a deploy/privacy-node | grep '"search"'`. Stop a bank's node and the others' lines show `pending` within the deadline.

## 5. Retry with capped backoff, full jitter and expiry

**Problem.** When a bank is offline, the searches and deliveries that need it must wait and try again, without all arriving in the same second when it returns.

**Here.** `backoff` in [store.py](../src/mulechain/node/store.py) waits `random(0, min(300 s, 10 s × 2^attempts))` before the next attempt. The doubling spaces attempts out, the cap keeps the wait under five minutes, and choosing a random point in the whole range (full jitter) spreads the load. Searches and outbox messages both use it. A search older than `EXPIRY` (24 hours) is no longer tried. Replay does the same towards an ingestor, starting at 1 second with a 30-second cap.

**Source.** The formula is "Full Jitter" in Brooker, [Exponential Backoff And Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) (AWS Architecture Blog, 2015), which compares backoff strategies by simulation. The AWS Builders' Library article [Timeouts, retries, and backoff with jitter](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/) explains why retries need backoff, a cap and jitter.

**See it.** `test_backoff_doubles_up_to_the_cap_with_full_jitter`, `test_an_unacknowledged_message_waits_then_is_due_again` and `test_a_pending_search_is_retried_until_it_expires` ([test_node_store.py](../tests/test_node_store.py)).

**Limit.** A case whose search expires or fails keeps showing "Waiting for bank X" ([protocol.md](protocol.md#known-limits)). Outbox messages have no expiry. A chain delivery ends when the peer acknowledges or refuses it; a batch of case messages that the case service can't store is sent again until it can, and holds up the messages behind it.

## 6. Transactional outbox and dead letters

**Problem.** A verified chain must reach every bank on it and this bank's case service. If the node stored the chain and then crashed before sending, the others would never hear of it.

**Here.** `save_chain` in [store.py](../src/mulechain/node/store.py) writes the chain and one `outbox` row per message it owes in a single transaction. `deliver` in [node.py](../src/mulechain/node/node.py) then sends the due rows and marks each one `sent` only when the receiver acknowledges it. A crash at any point leaves the row queued, so the worst case is a message sent twice, which receivers ignore (pattern 3). A peer that answers `INVALID_ARGUMENT` read the chain and refused it, so sending again can't help: the row becomes `dead` (a dead letter) and stays in the table for inspection. Any other failure is retried with backoff.

**Source.** Richardson's [Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) pattern, and the [Dead Letter Channel](https://www.enterpriseintegrationpatterns.com/patterns/messaging/DeadLetterChannel.html) in *Enterprise Integration Patterns*.

**See it.** `test_a_chain_and_what_it_owes_are_stored_once` ([test_node_store.py](../tests/test_node_store.py)) and `test_a_refused_delivery_becomes_a_dead_letter_and_others_are_retried` ([test_system.py](../tests/test_system.py)). In a cluster, `SELECT dest, state, count(*) FROM outbox GROUP BY 1, 2` shows what is still owed.

## 7. Advisory locks

**Problem.** In the HA bank two privacy-node replicas share one PostgreSQL database. Both must answer peers, but only one at a time should read hits, run searches and send the outbox. The case service must never create two cases for one chain.

**Here.** A PostgreSQL advisory lock is a named lock that the database holds for a client (`try_lock` and `transaction(lock=...)` in [db.py](../src/mulechain/db.py)). The services use two, and `db.py` takes a third while it creates the tables, so two pods that start together don't collide:

- `HOUSEKEEPING_LOCK = 3104` ([store.py](../src/mulechain/node/store.py)). Each pass of the background work in `work_once` tries to take it and skips the pass if the other replica holds it. It belongs to the database session, so PostgreSQL frees it by itself if the holder dies. There is no leader election and no lease to time out.
- `JOIN_LOCK = 3105` ([case/store.py](../src/mulechain/case/store.py)). `record` stores each batch of messages in one transaction that holds this lock, so two writers can't both start a case for the same transfer.

**Source.** PostgreSQL documentation, [Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS): a lock taken at session level is held until it is released or the session ends.

**See it.** Bank A runs two privacy nodes on k3d. Delete either pod and the other takes over the background work on its next pass.

**Limit.** The locks exist only on PostgreSQL. The unit tests and the offline evaluator use SQLite with one process, where they always succeed.

## 8. Key agreement with no dealer

**Problem.** The two banks on a transfer must give it the same name, and no third bank may be able to compute that name. A central party that hands out keys could read everything.

**Here.** Each bank makes its own private keys and publishes only public keys, through its node's `Keys` call. Any two banks then derive the same pair key on their own: `HKDF(X25519(my private key, your public key))` (`pair_keys` in [crypto.py](../src/mulechain/crypto.py)). X25519 lets two banks compute the same secret from their own private key and the other's public key, and HKDF turns that secret into a key. A transfer's token is an HMAC of its payment ID under that pair key. Rotation re-indexes nothing already stored, and a bank publishes a change to its keys only after `KEY_SETTLE_SECONDS = 120` ([security.md](security.md#key-rotation)).

**Source.** Standard primitives only: X25519 key agreement ([RFC 7748](https://www.rfc-editor.org/rfc/rfc7748)), HKDF ([RFC 5869](https://www.rfc-editor.org/rfc/rfc5869)), HMAC ([RFC 2104](https://www.rfc-editor.org/rfc/rfc2104)), and Ed25519 for the signatures on claims ([RFC 8032](https://www.rfc-editor.org/rfc/rfc8032)), all from the Python `cryptography` library.

**See it.** `test_two_banks_derive_the_same_token_and_a_third_cannot` and `test_a_rotation_never_leaves_a_token_the_peer_has_not_indexed` ([test_crypto.py](../tests/test_crypto.py)), and `test_a_key_change_is_published_only_after_it_has_settled` ([test_system.py](../tests/test_system.py)).

**Limit.** The wait is a timer in the node's memory. A node that restarts within two minutes of a key change publishes the change at once.

## 9. Zero trust

Zero trust means that being inside the network earns nothing: every caller proves who it is, and every path is allowed by name.

**Problem.** A bank must not rely on a peer being honest about who it is, and a pod inside a bank must not be able to reach whatever it likes.

**Here.**

- **Identity comes from the certificate.** Banks talk over mutual TLS. `caller` in [node.py](../src/mulechain/node/node.py) reads the calling bank from the client certificate's URI name (`spiffe://csc3104/bank/<X>/privacy-node`) and never from a field in the message.
- **Least-privilege answers.** `lookup` in [bank.py](../src/mulechain/node/bank.py) answers about a transfer only to the bank on its other side, even if another bank has learnt the token.
- **Nothing is taken on trust.** Every claim in a chain is signed, and `valid_chain` in [claims.py](../src/mulechain/node/claims.py) checks every signature and that neighbouring claims name each other.
- **Default-deny network.** Every bank pod starts with no network access apart from DNS and the Kubernetes API server. [networkpolicy.yaml](../deploy/charts/bank/templates/networkpolicy.yaml) then allows each path a service needs by name, at both ends.
- **Restricted pods.** The bank namespaces enforce the restricted Pod Security Standard ([namespaces.yaml](../deploy/charts/platform/templates/namespaces.yaml)): non-root and no added privileges. The services' own pods also get no Kubernetes API token (`bank.pod` in [_helpers.tpl](../deploy/charts/bank/templates/_helpers.tpl)).

**Source.** NIST SP 800-207, [Zero Trust Architecture](https://csrc.nist.gov/pubs/sp/800/207/final) (2020): no trust is granted because of network location, and each request is authenticated and authorised. The certificate name has the form of a [SPIFFE ID](https://github.com/spiffe/spiffe/blob/main/standards/SPIFFE-ID.md). Kubernetes documents default-deny [network policies](https://kubernetes.io/docs/concepts/services-networking/network-policies/) and the [Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/).

**See it.** `make cluster-check` tries every allowed and blocked path from inside the pods, and calls a node with no certificate, a self-signed one, and a lab certificate that names no bank. [test_mtls.py](../tests/test_mtls.py) does the mTLS part without a cluster. [security.md](security.md) lists what is still disclosed.

## 10. Bulkhead and graceful degradation

A bulkhead walls off one part so that its failure can't spread, like a ship's watertight compartments.

**Problem.** A slow or broken part should cost only what that part provides.

**Here.**

- **Bulkhead around the summary service.** `summarizer` in [case/app.py](../src/mulechain/case/app.py) gives the call `SUMMARY_SECONDS = 2`. Any failure or slow answer means the page shows no draft, and the case itself is unaffected.
- **A bank being down delays cases but loses none.** A search that got no answer is retried (pattern 5). Meanwhile its case is shown as pending with the bank it is waiting for, and searches that don't need that bank carry on.
- **One queue per bank in replay.** A slow ingestor delays only its own bank's batches ([replay.py](../src/mulechain/replay.py)).
- **Event time.** The rules use each payment's own time, not its arrival time, so replay speed, retries and late events within the allowed lateness don't change what is detected (`_score` in [scorer.py](../src/mulechain/scorer.py)).

**Source.** Bulkheads are a stability pattern in Nygard, *Release It!* (2nd ed., 2018), also described as the [Bulkhead pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/bulkhead) in the Azure Architecture Center. Scoring by event time and not arrival time follows Akidau et al., [The Dataflow Model](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf) (VLDB 2015).

**See it.** `test_a_slow_or_broken_summary_service_only_costs_the_draft` ([test_case_app.py](../tests/test_case_app.py)), `test_a_bank_that_is_down_delays_cases_but_loses_none` ([test_system.py](../tests/test_system.py)) and `test_event_time_decides_not_arrival_order` ([test_scorer.py](../tests/test_scorer.py)). In a cluster: `kubectl scale deploy/privacy-node -n bank-c --replicas=0`, replay, then scale it back to 1.
