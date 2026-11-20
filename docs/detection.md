# Detection

How a bank turns its own payments into rule hits and local links, alone, before any other bank is asked. The code is [scorer.py](../src/mulechain/scorer.py); the events it reads are described in [data.md](data.md), and what the privacy node does with its output in [protocol.md](protocol.md).

```text
Kafka topic (6 partitions, keyed by account) → scorer workers (1 to 6, KEDA) → Valkey (state, tokens, links, hits)
```

## The topic

Each bank has one topic, `bank-<X>-transactions`, declared in [kafka.yaml](../deploy/charts/bank/templates/kafka.yaml).

| Setting | Value | Why |
| --- | --- | --- |
| Partitions | 6 | Up to six scorer workers |
| Record key | The bank's own `account` | All of an account's events land in one partition, so one worker at a time owns the account |
| Retention | Forever (`retention.ms: -1`) | Kafka is the source of truth; Valkey is rebuilt from it |
| Replication | 3 brokers with `min.insync.replicas` 2 in a bank deployed with replicas (`ha`); 1 broker otherwise | A write survives the loss of one broker |

Kafka keeps the order of one partition, not the order across partitions, and a resent batch can write an event twice. The scorer is built so that neither matters ([below](#event-time-repeats-and-late-events)).

## The four rules

The rules run at every bank on bank transfers (GIRO and MEPS+, the only payments ingested). They flag suspicion, not proven laundering. No regulator publishes numeric thresholds, so each rule follows a published red flag where there is one, and its numbers were tuned on the dev split ([evaluation.md](evaluation.md)).

A **link** says "this incoming payment and this outgoing payment on one account look like the same money moving on". A **hit** is a rule firing that the analyst should see.

| Rule | Fires when | Basis | Produces |
| --- | --- | --- | --- |
| Fast pass-through | Money in, then 90–98 % of it out of the same account 1 to 30 minutes later (`FAST_RATIO`, `FAST_DELAY`) | [Guidelines to MAS Notice 626](https://www.mas.gov.sg/regulation/guidelines/guidelines-to-notice-626-on-prevention-of-money-laundering-and-cft-for-banks), Appendix B, B-2(iii): assets "withdrawn immediately after being deposited" | A link |
| Chain | Money in, then 90–110 % of it out of the same account between 1 minute and 72 hours later, and at least one of the two payments is with another bank (`CHAIN_RATIO`, `CHAIN_DELAY`) | Same guidelines, B-4(i): "high velocity of funds through an account"; 90 % of Altman's laundering hops follow within about 70 hours | A link, a `chain` hit, and a cross-bank search |
| New payee | A transfer to an account at another bank that this customer has never paid | No published red flag is cited. Fan-in, fan-out and scatter-gather laundering all create first-time payer and payee pairs | A `new_payee` hit |
| Quick repeat | A transfer to a payee at another bank whose previous payment from this account was flagged (a new payee hit, or the outgoing leg of a chain link) at most 24 hours earlier (`REPEAT_WINDOW`) | No published red flag is cited. It catches the first payment that follows a flagged one to the same payee | A `quick_repeat` hit |

- Every qualifying in/out pair on an account is linked. The scorer doesn't split an incoming amount across several outgoing payments.
- The fast band lies inside the chain band, so a fast link with another bank on either side is also a chain link. A fast link alone, with both payments inside the bank, makes no hit and no case (`test_fast_pass_through_inside_the_bank_links_without_a_hit`). It still lets the privacy node follow the money through the bank to the next cross-bank payment.
- Each `chain` hit starts one search at the privacy node (`take_hits` in [node.py](../src/mulechain/node/node.py)). Every hit becomes a case message; how hits and chains become Tier 1 cases and Tier 2 alerts is in [cases.md](cases.md).
- Tests: `test_chain_links_money_passed_on_across_banks`, `test_no_link_outside_the_ratio_or_delay`, `test_new_payee_then_quick_repeat_within_a_day` in [test_scorer.py](../tests/test_scorer.py).

## The scorer

A worker is a Kafka consumer in the group `scorer-<bank>` (`main`). For each event it calls `Scorer.process`, which does five things and commits them together:

1. **Record the bank's side of the payment** (`pay:`), and for a transfer with another bank, its token (`tok:`). The token is a keyed hash of the payment ID that only the two banks on the transfer can compute ([security.md](security.md)). Every cross-bank transfer is indexed, flagged or not, because a peer may ask about any of them. Until the privacy node has fetched the other bank's public keys, the worker waits.
2. **Read the account's window** of recent events and stop if this event is a repeat or too late.
3. **Pair the event with each opposite one** in the window and write the fast and chain links.
4. **Check the payee** for new payee and quick repeat (outgoing events only).
5. **Add the event to the window** for the events that follow.

All of an event's writes, plus its partition's next offset, go to Valkey in one `MULTI`/`EXEC` transaction: all of them happen or none do. A worker stops between events on SIGTERM. Each event that makes a link or a hit logs one JSON line with the payment ID, the links, the hits and the record's Kafka timestamp, which is what latency is measured from.

### Event time, repeats and late events

- **Event time, not arrival time.** Windows use the payment's own `event_time`, so replay speed doesn't change what is detected, and an `in` that arrives after its `out` is still paired (`test_event_time_decides_not_arrival_order`).
- **Repeats.** An event already in its account's window (same payment and direction) writes nothing new, and a hit's ID is a hash of the bank, the rule and the payments (`hit_identity`), so scoring an event twice is harmless (`test_a_replayed_event_changes_nothing`).
- **Late events.** An event more than 72 hours (`WINDOW`) behind the newest event seen on its account is recorded but not scored (`test_an_event_later_than_the_allowed_lateness_is_not_scored`). The window keeps twice that, 144 hours, so an accepted late event still finds its partner.
- **One arrival-order effect.** If a chain's outgoing payment arrives before its incoming one, the link and the hit are still written, but that outgoing payment doesn't count as flagged for a later quick repeat.

### Valkey layout

One Valkey per bank (ElastiCache on AWS), so keys carry no bank name. Only the scorer and the privacy node connect to it. Payment IDs, accounts and amounts stay in it; only tokens leave the bank.

| Key | Type | Holds | Read by |
| --- | --- | --- | --- |
| `win:<account>` | sorted set | The account's recent events (payment, direction, amount, other bank), scored by event time | Scorer |
| `pay:<payment>:<in\|out>` | hash | This bank's side of a payment: `account`, `amount`, `counterparty_bank`, and `token` for a cross-bank transfer | Node |
| `tok:<token>` | string | `<payment>:<in\|out>`: the index a peer's question lands in. One entry per key pair during a key rotation | Node |
| `link:in:<payment>`, `link:out:<payment>` | set | The outgoing payments linked onward from an incoming one, and the reverse | Node |
| `last:<account>:<payee>` | string | `<unix seconds>:<flagged>`: this customer's last payment to that payee | Scorer |
| `hits` | stream | One entry per hit: `id`, `rule`, `incoming`, `outgoing` | Node |
| `offsets` | hash | The next Kafka offset to score, per partition | Scorer |
| `keys` | hash | The other banks' public keys, written by the privacy node; the scorer derives its token keys from them and re-reads them every 10 s (`PairKeys`) | Scorer |

Every key except `last:`, `offsets` and `keys` expires 7 days after its last write (`TTL`). That is garbage collection only; it has to stay above the 144-hour window. `last:` entries are the payee history and are kept, and the `hits` stream is never trimmed (every new hit renews it), so both grow with the workload.

## Rebuilding Valkey from the log

Valkey is not persisted, and it holds its own position in the topic: the `offsets` hash, written with each event's state ([pattern 1](patterns.md#1-the-log-is-the-source-of-truth)). Workers that start on an empty Valkey therefore read the whole topic again and rebuild every key.

**A lost Valkey rebuilds itself.** Before a worker scores an event, it compares the partition's offset in Valkey with the one it last wrote there (`Scorer.process`). If they differ, Valkey has restarted empty. The worker stops without writing, Kubernetes restarts it, and it reads the partition from the start of the topic. The peers' public keys were in Valkey too, so a starting worker first waits until the privacy node has written them again, which takes up to 10 seconds.

```sh
make rebuild BANK=a     # the same on purpose: pause the scorers, empty Valkey, start the scorers again
```

A rebuild writes every hit to the stream again. The privacy node reads them again and nothing is duplicated, because a search is keyed by its two payments and a case message by the hit's ID ([protocol.md](protocol.md)). The node removes hits from the stream once it has stored them, so the stream holds only what is not yet read.

Limits:

- **A bank answers from partial state while it rebuilds.** The privacy node keeps running. A search of its own whose payments are missing stays pending and is tried again. A peer that asks about a transfer not yet rebuilt gets an empty answer and also tries again. But a peer that asks about a transfer whose onward link is not yet rebuilt gets a chain that ends at this bank. Run `make rebuild` while the bank is idle.
- **Workers wait for keys.** A worker scores nothing until Valkey holds every peer's public keys, because a transfer to that bank would get no token. A bank whose privacy node has never reached one of its peers therefore doesn't start scoring. After the first contact the node keeps the keys in PostgreSQL, so a peer that is down later doesn't stop the scorers.

## Autoscaling on lag

A KEDA `ScaledObject` (the [Apache Kafka scaler](https://keda.sh/docs/2.21/scalers/apache-kafka/)) in [scorer.yaml](../deploy/charts/bank/templates/scorer.yaml) sizes the scorer Deployment from the consumer group's lag, the number of events written but not yet scored.

| Setting | Value |
| --- | --- |
| Workers | 1 to 6 (`minReplicaCount`, `maxReplicaCount`): at most one per partition |
| `lagThreshold` | 100: one more worker per 100 events waiting |
| `offsetResetPolicy` | `earliest`, so a group that has committed nothing yet shows the whole topic as lag |
| Reaction time | The defaults: KEDA polls the lag every 30 s ([ScaledObject](https://keda.sh/docs/2.21/reference/scaledobject-spec/)), and Kubernetes removes workers about 5 minutes after the lag has gone ([HorizontalPodAutoscaler](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)) |

When workers are added or removed, Kafka moves partitions between them. A worker taking over a partition starts at the offset in Valkey, exactly where the last one stopped. Watch it with `kubectl get hpa -A` during a replay.
