# Case handling contract

Owner: Jia Rui · Partners: Zane (result delivery), Cindy, Isaac (alert counts), Caleb (report inputs) · Status: TODO

Scope: the bank-local case schema, canonical chain identity, durable alert uniqueness, the analyst UI, mock intake and the AI-assisted report.

## Cases and alerts

| Item | Decision |
| --- | --- |
| Case schema | TODO |
| When a chain is complete enough to alert | TODO |
| Canonical chain identity across overlapping searches | TODO. Deduplicating queries alone doesn't stop duplicate alerts from searches that started at different banks |
| Unique alert key (durable, bank-local) | TODO |
| Partial → confirmed updates | TODO |
| Durable pending notifications and acks | TODO. Retries may happen; only one alert is visible to the user |

## Analyst UI

- Framework: FastAPI + HTMX or Streamlit (TODO).
- Analyst decisions (all simulated): continue monitoring, suspend outgoing transactions, freeze accounts, report chain evidence to the local authorities (the mock intake).
- The mock intake records timestamps and is outside the detection path.

## AI-assisted report

- Inputs are limited to permitted case fields (Caleb reviews them). The report shows its supporting evidence and requires human review.
- Model and hosting: TODO.
- A report failure must not block detection. A template fallback keeps the demo usable but **does not count** as AI reporting.

## Open

- TODO
