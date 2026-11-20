# AGENTS

## Project context

This is a **university coursework project** by undergraduates. It is not a production system and will never be deployed to real users.

## Research

- Before implementing, read the code and `docs/` relevant to the task so changes follow existing patterns. Scale this to the task; a small fix needs only the files it touches.
- Check library and framework APIs against current documentation, not memory; the versions here may be newer than your training data.
- Fetch and branch from the latest `origin/main` before starting work or a review.

## When to ask

- Ask before deciding anything that changes scope, priorities, the design in `docs/`, or is hard to reverse.
- For smaller ambiguities, pick the option most consistent with the existing code and state the assumption in your summary.

## Engineering posture

Optimize for **low line count, readability, and simplicity** — not for production hardening. When a tradeoff exists:

- **Prefer fewer lines of code.** Skip the defensive checks, input validation, retry logic and abstractions a production codebase would require.
- **Prefer readability.** A short, obvious solution beats a clever, dense one.
- **Security and robustness may be traded for simplicity.** Within reason, trust inputs, skip edge-case handling, and omit safeguards (rate limiting, sanitization layers, error boundaries) when that makes the code clearer and shorter.
- **Don't over-engineer.** No speculative extensibility, no config layers for options nobody uses yet, no premature abstraction.

## Checks

After each change, run from the repository root:

```sh
make check    # uv sync --locked, ruff check, ruff format --check, pytest
```

- After changing a chart, also run `helm lint deploy/charts/*` (CI does).
- `deploy/test_cluster.py` needs a running cluster (`make k3d`, `make replay`, `make cluster-check`); run it when a change touches the charts, NetworkPolicies, mTLS or the image.
- When a change alters a command, file path or behaviour described in `docs/`, update that doc in the same PR. The docs describe the system as it is: no names, owners, status markers or history.

## Layout

- `src/mulechain/` is one package with one module per component (`ingestor`, `scorer`, `node/`, `case/`, `summary`, `replay`, `prepare`, `evaluate`) and three shared modules (`crypto`, `db`, `serverless`). A service module's docstring says which patterns from `docs/patterns.md` it shows; keep that true.
- `tests/` holds the unit tests; `deploy/` holds the k3d config, helmfile, charts, Terraform and cluster checks; the `Makefile` holds every command.
- The offline evaluator (`mulechain.evaluate`) runs the same scorer, node and case code as the cluster. A change to detection or the protocol must keep the two in step.

## Python

- Use `uv` for Python environments, dependencies, and commands, not pip.
- Follow the Python version in `.python-version` and requirements in `pyproject.toml`.
- Runtime dependencies go in `[project] dependencies`; development tools in the `dev` group; what only `mulechain.prepare` needs in the `data` group.
- Commit `uv.lock` and update it through `uv`; do not edit it by hand.
- Run modules with `uv run python -m mulechain.<module>`.
- Regenerate `*_pb2*.py` with `make proto`; don't edit them.
