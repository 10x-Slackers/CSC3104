# AGENTS

## Project context

This is a **university coursework project** done by undergraduates. It is not a production system and will never be deployed to real users.

## Research First

- Always research before implementing a task — both the codebase and online. Understand existing patterns, dependencies, and conventions before writing code.
- Refer to the latest documentation and existing implementations of any libraries or frameworks in use.
- Check `docs/` for architectural and planning details relevant to the task.
- Fetch and branch from the latest `origin/main` before starting work or a review.

## Ask When Unsure

- Clarify ambiguities before proceeding — ask rather than guess.
- When in doubt about scope, priorities, or critical decisions, surface the uncertainty and let the user decide.

## Engineering posture

Optimize for **low line count, readability, and simplicity** — not for production hardening. When a tradeoff exists:

- **Prefer fewer lines of code.** Skip defensive checks, input validation, retry logic, and abstractions that a production codebase would require.
- **Prefer readability.** A short, obvious solution beats a clever, dense one.
- **Security and robustness may be traded for simplicity.** It is acceptable to a certain degree to trust inputs, skip edge-case handling, and omit safeguards (rate limiting, sanitization layers, error boundaries, etc.) if removing them makes the code clearer and shorter.
- **Don't over-engineer.** No speculative extensibility, no config layers for options nobody uses yet, no premature abstraction.

## Checks

- After every task, run formatting and linting checks if applicable.
- For example, for Python, run the following commands from the repository root:

```sh
uv sync --all-packages --locked
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

- When a change alters a command, file path or behaviour described in `docs/`, update that doc in the same PR.

## Python

- Use `uv` for Python environments, dependencies, and commands, not pip.
- Follow the Python version in `.python-version` and requirements in `pyproject.toml`.
- Keep runtime dependencies with the application that needs them and shared development tools in the root `dev` dependency group.
- Use the shared workspace environment and lockfile rather than separate member environments.
- Commit `uv.lock` and update it through `uv`; do not edit it by hand.
- Each app is a workspace package: `apps/<app>/src/<package>/` and `apps/<app>/tests/`.
- Import other apps by package name (`from security import ...`) and declare them as `{ workspace = true }` sources. Run modules with `uv run python -m <package>.<module>`.
- Regenerate `*_pb2*.py` with the command in the `.proto` header; don't edit them.
