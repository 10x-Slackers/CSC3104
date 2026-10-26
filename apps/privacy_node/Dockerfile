# Build from the repo root: docker build -f apps/privacy_node/Dockerfile -t privacy-node:dev .
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY apps ./apps
RUN uv sync --package privacy-node --locked --no-dev
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
EXPOSE 50051
ENTRYPOINT ["python", "-m", "apps.privacy_node.node"]
