# Build from the repo root: docker build -f apps/case/Dockerfile -t case-service:dev .
# Runs the analyst UI on 8000; for the mock intake, override the command with
#   python -m apps.case.intake   (port 8001)
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY apps ./apps
RUN uv sync --all-packages --locked --no-dev
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["python", "-m", "apps.case.app"]
