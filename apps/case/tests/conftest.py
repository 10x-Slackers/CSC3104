import os

import pytest

from case.case_store import CaseStore

# Set CASE_TEST_DATABASE_URL=postgresql://... to run every store test on PostgreSQL too.
PG = os.environ.get("CASE_TEST_DATABASE_URL")
TABLES = "cases, transfers, messages, claims, own_legs, alerts, notifications"


@pytest.fixture(params=["sqlite"] + (["postgres"] if PG else []))
def url(request, tmp_path):
    if request.param == "sqlite":
        return str(tmp_path / "cases.db")
    import psycopg

    with psycopg.connect(PG, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {TABLES}")
    return PG


@pytest.fixture
def store(url):
    return CaseStore(url)
