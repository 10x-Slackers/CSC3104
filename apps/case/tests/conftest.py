import pytest

from case.case_store import SqliteCaseStore


@pytest.fixture
def store(tmp_path):
    return SqliteCaseStore(tmp_path / "cases.db")
