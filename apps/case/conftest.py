import sys
from pathlib import Path

# The pytest executable does not add the repository root for namespace imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest

from apps.case.case_store import SqliteCaseStore


@pytest.fixture
def store(tmp_path):
    return SqliteCaseStore(tmp_path / "cases.db")
