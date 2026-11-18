import pytest


@pytest.fixture(autouse=True)
def no_jitter(monkeypatch):
    """Pending searches are due again at once, unless a test sets the jitter itself."""
    monkeypatch.setattr("privacy_node.store.random.random", lambda: 0.0)
