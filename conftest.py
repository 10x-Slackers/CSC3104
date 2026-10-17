"""Keep pytest temporary files separate for each run and Windows account."""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    if config.option.basetemp is not None:
        return
    cache = Path(__file__).parent / ".cache"
    cache.mkdir(exist_ok=True)
    temporary = TemporaryDirectory(prefix="pytest-", dir=cache)
    config.add_cleanup(temporary.cleanup)
    config.option.basetemp = str(Path(temporary.name) / "tmp")
