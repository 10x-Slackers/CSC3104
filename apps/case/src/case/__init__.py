"""Role 5: bank-local cases, alert tiers and the read-only analyst UI."""

import json
import time


def log(**fields) -> None:
    """One timestamped JSON line to stdout."""
    print(json.dumps({"ts": time.time(), "service": "case"} | fields), flush=True)
