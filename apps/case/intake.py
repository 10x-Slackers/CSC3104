"""Mock authority intake: records each bank's reports and their arrival times.

    INTAKE_DB=intake.db PORT=8001 uv run apps/case/intake.py

Outside the detection path. received_at - raised_at is the intake response time
(docs/evaluation.md); never use it as detection latency.
"""

import json
import os
import sqlite3
import time

import uvicorn
from fastapi import FastAPI

SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    event_id TEXT NOT NULL,
    bank TEXT NOT NULL,
    case_id TEXT NOT NULL,
    raised_at REAL,
    received_at REAL NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (bank, event_id)
);
"""


def create_app(db):
    with sqlite3.connect(db) as conn:
        conn.executescript(SCHEMA)
    app = FastAPI(title="Mock intake")

    @app.post("/reports", status_code=204)
    def receive(report: dict):
        """Redeliveries keep the first arrival time."""
        with sqlite3.connect(db) as conn:
            conn.execute(
                "INSERT INTO reports VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (
                    report["event_id"],
                    report["bank"],
                    report["case_id"],
                    report.get("raised_at"),
                    time.time(),
                    json.dumps(report, sort_keys=True),
                ),
            )

    @app.get("/reports")
    def reports():
        with sqlite3.connect(db) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM reports ORDER BY received_at")
            return [dict(row) | {"body": json.loads(row["body"])} for row in rows]

    return app


if __name__ == "__main__":
    uvicorn.run(
        create_app(os.environ.get("INTAKE_DB", "intake.db")),
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8001")),
    )
