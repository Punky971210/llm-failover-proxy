from __future__ import annotations

import json
import time
from pathlib import Path

DB_PATH = Path("/data/proxy_results.db")


class ResultStore:
    """Persist LLM responses so frontend can retrieve them after WS disconnect."""

    def __init__(self, db_path: str | Path = DB_PATH) -> None:
        import sqlite3

        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT    NOT NULL,
                req_id      TEXT    NOT NULL,
                model       TEXT,
                content     TEXT,
                created_at  REAL,
                consumed    INTEGER DEFAULT 0
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_session ON results(session_id, consumed)"
        )
        self._conn.commit()

    def save(self, session_id: str, req_id: str, model: str, content: str) -> None:
        self._conn.execute(
            "INSERT INTO results (session_id, req_id, model, content, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (session_id, req_id, model, content, time.time()),
        )
        self._conn.commit()

    def get_unconsumed(self, session_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT id, req_id, model, content, created_at "
            "FROM results WHERE session_id=? AND consumed=0 ORDER BY id",
            (session_id,),
        ).fetchall()
        return [
            {
                "id": r[0],
                "req_id": r[1],
                "model": r[2],
                "content": r[3],
                "created_at": r[4],
            }
            for r in rows
        ]

    def mark_consumed(self, ids: list[int]) -> None:
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        self._conn.execute(
            f"UPDATE results SET consumed=1 WHERE id IN ({placeholders})", ids
        )
        self._conn.commit()
