"""
LLM 响应结果存储 + 用量统计

Schema
├── results             (A2: 前端 WS 重连后恢复)
├── request_log         (P0: 用量统计)
└── switch_log          (P0: 切换事件)
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

DB_PATH = Path("/data/proxy_results.db")


class ResultStore:
    """Persist LLM responses and usage stats."""

    def __init__(self, db_path: str | Path = DB_PATH) -> None:
        import sqlite3

        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_tables()

    def _init_tables(self) -> None:
        # A2: result store
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
        # P0: request log
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS request_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                provider        TEXT    NOT NULL,
                model           TEXT,
                prompt_tokens   INTEGER DEFAULT 0,
                completion_tokens INTEGER DEFAULT 0,
                duration_ms     INTEGER DEFAULT 0,
                ttft_ms         INTEGER DEFAULT 0,
                switched        INTEGER DEFAULT 0,
                switched_from   TEXT,
                error           TEXT,
                created_at      REAL    NOT NULL
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_request_created ON request_log(created_at)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_request_provider ON request_log(provider)"
        )
        # P0: switch log
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS switch_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                from_provider   TEXT    NOT NULL,
                to_provider     TEXT,
                trigger_type    TEXT    NOT NULL,
                reason          TEXT,
                metrics_snapshot TEXT,
                created_at      REAL    NOT NULL
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_switch_created ON switch_log(created_at)"
        )
        self._conn.commit()

    # ── A2: result store ───────────────────────────────────────────────

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

    # ── P0: request log ────────────────────────────────────────────────

    def log_request(
        self,
        provider: str,
        model: str,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        duration_ms: int = 0,
        ttft_ms: int | None = None,
        switched: bool = False,
        switched_from: str | None = None,
        error: str | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO request_log (provider, model, prompt_tokens, completion_tokens, "
            "duration_ms, ttft_ms, switched, switched_from, error, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                provider, model, prompt_tokens, completion_tokens,
                duration_ms, ttft_ms or 0, 1 if switched else 0,
                switched_from, error, time.time(),
            ),
        )
        self._conn.commit()

    def log_switch(
        self,
        from_provider: str,
        to_provider: str | None = None,
        trigger_type: str = "hard",
        reason: str = "",
        metrics_snapshot: dict | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO switch_log (from_provider, to_provider, trigger_type, reason, "
            "metrics_snapshot, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                from_provider, to_provider, trigger_type, reason,
                json.dumps(metrics_snapshot) if metrics_snapshot else None,
                time.time(),
            ),
        )
        self._conn.commit()

    # ── P0: stats queries ──────────────────────────────────────────────

    def get_stats_summary(self, hours: int = 24) -> dict:
        cutoff = time.time() - hours * 3600
        row = self._conn.execute(
            "SELECT COUNT(*) as total, "
            "COALESCE(SUM(prompt_tokens),0) as prompt_tokens, "
            "COALESCE(SUM(completion_tokens),0) as completion_tokens, "
            "COALESCE(AVG(duration_ms),0) as avg_duration_ms, "
            "COALESCE(SUM(switched),0) as switches "
            "FROM request_log WHERE created_at >= ?",
            (cutoff,),
        ).fetchone()
        switch_row = self._conn.execute(
            "SELECT COUNT(*) as total FROM switch_log WHERE created_at >= ?",
            (cutoff,),
        ).fetchone()
        return {
            "total_requests": row["total"] if row else 0,
            "total_prompt_tokens": row["prompt_tokens"] if row else 0,
            "total_completion_tokens": row["completion_tokens"] if row else 0,
            "avg_duration_ms": round(row["avg_duration_ms"], 1) if row and row["total"] > 0 else 0,
            "total_switches": switch_row["total"] if switch_row else 0,
            "error_count": 0,
        }

    def get_stats_by_provider(self, hours: int = 24) -> list[dict]:
        cutoff = time.time() - hours * 3600
        rows = self._conn.execute(
            "SELECT provider, COUNT(*) as requests, "
            "COALESCE(SUM(prompt_tokens),0) as prompt_tokens, "
            "COALESCE(SUM(completion_tokens),0) as completion_tokens, "
            "COALESCE(AVG(duration_ms),0) as avg_duration_ms, "
            "COALESCE(SUM(switched),0) as switches "
            "FROM request_log WHERE created_at >= ? "
            "GROUP BY provider ORDER BY requests DESC",
            (cutoff,),
        ).fetchall()
        return [
            {
                "provider": r["provider"],
                "requests": r["requests"],
                "prompt_tokens": r["prompt_tokens"],
                "completion_tokens": r["completion_tokens"],
                "avg_duration_ms": round(r["avg_duration_ms"], 1),
                "switches": r["switches"],
            }
            for r in rows
        ]

    def get_recent_switches(self, hours: int = 24, limit: int = 50) -> list[dict]:
        cutoff = time.time() - hours * 3600
        rows = self._conn.execute(
            "SELECT id, from_provider, to_provider, trigger_type, reason, "
            "metrics_snapshot, created_at "
            "FROM switch_log WHERE created_at >= ? "
            "ORDER BY created_at DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
        return [
            {
                "id": r["id"],
                "from_provider": r["from_provider"],
                "to_provider": r["to_provider"],
                "trigger_type": r["trigger_type"],
                "reason": r["reason"],
                "metrics_snapshot": json.loads(r["metrics_snapshot"]) if r["metrics_snapshot"] else None,
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def get_trend_buckets(self, hours: int = 168) -> list[dict]:
        """Return time-series buckets for trend charts. Granularity adapts to range."""
        cutoff = time.time() - hours * 3600
        if hours <= 24:
            seconds = 3600       # 1-hour buckets → up to 24 points
        elif hours <= 168:
            seconds = 21600      # 6-hour buckets → up to 28 points
        else:
            seconds = 86400      # 24-hour buckets → up to 30 points

        rows = self._conn.execute(
            "SELECT CAST(STRFTIME('%s', DATETIME(created_at, 'unixepoch')) / ? AS INTEGER) * ? AS bucket, "
            "COUNT(*) AS requests, "
            "COALESCE(SUM(switched),0) as switches "
            "FROM request_log WHERE created_at >= ? "
            "GROUP BY bucket ORDER BY bucket",
            (seconds, seconds, cutoff),
        ).fetchall()
        return [
            {"timestamp": r["bucket"], "requests": r["requests"], "switches": r["switches"]}
            for r in rows
        ]

    def close(self) -> None:
        self._conn.close()
