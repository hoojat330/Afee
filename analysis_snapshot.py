"""
analysis_snapshot.py
─────────────────────────────────────────────────────────────────────────────
Captures and persists exactly what a strategy saw at the moment a signal was
approved, so the chart can be perfectly reproduced later (the "View Chart
Analysis" button) without ever re-running strategy logic or touching newer
candles.

Storage: a single dedicated table, `chart_snapshots`, inside the bot's
EXISTING SQLite database (the same file as `signals`, `results`, etc. — no
new database is created, and no existing table is touched or altered).
Only the minimal JSON blob needed to redraw the chart is stored, one row per
signal_id:

    CREATE TABLE IF NOT EXISTS chart_snapshots (
        signal_id     INTEGER PRIMARY KEY,   -- same id as signals.id
        snapshot_json TEXT    NOT NULL,       -- everything render_chart() needs
        created_at    TEXT    NOT NULL
    )
    CREATE INDEX IF NOT EXISTS idx_chart_snapshots_created_at ON chart_snapshots(created_at)

Retention: see cleanup_old_snapshots() — deletes rows older than a
configurable number of days (default 365). Meant to be called periodically
from a background task in the main bot process (never inline in the signal
path), so it can never delay or block signal generation.

Nothing in this module ever re-analyzes the market or recalculates
entry/stop/tp1/tp2 — it only serializes/deserializes values it is given.
"""

from __future__ import annotations
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional

# Same DB file the rest of the bot uses (see DB_FILE in the main bot module).
# Passed in explicitly by the caller in production (see
# build_and_save_chart_snapshot() / analysis_replay.render_replay() in the
# main bot file) — this default only matters if the module is ever run/
# tested standalone.
DEFAULT_DB_FILE = "afee_trader.db"

# Configurable retention period for chart_snapshots rows (see
# CHART_SNAPSHOT_RETENTION_DAYS in the main bot module, which is what
# production actually passes in — this default only matters for standalone/
# test use of this module).
DEFAULT_RETENTION_DAYS = 365


def _connect(db_path: str = DEFAULT_DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout=10000;")
    return conn


def ensure_table(db_path: str = DEFAULT_DB_FILE) -> None:
    """Idempotent, additive-only: creates the dedicated snapshots table (and
    its supporting index) if they don't exist yet. Never touches any other
    table. Safe to call from anywhere, any number of times (mirrors the
    bot's own init_database() pattern of CREATE TABLE/INDEX IF NOT EXISTS)."""
    conn = _connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chart_snapshots (
                signal_id     INTEGER PRIMARY KEY,
                snapshot_json TEXT    NOT NULL,
                created_at    TEXT    NOT NULL
            )
        """)
        # signal_id lookups (the replay button's read path) are already O(log n)
        # for free — it's the table's INTEGER PRIMARY KEY, i.e. the SQLite
        # rowid alias — so no extra index is needed there. created_at has no
        # such built-in index, and the cleanup job below filters/deletes by
        # it, so this index is what actually speeds that up (and any future
        # "snapshots from the last N days" style query) without adding any
        # write-side cost worth worrying about (one small index, ~daily job).
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chart_snapshots_created_at "
                     "ON chart_snapshots(created_at)")
        conn.commit()
    finally:
        conn.close()


def cleanup_old_snapshots(retention_days: int = DEFAULT_RETENTION_DAYS,
                           db_path: str = DEFAULT_DB_FILE) -> Optional[int]:
    """Deletes chart_snapshots rows older than `retention_days`. Returns the
    number of rows deleted, or None if the cleanup failed for any reason —
    callers must never let a cleanup failure propagate (this is background
    housekeeping, never a critical dependency, same failsafe posture as the
    rest of this feature). Only touches chart_snapshots; every other table
    is completely untouched by this function."""
    try:
        ensure_table(db_path)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        conn = _connect(db_path)
        try:
            cur = conn.execute("DELETE FROM chart_snapshots WHERE created_at < ?", (cutoff,))
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()
    except Exception:
        return None


def build_snapshot(result: dict, candles: list[dict], entry_timeframe: str,
                    structure_timeframe: Optional[str] = None,
                    trigger_idx: Optional[int] = None) -> dict:
    """
    Build the minimum JSON-serializable snapshot needed to perfectly
    recreate the chart later. `result` is the exact Signal dict a strategy
    returned (untouched); `candles` is the exact OHLCV list that strategy
    used for its entry-timeframe decision (not re-fetched); `result["visual"]`
    (if present) carries the strategy-specific drawing objects (fib anchors,
    zone level, breakout index, etc.) using indices into `candles`.
    """
    visual = result.get("visual") or {}
    if trigger_idx is None:
        trigger_idx = visual.get("trigger_idx", len(candles) - 1)
    trigger_idx = max(0, min(trigger_idx, len(candles) - 1))

    # NEW: preserve open_time + a top-level trigger_open_time. Needed only by
    # the "📸 Live Chart Update" feature to line up the original signal
    # candle with a freshly-fetched candle list by exact timestamp match —
    # a lookup, not a recalculation. entry/stop/tp1/tp2 below are untouched,
    # read verbatim from `result` exactly as before.
    trigger_open_time = candles[trigger_idx].get("open_time") if candles else None

    return {
        "symbol": result.get("symbol"),
        "signal_uid": result.get("signal_uid"),
        "strategy": result.get("strategy"),
        "direction": result.get("direction"),
        "score": result.get("score"),
        "signal_timestamp": datetime.now(timezone.utc).isoformat(),
        "structure_timeframe": structure_timeframe,
        "entry_timeframe": entry_timeframe,
        "entry": result.get("entry"),
        "stop": result.get("stop"),
        "tp1": result.get("tp1"),
        "tp2": result.get("tp2"),
        "market_regime": result.get("market_regime"),
        "trigger_idx": trigger_idx,
        "trigger_open_time": trigger_open_time,
        # Store only the OHLCV fields the renderer needs (no bloat).
        "candles": [
            {"open_time": c.get("open_time"), "open": c["open"], "high": c["high"],
             "low": c["low"], "close": c["close"], "volume": c["volume"]}
            for c in candles
        ],
        "visual": visual,
    }


def save_snapshot(signal_id: int, snapshot: dict, db_path: str = DEFAULT_DB_FILE) -> bool:
    try:
        ensure_table(db_path)
        conn = _connect(db_path)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO chart_snapshots (signal_id, snapshot_json, created_at) "
                "VALUES (?, ?, ?)",
                (signal_id, json.dumps(snapshot), datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception:
        return False


def load_snapshot(signal_id: int, db_path: str = DEFAULT_DB_FILE) -> Optional[dict]:
    try:
        conn = _connect(db_path)
        try:
            row = conn.execute(
                "SELECT snapshot_json FROM chart_snapshots WHERE signal_id = ?",
                (signal_id,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return json.loads(row[0])
    except Exception:
        return None
