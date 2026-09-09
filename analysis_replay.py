"""
analysis_replay.py
─────────────────────────────────────────────────────────────────────────────
Serves the "View Chart Analysis" button. Loads the saved snapshot for a
signal_id and renders it again through the SAME chart_renderer used at
signal time — never re-running strategy logic, never re-analyzing the
market, never using newer candles. If the snapshot is missing, returns None
so the caller can show a friendly message instead of crashing.
"""

from __future__ import annotations
import logging
from typing import Optional

import analysis_snapshot
import chart_renderer

log = logging.getLogger(__name__)


def render_replay(signal_id: int, db_path: str = analysis_snapshot.DEFAULT_DB_FILE) -> Optional[bytes]:
    snapshot = analysis_snapshot.load_snapshot(signal_id, db_path=db_path)
    if snapshot is None:
        log.info(f"analysis_replay: no snapshot for signal_id={signal_id}")
        return None
    try:
        return chart_renderer.render_chart(snapshot)
    except Exception as e:
        log.error(f"analysis_replay: render failed for signal_id={signal_id}: {e}")
        return None
