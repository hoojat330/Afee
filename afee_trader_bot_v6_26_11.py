#!/usr/bin/env python3
"""
AFEE TRADER BOT - Advanced AI Trading Signal Bot
Strategies: Stop Hunter | Hammer Fibonacci | HB | Trigger Fibonacci | RSI Divergence | Exhaustion 
"""

import asyncio
import contextvars
import copy
import hashlib
import aiohttp
import json
import time
import logging
import os
import sqlite3
import shutil
from datetime import datetime, timezone
from typing import Optional
import math

# ─── AI CHART ANALYSIS (visualization-only, additive feature) ────────────────
# Isolated rendering/replay layer — see chart_renderer.py, drawing_tools.py,
# analysis_snapshot.py, analysis_replay.py. None of these modules import any
# strategy code, and no strategy/signal/scoring/filter/DB code depends on
# them either. If they fail to import (e.g. matplotlib missing on a given
# deployment) the bot must keep working exactly as before — chart rendering
# is optional, never a critical dependency, per spec.
try:
    import chart_renderer as _chart_renderer
    import analysis_snapshot as _analysis_snapshot
    import analysis_replay as _analysis_replay
    CHART_FEATURE_AVAILABLE = True
except Exception as _chart_import_err:
    _chart_renderer = None
    _analysis_snapshot = None
    _analysis_replay = None
    CHART_FEATURE_AVAILABLE = False
    logging.getLogger(__name__).warning(
        f"AI Chart Analysis feature disabled (import failed): {_chart_import_err}"
    )

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_VERSION = "6.26.11"
TELEGRAM_TOKEN = "7166787350:AAFGk-3oqFeX2w5yXv28X3hxKXQblbXV-94"
TELEGRAM_CHAT_ID = "-1002532379243"
# FIX (Pass 3 audit, critical): gates who is allowed to become the bot's
# super-admin — see the /start handler in handle_update() for why this
# matters. Set this to your own numeric Telegram user ID (message
# @userinfobot to get it) via the OWNER_TELEGRAM_ID environment variable.
OWNER_TELEGRAM_ID = os.environ.get("OWNER_TELEGRAM_ID", "").strip()
BINANCE_BASE = "https://fapi.binance.com/fapi/v1"  # Futures API (USDT-M Perpetual)

# Timeframe map  (Binance interval string)
TF = {
    "1m": "1m", "3m": "3m", "5m": "5m",
    "15m": "15m", "1h": "1h", "4h": "4h",
}

BLACKLIST_FILE   = "blacklist.json"
TRADES_FILE      = "trades.json"     # فقط برای بکاپ/مهاجرت دیتای قدیمی؛ منبع اصلی دیتابیس است
DB_FILE          = "afee_trader.db"  # دیتابیس SQLite اصلی (سیگنال‌ها، نتایج، آمار)
# AI CHART ANALYSIS — how long chart_snapshots rows are kept before the
# background cleanup job deletes them (see chart_snapshot_cleanup_loop() in
# main()). Configurable via env var; purely a housekeeping setting, has no
# effect on trading logic, signals, or statistics.
CHART_SNAPSHOT_RETENTION_DAYS = int(os.environ.get("CHART_SNAPSHOT_RETENTION_DAYS", "365"))
# Signal Rejection Report — timezone used ONLY for the admin report's day
# boundaries (Today/Yesterday/Two days ago) and the displayed time range.
# Every timestamp actually written to the database remains UTC, always —
# this setting never touches storage, only how report ranges are computed
# and converted to UTC before querying. Example: "Asia/Tehran".
REPORT_TIMEZONE = os.environ.get("REPORT_TIMEZONE", "UTC")
SIGNAL_COOLDOWN  = 3600   # seconds between signals for same coin/strategy
SCAN_INTERVAL    = 60     # seconds to wait after each full scan
# FIX (bug audit): تعداد شکست متوالی fetch قیمت قبل از بستن اجباری معامله به EXPIRED
MAX_PRICE_FETCH_FAILURES = 5
TOP_N_COINS_DEFAULT   = 40      # مقدار پیش‌فرض — قابل تغییر از bot_config.json بدون ویرایش کد
QUALITY_POOL_SIZE_DEFAULT = 150  # مقدار پیش‌فرض — قابل تغییر از bot_config.json

# ─── BOT CONFIG FILE (Patch #4/#5: Dynamic Score + TOP_N_COINS از فایل config) ──
# Unified persistent settings file — ALL user-configurable settings (strategy
# enable/disable, filters, thresholds, score settings, bot options, and the
# Dynamic Score / TOP_N_COINS values below) live in this single JSON file.
# Copying bot_config.json to another server/deployment restores every saved
# setting. No secrets (API keys, Telegram tokens) are ever written here.
BOT_CONFIG_FILE = "bot_config.json"
# Legacy filename used by older bot versions for the "state" half of the
# settings (admins, disabled_strategies, filter toggles, etc.). Kept only so
# we can migrate any existing legacy file into the unified bot_config.json
# below — no new data is ever written to this legacy path.
LEGACY_BOT_STATE_FILE = "bot_state.json"

BOT_CONFIG_DEFAULTS = {
    "TOP_N_COINS": TOP_N_COINS_DEFAULT,
    "QUALITY_POOL_SIZE": QUALITY_POOL_SIZE_DEFAULT,
    "DYNAMIC_SCORE_THRESHOLD_TREND": 75,
    "DYNAMIC_SCORE_THRESHOLD_RANGE": 85,
    "DYNAMIC_SCORE_THRESHOLD_HIGH_VOLATILITY": 90,
    "DYNAMIC_SCORE_THRESHOLD_WHIPSAW": 90,
    "DYNAMIC_SCORE_THRESHOLD_TRANSITIONAL": 70,
}

# ─── CONFIG BACKUP / RESTORE (feature: /exportconfig, /importconfig) ──────────
# Keys that must never be read from an imported bot_config.json, even if
# present in the uploaded file — secrets/credentials are never stored in
# bot_config.json by this bot in the first place, but this blocklist is a
# defense-in-depth guard applied to any imported file regardless of source.
CONFIG_IMPORT_SECRET_BLOCKLIST = {
    "TELEGRAM_TOKEN", "TOKEN", "BOT_TOKEN",
    "API_KEY", "API_SECRET", "APIKEY", "APISECRET",
    "SECRET", "SECRETS", "OWNER_TELEGRAM_ID", "ENV",
}
# Minimal sanity-check: an imported bot_config.json must at least contain the
# core numeric settings this bot ships with by default. This catches a file
# that isn't really a bot_config.json (wrong export, unrelated JSON, etc.)
# without being so strict that a slightly older/newer legitimate export gets
# rejected — everything else in the unified file (admins, channels, strategy
# toggles, etc.) is optional/backward-compatible via .setdefault()-style merges
# elsewhere in this file.
CONFIG_IMPORT_REQUIRED_KEYS = set(BOT_CONFIG_DEFAULTS.keys())
CONFIG_IMPORT_MAX_BYTES = 1_000_000  # 1 MB

# In-memory only (never written to disk): tracks which super-admin uids are
# currently in an import flow. Kept out of bot_state/bot_config/bot_runtime on
# purpose — this is pure UI-flow bookkeeping, not a persistent bot setting.
_PENDING_IMPORT_WAIT: set = set()   # uids that ran /importconfig and are now expected to upload a file
_PENDING_IMPORT_DATA: dict = {}     # uid -> raw file bytes awaiting Yes/No confirmation

# bot_runtime.json — TRANSIENT/operational data only (never a user setting):
# pending Telegram flows and last-execution bookkeeping timestamps. Anything
# NOT in this set that lives in bot_state/config is a permanent user setting
# and belongs in bot_config.json instead. See load_state()/save_state() below.
BOT_RUNTIME_FILE = "bot_runtime.json"
RUNTIME_STATE_KEYS = {
    "pending_admin_add",
    "pending_analysis",
    "pending_replay",
    "pending_backtest",
    # Task 3 (/setscore panel): uid + which strategy idx is awaiting a typed
    # minimum-score number — same "waiting for text reply" pattern as
    # pending_analysis/pending_replay above, so it's pure runtime bookkeeping
    # (not a user setting) and belongs in bot_runtime.json, not bot_config.json.
    "pending_setscore",
    "last_report_date",
    "last_reweight_date",
    # Daily Report (feature): purely operational bookkeeping for which
    # message to edit, not a user-facing setting — belongs with the other
    # runtime/operational keys above, not in the permanent config file.
    "daily_report_msg_ids",
    "daily_report_msg_date",
    "daily_report_last_text",
    # Daily Report (day-boundary tracking): the Asia/Tehran calendar day the
    # bot is CURRENTLY silently tracking (not yet published — see
    # check_daily_report_rollover()). The report content itself also covers
    # exactly this complete Asia/Tehran calendar day (see
    # tehran_date_of_iso(), get_trades_for_report(), build_daily_report());
    # this key doubles as both *when* (Tehran wall-clock) the next report
    # gets sent and *which* Tehran day it covers. Pure runtime bookkeeping,
    # never a user setting.
    "daily_report_collecting_day",
    # Weekly/Monthly Signal Performance Report (feature): which calendar
    # week/month has already been sent, purely for duplicate-prevention
    # across restarts/redeployments — same bookkeeping role as
    # daily_report_collecting_day above, never a user-facing setting.
    "weekly_report_last_week_id",
    "monthly_report_last_month_id",
}

def load_bot_config() -> dict:
    """Read the editable settings from bot_config.json (no bot restart needed).
    Note: since bot_config.json is now the single unified persistence file
    (Task 1), this can also return other, non-"config" keys (bot state,
    strategy toggles, etc.) that live in the same file — callers that only
    want the small numeric config values should filter by BOT_CONFIG_DEFAULTS
    keys (see the /config command handler) rather than iterating everything."""
    defaults = BOT_CONFIG_DEFAULTS
    if not os.path.exists(BOT_CONFIG_FILE):
        # If the file doesn't exist yet, create it with default values
        try:
            import json as _json
            with open(BOT_CONFIG_FILE, "w", encoding="utf-8") as _f:
                _json.dump(defaults, _f, indent=2, ensure_ascii=False)
        except Exception:
            pass
        return defaults
    try:
        import json as _json
        with open(BOT_CONFIG_FILE, encoding="utf-8") as f:
            cfg = _json.load(f)
        # Make sure all keys exist (backward-compat)
        for k, v in defaults.items():
            cfg.setdefault(k, v)
        return cfg
    except Exception as e:
        log.warning(f"bot_config.json was corrupted, using default values: {e}")
        return defaults

def get_top_n_coins() -> int:
    """تعداد نهایی نمادهای اسکن را از bot_config.json می‌خواند (Patch #5)."""
    return int(load_bot_config().get("TOP_N_COINS", TOP_N_COINS_DEFAULT))

def get_quality_pool_size() -> int:
    """تعداد نامزدهای اولیه کیفیت را از bot_config.json می‌خواند."""
    return int(load_bot_config().get("QUALITY_POOL_SIZE", QUALITY_POOL_SIZE_DEFAULT))

# ─── Dynamic Score thresholds (Patch #4) ──────────────────────────────────────
def get_dynamic_score_threshold(regime_label: str, runtime_min: int, cfg: dict = None) -> int:
    """
    Patch #4: آستانه‌های Dynamic Score از bot_config.json خوانده می‌شوند (نه hardcoded).
    مقادیر قابل ویرایش در bot_config.json:
      DYNAMIC_SCORE_THRESHOLD_TREND       (پیش‌فرض: 75)
      DYNAMIC_SCORE_THRESHOLD_RANGE       (پیش‌فرض: 85)
      DYNAMIC_SCORE_THRESHOLD_HIGH_VOLATILITY (پیش‌فرض: 90)
      DYNAMIC_SCORE_THRESHOLD_WHIPSAW     (پیش‌فرض: 90)
      DYNAMIC_SCORE_THRESHOLD_TRANSITIONAL (پیش‌فرض: 70)

    REPRODUCIBILITY FIX (root cause D): `cfg` is an optional pre-loaded
    config dict (same shape as load_bot_config()'s return value). Live
    scanning never passes it (cfg=None), so it reads bot_config.json fresh
    exactly as before — zero behavior change there. Historical Replay passes
    a config snapshot taken ONCE at job start (see _run_replay_backtest_job),
    so every candidate in the same job — and across two separate "identical"
    replay requests, as long as nobody actually changed the config in
    between — reads the exact same threshold values, instead of picking up
    whatever bot_config.json happens to contain at the moment each
    individual candidate is evaluated (which could drift mid-job, or between
    two runs, from an admin edit or an auto-tuning feature writing to the
    same file).
    """
    cfg = cfg if cfg is not None else load_bot_config()
    if "TRENDING" in regime_label:
        return max(runtime_min, cfg.get("DYNAMIC_SCORE_THRESHOLD_TREND", 75))
    elif "RANGING" in regime_label:
        return max(runtime_min, cfg.get("DYNAMIC_SCORE_THRESHOLD_RANGE", 85))
    elif "WHIPSAW" in regime_label:
        return max(runtime_min, cfg.get("DYNAMIC_SCORE_THRESHOLD_WHIPSAW", 90))
    elif "HIGH" in regime_label:
        return max(runtime_min, cfg.get("DYNAMIC_SCORE_THRESHOLD_HIGH_VOLATILITY", 90))
    elif "TRANSITIONAL" in regime_label:
        return max(runtime_min, cfg.get("DYNAMIC_SCORE_THRESHOLD_TRANSITIONAL", 70))
    return runtime_min

# compat aliases — سایر بخش‌های کد از این استفاده می‌کنند
PARALLEL_WORKERS = 15     # coins scanned simultaneously

# ─── BACKTEST ENGINE CONFIG ────────────────────────────────────────────────────
BACKTEST_BATCH_SIZE    = 25   # نمادهای موازی در هر batch بک‌تست
BACKTEST_MAX_QUEUE     = 3    # حداکثر jobهای بک‌تست در صف (از overload جلوگیری می‌کند)
BACKTEST_CACHE_TTL     = 300  # ثانیه — نتایج بک‌تست تکراری از cache تحویل داده می‌شوند

# ─── SESSION/ADX FILTER CONSTANTS ─────────────────────────────────────────────
ADX_THRESHOLD_DEFAULT     = 25.0
ATR_MULTIPLIER_DEFAULT    = 2.2
SESSION_FILTERS_DEFAULT   = {"london": True, "ny": True, "asian": True}

# ─── PROXY ────────────────────────────────────────────────────────────────────
PROXY = None  # Railway: no proxy needed (direct internet access)

# ─── LOGGING (UTF-8 safe for Windows CMD) ────────────────────────────────────
import sys
log = logging.getLogger("AFEE")
log.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
# File handler — always UTF-8
fh = logging.FileHandler("afee_bot.log", encoding="utf-8")
fh.setFormatter(fmt)
log.addHandler(fh)
# Console handler — safe encoding
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
log.addHandler(sh)

# ═══════════════════════════════════════════════════════════════════════════════
# 🔬 DIAGNOSTIC INSTRUMENTATION (temporary — signal-funnel audit)
# ─────────────────────────────────────────────────────────────────────────────
# Purpose: answer "exactly where are candidates being eliminated?" with real
# counted numbers instead of guesses. OFF by default (state["diagnostics_enabled"],
# default False) — zero behavior change and near-zero overhead when off, since
# every call site below is a single `if DIAG_ENABLED:` guarded counter bump.
#
# What it records, per strategy:
#   - Coarse funnel stage counters (raw candidate reached / internal gate passed
#     or rejected, by name / reached shared filters / rejected by which filter /
#     passed all filters) via diag_count(strategy, stage)
#   - The RAW pre-gate score of every candidate that reaches run_live_filters,
#     bucketed, via diag_score(strategy, score)
#   - The RAW ADX value of every candidate evaluated by the ADX filter,
#     bucketed, via diag_adx(strategy, adx_value)
#
# Enable with /diagon (admin), run a Historical Replay, then /diagexport to
# dump everything collected to a JSON file. /diagreset clears counters.
# This is intentionally simple (in-memory dict, flushed to disk on export) —
# it is meant to be run for one focused replay, not left on permanently.
from collections import defaultdict

DIAG_ENABLED = False  # flipped on/off via state["diagnostics_enabled"]; see set_diag_enabled()
_DIAG_STAGE_COUNTS: dict = defaultdict(lambda: defaultdict(int))   # {strategy: {stage: count}}
_DIAG_SCORE_BUCKETS: dict = defaultdict(lambda: defaultdict(int))  # {strategy: {"<70":n, "70-79":n, ...}}
_DIAG_ADX_BUCKETS: dict = defaultdict(lambda: defaultdict(int))    # {strategy: {"<15":n, "15-20":n, ...}}

def set_diag_enabled(on: bool) -> None:
    global DIAG_ENABLED
    DIAG_ENABLED = bool(on)

def diag_count(strategy: str, stage: str) -> None:
    if DIAG_ENABLED:
        _DIAG_STAGE_COUNTS[strategy][stage] += 1

def _score_bucket(score) -> str:
    try:
        s = int(score)
    except Exception:
        return "unknown"
    if s < 70: return "<70"
    if s < 80: return "70-79"
    if s < 90: return "80-89"
    if s < 95: return "90-94"
    if s < 98: return "95-97"
    return "98-100"

def diag_score(strategy: str, score) -> None:
    if DIAG_ENABLED:
        _DIAG_SCORE_BUCKETS[strategy][_score_bucket(score)] += 1

def _adx_bucket(adx) -> str:
    try:
        a = float(adx)
    except Exception:
        return "unknown"
    if a < 15: return "<15"
    if a < 18: return "15-18"
    if a < 20: return "18-20"
    if a < 25: return "20-25"
    if a < 30: return "25-30"
    return "30+"

def diag_adx(strategy: str, adx) -> None:
    if DIAG_ENABLED:
        _DIAG_ADX_BUCKETS[strategy][_adx_bucket(adx)] += 1

def diag_reset() -> None:
    _DIAG_STAGE_COUNTS.clear()
    _DIAG_SCORE_BUCKETS.clear()
    _DIAG_ADX_BUCKETS.clear()

def diag_export(path: str = "diagnostics_report.json") -> str:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "funnel_stage_counts": {k: dict(v) for k, v in _DIAG_STAGE_COUNTS.items()},
        "score_distribution":  {k: dict(v) for k, v in _DIAG_SCORE_BUCKETS.items()},
        "adx_distribution":    {k: dict(v) for k, v in _DIAG_ADX_BUCKETS.items()},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return path
# ═══════════════════════════════════════════════════════════════════════════════

# ─── ONE-TIME STARTUP MIGRATION ────────────────────────────────────────────────
# Two things need handling here, both safe/idempotent to run on every startup:
#  1) A very old bot version may have left a legacy bot_state.json on disk
#     (from before settings were unified into bot_config.json at all) — merge
#     it in if so.
#  2) An older version of THIS bot may have written transient/runtime fields
#     (see RUNTIME_STATE_KEYS) into bot_config.json alongside real settings.
#     bot_config.json must only ever contain permanent user configuration, so
#     any such fields found there get moved out into bot_runtime.json instead.
def _migrate_legacy_and_split_runtime():
    legacy_data = {}
    if os.path.exists(LEGACY_BOT_STATE_FILE):
        try:
            with open(LEGACY_BOT_STATE_FILE, encoding="utf-8") as f:
                legacy_data = json.load(f)
        except Exception as e:
            log.warning(f"Could not read legacy {LEGACY_BOT_STATE_FILE} for migration: {e}")
            try:
                os.replace(LEGACY_BOT_STATE_FILE, LEGACY_BOT_STATE_FILE + ".unreadable")
            except Exception:
                pass
            legacy_data = {}

    cfg_data = {}
    if os.path.exists(BOT_CONFIG_FILE):
        try:
            with open(BOT_CONFIG_FILE, encoding="utf-8") as f:
                cfg_data = json.load(f)
        except Exception:
            cfg_data = {}

    # Anything from legacy bot_state.json that isn't already in bot_config.json gets added.
    changed_cfg = False
    for k, v in legacy_data.items():
        if k not in cfg_data:
            cfg_data[k] = v
            changed_cfg = True

    # Pull any transient/runtime keys back out of bot_config.json — they
    # don't belong there (Task: bot_config.json = permanent settings only).
    runtime_from_cfg = {}
    for k in list(cfg_data.keys()):
        if k in RUNTIME_STATE_KEYS:
            runtime_from_cfg[k] = cfg_data.pop(k)
            changed_cfg = True

    if runtime_from_cfg:
        runtime_data = {}
        if os.path.exists(BOT_RUNTIME_FILE):
            try:
                with open(BOT_RUNTIME_FILE, encoding="utf-8") as f:
                    runtime_data = json.load(f)
            except Exception:
                runtime_data = {}
        # An existing bot_runtime.json value wins — it's more recent than
        # whatever was still sitting in bot_config.json before the split.
        for k, v in runtime_from_cfg.items():
            runtime_data.setdefault(k, v)
        try:
            tmp_file = BOT_RUNTIME_FILE + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(runtime_data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, BOT_RUNTIME_FILE)
            log.info(f"Moved runtime fields {sorted(runtime_from_cfg.keys())} out of {BOT_CONFIG_FILE} into {BOT_RUNTIME_FILE}.")
        except Exception as e:
            log.error(f"Failed writing split-out runtime fields to {BOT_RUNTIME_FILE}: {e}")

    if changed_cfg:
        try:
            tmp_file = BOT_CONFIG_FILE + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(cfg_data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, BOT_CONFIG_FILE)
        except Exception as e:
            log.error(f"Legacy/runtime migration write to {BOT_CONFIG_FILE} failed: {e}")

    if os.path.exists(LEGACY_BOT_STATE_FILE):
        try:
            os.replace(LEGACY_BOT_STATE_FILE, LEGACY_BOT_STATE_FILE + ".migrated")
            log.info(f"Migrated legacy {LEGACY_BOT_STATE_FILE} into {BOT_CONFIG_FILE}/{BOT_RUNTIME_FILE}.")
        except Exception:
            pass

_migrate_legacy_and_split_runtime()

# ─── BLACKLIST ─────────────────────────────────────────────────────────────────
# Persistence unified onto bot_config.json (Task 3) — the exact same
# permanent-settings mechanism Strategy Settings uses (save automatically,
# restore automatically after every restart, no separate file/DB). A
# blacklist.json left over from an older bot version is migrated in
# automatically, once, the first time the blacklist is read.
def load_blacklist() -> set:
    cfg = load_bot_config()
    if "blacklist" in cfg:
        try:
            return set(cfg.get("blacklist") or [])
        except Exception:
            return set()

    # One-time migration from the legacy blacklist.json file, if present.
    migrated = set()
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, encoding="utf-8") as f:
                migrated = set(json.load(f))
        except Exception as e:
            log.error(f"⚠️ blacklist.json خراب بود، خالی برگردانده شد: {e}")
            try:
                os.replace(BLACKLIST_FILE, BLACKLIST_FILE + ".corrupted")
            except Exception:
                pass
    save_blacklist(migrated)
    if os.path.exists(BLACKLIST_FILE):
        try:
            os.replace(BLACKLIST_FILE, BLACKLIST_FILE + ".migrated")
            log.info(f"Migrated legacy {BLACKLIST_FILE} into {BOT_CONFIG_FILE}.")
        except Exception:
            pass
    return migrated

def save_blacklist(bl: set):
    try:
        cfg_now = load_bot_config()
        cfg_now["blacklist"] = sorted(bl)
        tmp_file = BOT_CONFIG_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(cfg_now, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, BOT_CONFIG_FILE)
    except Exception as e:
        log.error(f"خطا در ذخیره blacklist در {BOT_CONFIG_FILE}: {e}")

def add_to_blacklist(symbol: str):
    bl = load_blacklist()
    bl.add(symbol.upper())
    save_blacklist(bl)
    log.info(f"Added {symbol} to blacklist")

# ─── TRADE LOG (برای اتو بک‌تست و گزارش روزانه) ────────────────────────────────
# ─── DATABASE (SQLite) ────────────────────────────────────────────────────────
# جایگزین کامل ذخیره‌سازی JSON برای معاملات با دیتابیس SQLite.
# Schema: signals (سیگنال‌های صادرشده) + results (نتیجه نهایی هر سیگنال، 1-به-1 با signals).
# تمام توابع زیر همان نام و رفتار قبلی (load_trades/save_trades/log_trade/...) را حفظ کرده‌اند
# تا بقیه کد بدون تغییر کار کند؛ فقط لایه ذخیره‌سازی از JSON به SQLite تغییر کرده است.

_db_lock = asyncio.Lock()  # جلوگیری از نوشتن هم‌زمان چند coroutine روی دیتابیس

def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")  # نوشتن امن‌تر و هم‌زمان‌تر
    conn.execute("PRAGMA foreign_keys=ON;")
    # FIX (Pass 2 audit, low-medium severity): make the "wait for a writer"
    # timeout explicit rather than relying on sqlite3's undocumented-in-code
    # default. With ~24 separate call sites opening/closing their own short-
    # lived connections (main scan loop, backtest worker, Telegram command
    # handlers, daily report/reweight loops all running concurrently), a
    # "database is locked" SQLITE_BUSY error is possible under contention;
    # without a busy_timeout, sqlite3 raises immediately instead of retrying.
    conn.execute("PRAGMA busy_timeout=10000;")  # retry for up to 10s before raising
    return conn

def init_database():
    """ساخت جدول‌های دیتابیس در صورت عدم وجود. اجرای چندباره این تابع بی‌خطر است (IF NOT EXISTS)."""
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                direction   TEXT    NOT NULL,
                strategy    TEXT    NOT NULL,
                entry       REAL    NOT NULL,
                stop        REAL    NOT NULL,
                tp1         REAL    NOT NULL,
                tp2         REAL    NOT NULL,
                score       INTEGER,
                opened_at   TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS results (
                signal_id    INTEGER PRIMARY KEY REFERENCES signals(id),
                status       TEXT    NOT NULL DEFAULT 'PENDING',
                -- Patch #6 states: PENDING|ENTERED|TP1_HIT|TP2_HIT|SL_HIT|EXPIRED|MISSED
                -- Legacy states kept for compatibility: OPEN|TP1|TP2|SL|EXPIRED
                entered_at   TEXT,
                tp1_hit      INTEGER NOT NULL DEFAULT 0,   -- Patch #4: 0/1 independent flag
                tp2_hit      INTEGER NOT NULL DEFAULT 0,   -- Patch #4: 0/1 independent flag
                tp1_hit_at   TEXT,
                tp2_hit_at   TEXT,
                closed_at    TEXT,
                close_price  REAL,
                pnl_percent  REAL,
                rr_multiple  REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategy_weights (
                strategy     TEXT    PRIMARY KEY,
                weight       REAL    NOT NULL DEFAULT 1.0,
                updated_at   TEXT
            )
        """)
        # ── جدید: ذخیره نتایج بک‌تست تفصیلی (هر trade یک ردیف) ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_trades (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id        TEXT    NOT NULL,
                symbol        TEXT    NOT NULL,
                strategy      TEXT    NOT NULL,
                direction     TEXT    NOT NULL,
                entry         REAL    NOT NULL,
                stop          REAL    NOT NULL,
                tp1           REAL    NOT NULL,
                tp2           REAL    NOT NULL,
                entry_ts      TEXT,
                exit_ts       TEXT,
                outcome       TEXT,   -- TP1|TP2|SL|OPEN
                pnl_percent   REAL,
                rr_multiple   REAL,
                trend_state   TEXT,
                adx_value     REAL,
                ema200_slope  TEXT,
                vol_regime    TEXT,
                session       TEXT,
                entry_reason  TEXT,
                created_at    TEXT    NOT NULL
            )
        """)
        # ── جدید: ذخیره job metadata برای صف بک‌تست ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_jobs (
                job_id      TEXT    PRIMARY KEY,
                status      TEXT    NOT NULL DEFAULT 'QUEUED',  -- QUEUED|RUNNING|DONE|FAILED
                requested_by TEXT,
                params      TEXT,   -- JSON: timerange, symbols, strategies
                created_at  TEXT    NOT NULL,
                completed_at TEXT,
                result_summary TEXT  -- JSON: total, wins, losses, pnl
            )
        """)
        # ── جدید: پیکربندی فیلترها (قابل تغییر در Runtime) ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS filter_config (
                key         TEXT    PRIMARY KEY,
                value       TEXT    NOT NULL,
                updated_at  TEXT
            )
        """)
        # ── signal_notifications: ذخیره message_id تلگرام و وضعیت اعلان‌های Entry/TP/SL/MISSED ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_notifications (
                signal_id       INTEGER PRIMARY KEY REFERENCES signals(id),
                tg_message_id   INTEGER,
                entry_notified  INTEGER NOT NULL DEFAULT 0,
                tp1_notified    INTEGER NOT NULL DEFAULT 0,
                tp2_notified    INTEGER NOT NULL DEFAULT 0,
                sl_notified     INTEGER NOT NULL DEFAULT 0,
                missed_notified INTEGER NOT NULL DEFAULT 0,
                trade_closed    INTEGER NOT NULL DEFAULT 0
            )
        """)
        # FIX (lifecycle patch): add entry_notified/missed_notified columns to pre-existing
        # signal_notifications tables (idempotent migration, same pattern as results table below)
        existing_notif_cols = {row[1] for row in conn.execute("PRAGMA table_info(signal_notifications)").fetchall()}
        notif_migrations = [
            ("entry_notified",  "ALTER TABLE signal_notifications ADD COLUMN entry_notified  INTEGER NOT NULL DEFAULT 0"),
            ("missed_notified", "ALTER TABLE signal_notifications ADD COLUMN missed_notified INTEGER NOT NULL DEFAULT 0"),
        ]
        for col_name, sql in notif_migrations:
            if col_name not in existing_notif_cols:
                try:
                    conn.execute(sql)
                    log.info(f"DB migration: added column signal_notifications.{col_name}")
                except Exception as e:
                    log.warning(f"DB migration skipped signal_notifications.{col_name}: {e}")

        # ── AI CHART ANALYSIS — dedicated snapshot table (additive, isolated) ──
        # Stores only the minimal JSON needed to replay a signal's chart
        # later (see analysis_snapshot.py). Deliberately its own table, with
        # no FOREIGN KEY constraint on signals(id), so a missing/failed
        # snapshot can never affect writes to `signals` — chart rendering
        # must remain fully optional and decoupled from trading logic.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chart_snapshots (
                signal_id     INTEGER PRIMARY KEY,
                snapshot_json TEXT    NOT NULL,
                created_at    TEXT    NOT NULL
            )
        """)
        # signal_id lookups (the replay button's read path) are already
        # O(log n) for free via the INTEGER PRIMARY KEY (SQLite rowid alias)
        # — no extra index needed there. created_at has no such built-in
        # index, and the retention cleanup job below filters/deletes by it,
        # so this is the one index that's actually beneficial here.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chart_snapshots_created_at "
                     "ON chart_snapshots(created_at)")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_strategy ON signals(symbol, strategy)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_results_status ON results(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_results_closed_at ON results(closed_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bt_trades_job ON backtest_trades(job_id)")

        # ── HISTORICAL STRATEGY REPLAY — completely separate from Advanced
        # Backtest (backtest_jobs/backtest_trades above, which stay untouched
        # and keep reading REAL live signals via _load_closed_trades_from_db).
        # This system re-runs the actual strategy functions against
        # historical candles to generate HYPOTHETICAL signals/trades. Its own
        # tables, its own ID namespace (BT-YYYYMMDD-NNN), never mixed with
        # live signals/results or with Advanced Backtest's data.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS replay_backtests (
                backtest_id       TEXT    PRIMARY KEY,   -- e.g. BT-20260815-001
                status            TEXT    NOT NULL DEFAULT 'RUNNING',
                -- RUNNING | DONE | PARTIAL | INTERRUPTED | FAILED
                period_label      TEXT    NOT NULL,       -- "24h", "7d", ...
                scope_label       TEXT    NOT NULL,       -- e.g. "Live-configured universe (TOP_N_COINS=100, quality-ranked from pool of 120)"
                symbols_json      TEXT    NOT NULL,       -- full symbol list for this run, IN SELECTION ORDER (diagnostic item 2)
                settings_snapshot_json TEXT NOT NULL,     -- frozen per-strategy settings active at job start (diagnostic item 4)
                total_symbols     INTEGER NOT NULL DEFAULT 0,
                symbols_done      INTEGER NOT NULL DEFAULT 0,
                signals_found     INTEGER NOT NULL DEFAULT 0,
                trades_resolved   INTEGER NOT NULL DEFAULT 0,
                summary_json      TEXT,                   -- filled once DONE/PARTIAL
                created_at        TEXT    NOT NULL,
                updated_at        TEXT    NOT NULL,
                completed_at      TEXT,
                period_start_ms   INTEGER,                -- diagnostic item 1
                period_end_ms     INTEGER,                -- diagnostic item 1
                weights_snapshot_json TEXT,                -- frozen strategy_weights at job start (diagnostic item 3)
                config_snapshot_json  TEXT                 -- frozen bot_config.json (dynamic thresholds, min_signal_score, TOP_N_COINS, QUALITY_POOL_SIZE) at job start (diagnostic item 4)
            )
        """)
        # REPRODUCIBILITY DIAGNOSTICS (temporary — see the reproducibility
        # investigation). OFF by default; populated only while DIAG_ENABLED
        # is on (same /diagon toggle as the existing lightweight funnel
        # counters above), scoped per backtest_id so two runs can be
        # directly diffed. Meant to be run for one focused comparison, then
        # cleared with /diagreset — not left on for ordinary replay usage.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS replay_diag_candidates (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                backtest_id     TEXT    NOT NULL,
                symbol          TEXT    NOT NULL,
                strategy        TEXT    NOT NULL,
                sim_time_ms     INTEGER NOT NULL,
                direction       TEXT,
                regime_label    TEXT,
                raw_score       REAL,            -- diagnostic item 6/7: score BEFORE weight/filters
                weight_applied  REAL,            -- diagnostic item 7: frozen strategy weight used
                final_score     REAL,            -- diagnostic item 7: score AFTER weight, before gate
                min_required    REAL,            -- diagnostic item 7: effective dynamic/min-score threshold
                passed_filters  INTEGER,         -- diagnostic item 6/8: 1/0
                passed_score_gate INTEGER,       -- diagnostic item 8: 1/0
                rejection_reason TEXT,           -- diagnostic item 8: exact reason string, or NULL if accepted
                entry REAL, stop REAL, tp1 REAL, tp2 REAL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_replay_diag_candidates_bt "
                     "ON replay_diag_candidates(backtest_id, symbol, strategy, sim_time_ms)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS replay_diag_candles (
                backtest_id   TEXT    NOT NULL,
                symbol        TEXT    NOT NULL,
                timeframe     TEXT    NOT NULL,
                candle_count  INTEGER NOT NULL,
                first_open_ms INTEGER,
                last_open_ms  INTEGER,
                sha256_hash   TEXT    NOT NULL,   -- diagnostic item 5: hash of the exact OHLCV series used
                PRIMARY KEY (backtest_id, symbol, timeframe)
            )
        """)
        # REPRODUCIBILITY FIX: add period_start_ms/period_end_ms/weights_
        # snapshot_json/config_snapshot_json to pre-existing replay_backtests
        # tables (idempotent migration, same pattern as signal_notifications
        # above) — these are the diagnostic columns needed to compare two
        # runs for the reproducibility investigation.
        existing_rb_cols = {row[1] for row in conn.execute("PRAGMA table_info(replay_backtests)").fetchall()}
        rb_migrations = [
            ("period_start_ms",       "ALTER TABLE replay_backtests ADD COLUMN period_start_ms INTEGER"),
            ("period_end_ms",         "ALTER TABLE replay_backtests ADD COLUMN period_end_ms INTEGER"),
            ("weights_snapshot_json", "ALTER TABLE replay_backtests ADD COLUMN weights_snapshot_json TEXT"),
            ("config_snapshot_json",  "ALTER TABLE replay_backtests ADD COLUMN config_snapshot_json TEXT"),
        ]
        for col_name, sql in rb_migrations:
            if col_name not in existing_rb_cols:
                try:
                    conn.execute(sql)
                    log.info(f"DB migration: added column replay_backtests.{col_name}")
                except Exception as e:
                    log.warning(f"DB migration skipped replay_backtests.{col_name}: {e}")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS replay_trades (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                backtest_id   TEXT    NOT NULL REFERENCES replay_backtests(backtest_id),
                symbol        TEXT    NOT NULL,
                strategy      TEXT    NOT NULL,
                direction     TEXT    NOT NULL,
                timeframe     TEXT,
                score         INTEGER,
                market_regime TEXT,
                signal_time   TEXT    NOT NULL,   -- simulated detection time
                entry_time    TEXT,               -- when hypothetical entry was touched
                entry         REAL    NOT NULL,
                stop          REAL    NOT NULL,
                tp1           REAL    NOT NULL,
                tp2           REAL    NOT NULL,
                exit_time     TEXT,
                exit_price    REAL,
                result        TEXT    NOT NULL DEFAULT 'PENDING',
                -- PENDING | MISSED | SL | TP1_TOUCHSL | TP2 | OPEN (unresolved at window end)
                pnl_percent   REAL,
                rr_multiple   REAL,
                visual_json   TEXT,   -- wave/zone/fib anchors, for chart audit reconstruction
                created_at    TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS replay_chart_snapshots (
                trade_id      INTEGER PRIMARY KEY REFERENCES replay_trades(id),
                snapshot_json TEXT    NOT NULL,
                created_at    TEXT    NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_replay_trades_bt ON replay_trades(backtest_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_replay_trades_strategy ON replay_trades(backtest_id, strategy)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_replay_trades_symbol ON replay_trades(backtest_id, symbol)")
        conn.commit()

        # ── v4.16 schema migration: add Signal ID (signal_uid) column to signals table
        # (idempotent — same ALTER TABLE ... IF NOT EXISTS pattern used for results below).
        # Purpose: a stable, human-readable ID (SIG-YYYYMMDD-000123) attached to a signal
        # at creation time and carried through every log line for that signal's entire
        # lifecycle (generation → entry → TP1/TP2/SL/BE → close → daily report/backtest).
        # Logging/traceability only — never read by any strategy/filter/scoring logic.
        existing_signal_cols = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
        if "signal_uid" not in existing_signal_cols:
            try:
                conn.execute("ALTER TABLE signals ADD COLUMN signal_uid TEXT")
                log.info("DB migration: added column signals.signal_uid")
            except Exception as e:
                log.warning(f"DB migration skipped signals.signal_uid: {e}")
        conn.commit()

        # ── v3.1 schema migration: add new columns to existing results table (idempotent) ──
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(results)").fetchall()}
        migrations = [
            ("tp1_hit",    "ALTER TABLE results ADD COLUMN tp1_hit    INTEGER NOT NULL DEFAULT 0"),
            ("tp2_hit",    "ALTER TABLE results ADD COLUMN tp2_hit    INTEGER NOT NULL DEFAULT 0"),
            ("entered_at", "ALTER TABLE results ADD COLUMN entered_at TEXT"),
            ("tp1_hit_at", "ALTER TABLE results ADD COLUMN tp1_hit_at TEXT"),
            ("tp2_hit_at", "ALTER TABLE results ADD COLUMN tp2_hit_at TEXT"),
            # FIX (bug audit): فیل‌سیف برای fetch قیمت — شمارنده شکست‌های متوالی + دلیل بسته شدن
            ("price_fail_count", "ALTER TABLE results ADD COLUMN price_fail_count INTEGER NOT NULL DEFAULT 0"),
            ("close_reason",     "ALTER TABLE results ADD COLUMN close_reason TEXT"),
            # Pending Signal Management / Override (feature): audit trail for
            # any result that was set manually (via the admin panel or
            # /override) instead of by the automatic monitor. Purely
            # additive/informational — never read by trading/monitoring
            # logic, only by admin-facing views.
            ("manual_result", "ALTER TABLE results ADD COLUMN manual_result INTEGER NOT NULL DEFAULT 0"),
            ("manual_by",      "ALTER TABLE results ADD COLUMN manual_by TEXT"),
            ("manual_at",      "ALTER TABLE results ADD COLUMN manual_at TEXT"),
        ]
        for col_name, sql in migrations:
            if col_name not in existing_cols:
                try:
                    conn.execute(sql)
                    log.info(f"DB migration: added column results.{col_name}")
                except Exception as e:
                    log.warning(f"DB migration skipped results.{col_name}: {e}")

        # Migrate legacy OPEN statuses to PENDING so state machine works correctly
        try:
            conn.execute("UPDATE results SET status='PENDING' WHERE status='OPEN'")
            # FIX v3.4: اگر سیستم آپگرید شد، مطمئن شو TP1_TOUCHSL در جدول قابل ذخیره است
            # (SQLite text column هر مقداری می‌گیرد — نیاز به ALTER TABLE نیست)
        except Exception:
            pass

        conn.commit()
    finally:
        conn.close()

def _migrate_json_trades_to_db():
    """مهاجرت یک‌باره دیتای قدیمی trades.json (در صورت وجود) به دیتابیس SQLite.
    بعد از مهاجرت موفق، فایل JSON برای امنیت به trades.json.migrated تغییرنام می‌یابد
    تا دیتای قدیمی هیچ‌وقت گم نشود، ولی این تابع دوباره روی آن اجرا نشود."""
    if not os.path.exists(TRADES_FILE):
        return
    try:
        with open(TRADES_FILE, encoding="utf-8") as f:
            old_trades = json.load(f)
        if not old_trades:
            os.replace(TRADES_FILE, TRADES_FILE + ".migrated")
            return

        conn = get_db_connection()
        migrated = 0
        try:
            for t in old_trades:
                cur = conn.execute(
                    "INSERT INTO signals (symbol, direction, strategy, entry, stop, tp1, tp2, score, opened_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (t["symbol"], t["direction"], t["strategy"], t["entry"], t["stop"],
                     t["tp1"], t["tp2"], t.get("score"), t["opened_at"])
                )
                signal_id = cur.lastrowid
                rr = None
                if t.get("pnl_percent") is not None and t.get("entry") and t.get("stop"):
                    risk_pct = abs(t["entry"] - t["stop"]) / t["entry"] * 100
                    rr = (t["pnl_percent"] / risk_pct) if risk_pct > 0 else None
                conn.execute(
                    "INSERT INTO results (signal_id, status, closed_at, close_price, pnl_percent, rr_multiple) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (signal_id, t.get("status", "OPEN"), t.get("closed_at"),
                     t.get("close_price"), t.get("pnl_percent"), rr)
                )
                migrated += 1
            conn.commit()
        finally:
            conn.close()

        os.replace(TRADES_FILE, TRADES_FILE + ".migrated")
        log.info(f"✅ مهاجرت {migrated} رکورد قدیمی از trades.json به دیتابیس SQLite انجام شد.")
    except Exception as e:
        log.error(f"⚠️ خطا در مهاجرت trades.json به دیتابیس: {e}")
        try:
            os.replace(TRADES_FILE, TRADES_FILE + ".migration_failed")
        except Exception:
            pass

def backup_database_to_json():
    """خروجی JSON از کل دیتابیس برای بکاپ (طبق درخواست: JSON فقط برای بکاپ، نه منبع اصلی)."""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy, s.entry, s.stop, s.tp1, s.tp2,
                   s.score, s.opened_at, r.status, r.closed_at, r.close_price, r.pnl_percent, r.rr_multiple
            FROM signals s LEFT JOIN results r ON r.signal_id = s.id
            ORDER BY s.id
        """).fetchall()
        data = [dict(row) for row in rows]
    finally:
        conn.close()

    tmp_file = TRADES_FILE + ".backup.tmp"
    backup_file = TRADES_FILE + ".backup"
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, backup_file)
    except Exception as e:
        log.error(f"خطا در ذخیره بکاپ JSON: {e}")

def _row_to_trade_dict(row: sqlite3.Row) -> dict:
    """یک ردیف join‌شده از signals+results را به همان شکل دیکشنری قدیمی trades.json تبدیل می‌کند
    تا کد بالادست (گزارش روزانه و غیره) بدون تغییر کار کند.
    Patch #4/#6: نگاشت state جدید به compat status قدیمی برای کدهای موجود."""
    raw_status = row["status"] or "PENDING"
    # Normalize new states to legacy-compatible values for existing report code
    _status_compat = {
        "PENDING": "OPEN", "ENTERED": "OPEN",
        "TP1_HIT": "TP1", "TP2_HIT": "TP2",
        "TP1_TOUCHSL": "TP1",  # FIX v3.4: TP1 زده شد سپس Break Even → نتیجه = TP1
        "SL_HIT": "SL", "MISSED": "MISSED",
        "EXPIRED": "EXPIRED", "OPEN": "OPEN",
        "TP1": "TP1", "TP2": "TP2", "SL": "SL",
    }
    compat_status = _status_compat.get(raw_status, raw_status)
    return {
        "symbol": row["symbol"], "direction": row["direction"], "strategy": row["strategy"],
        "entry": row["entry"], "stop": row["stop"], "tp1": row["tp1"], "tp2": row["tp2"],
        "score": row["score"], "opened_at": row["opened_at"],
        "signal_uid": row["signal_uid"] if "signal_uid" in row.keys() else None,
        "status": compat_status, "raw_status": raw_status,
        "tp1_hit": row["tp1_hit"] if "tp1_hit" in row.keys() else 0,
        "tp2_hit": row["tp2_hit"] if "tp2_hit" in row.keys() else 0,
        "closed_at": row["closed_at"], "close_price": row["close_price"],
        "pnl_percent": row["pnl_percent"], "rr_multiple": row["rr_multiple"], "id": row["id"],
    }

def load_trades() -> list:
    """تمام سیگنال‌ها + نتایج‌شان را از دیتابیس می‌خواند (جایگزین خوانش trades.json قدیمی)."""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy, s.entry, s.stop, s.tp1, s.tp2,
                   s.score, s.opened_at, s.signal_uid, r.status, r.tp1_hit, r.tp2_hit,
                   r.closed_at, r.close_price, r.pnl_percent, r.rr_multiple
            FROM signals s LEFT JOIN results r ON r.signal_id = s.id
            ORDER BY s.id
        """).fetchall()
        return [_row_to_trade_dict(row) for row in rows]
    finally:
        conn.close()

def save_trades(trades: list):
    """برای حفظ سازگاری با کدهای قدیمی نگه‌داشته شده؛ امروز دیگر مستقیم استفاده نمی‌شود
    چون نوشتن از طریق log_trade/update_trade_outcomes با دیتابیس انجام می‌شود.
    در صورت صدا زده شدن، فقط یک بکاپ JSON از وضعیت فعلی می‌سازد."""
    backup_database_to_json()

def generate_signal_uid(conn: sqlite3.Connection, signal_id: int, opened_at_dt: datetime = None) -> str:
    """New Signal ID System (feature): builds the human-facing ID in the form
    YYMMDD-NN — the two-digit UTC date plus that day's sequential signal count
    (01, 02, 03, ...). This is the ONE id used everywhere a signal needs to be
    referenced by a human: the signal message/caption, chart image, admin panel,
    daily report, and the /override command. Purely a display/reference id —
    never used by any trading logic, and the underlying DB primary key
    (signals.id) is completely unaffected and remains the real relational key.
    Must be called on the same connection/transaction as the INSERT that
    created `signal_id`, before commit, so the just-inserted row is visible
    to the COUNT(*) below (same-connection uncommitted reads work in SQLite)."""
    dt = opened_at_dt or datetime.now(timezone.utc)
    date_prefix = dt.strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE opened_at LIKE ? AND id <= ?",
        (f"{date_prefix}%", signal_id),
    ).fetchone()
    seq = row[0] if row and row[0] else 1
    return f"{dt.strftime('%y%m%d')}-{seq:02d}"

def generate_rejection_ref() -> str:
    """A signal that gets REJECTED before it ever becomes a trade has no DB row
    and therefore no signal_uid — but rejection reasons still need a unique
    per-attempt reference so a single rejection log line can be pinpointed
    during debugging. Ephemeral (never persisted, never shown to users) — kept
    only for internal log correlation, separate from the user-facing Signal ID."""
    now = datetime.now(timezone.utc)
    return f"REJ-{now.strftime('%Y%m%d-%H%M%S%f')[:15]}"

def log_trade(result: dict):
    """هر سیگنال صادر شده را در دیتابیس ثبت می‌کند (signals + ردیف اولیه PENDING در results).
    Signal ID & Traceability (feature): also stamps a unique signal_uid (SIG-YYYYMMDD-NNNNNN)
    on the row right after insert, derived from the row's own autoincrement id — this is the
    ID that gets carried through every later log line for this trade's lifecycle."""
    conn = get_db_connection()
    try:
        opened_at_dt = datetime.now(timezone.utc)
        opened_at = opened_at_dt.isoformat()
        cur = conn.execute(
            "INSERT INTO signals (symbol, direction, strategy, entry, stop, tp1, tp2, score, opened_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (result["symbol"], result["direction"], result["strategy"], result["entry"],
             result["stop"], result["tp1"], result["tp2"], result.get("score"),
             opened_at)
        )
        signal_id = cur.lastrowid
        signal_uid = generate_signal_uid(conn, signal_id, opened_at_dt)
        conn.execute("UPDATE signals SET signal_uid=? WHERE id=?", (signal_uid, signal_id))
        conn.execute(
            "INSERT INTO results (signal_id, status) VALUES (?, 'PENDING')",
            (signal_id,)
        )
        conn.commit()
        log.info(
            f"Signal generation | signal_id={signal_id} signal_uid={signal_uid} "
            f"{result['symbol']} {result['strategy']} {result['direction']} "
            f"entry={result['entry']} stop={result['stop']} tp1={result['tp1']} tp2={result['tp2']} "
            f"score={result.get('score')} opened_at={opened_at}"
        )
        return signal_id, signal_uid
    finally:
        conn.close()

def save_signal_tg_message_id(signal_id: int, tg_message_id: int):
    """ذخیره message_id تلگرام برای سیگنال ارسال‌شده — جهت استفاده در reply اعلان‌های بعدی."""
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT INTO signal_notifications (signal_id, tg_message_id) VALUES (?, ?) "
            "ON CONFLICT(signal_id) DO UPDATE SET tg_message_id=excluded.tg_message_id",
            (signal_id, tg_message_id)
        )
        conn.commit()
    except Exception as e:
        log.error(f"save_signal_tg_message_id error: {e}")
    finally:
        conn.close()

# ─── AI CHART ANALYSIS — snapshot build + render (isolated, additive) ────────
# This section only VISUALIZES a signal that has already been fully decided.
# It never re-analyzes the market, never re-runs strategy logic, and never
# touches candles newer than the signal itself (the `candles` list here is
# the exact list a strategy fetched and used — see the `visual` key each
# strategy now attaches to its own return dict). Any failure here must never
# block signal delivery; every entry point below is defensive.

def _parse_structure_timeframe(strategy: str, timeframe_str: str) -> Optional[str]:
    """Best-effort label for the higher timeframe used for market structure,
    for display/metadata purposes only — it never affects which candles are
    rendered (that is always the entry timeframe, from `visual['entry_tf']`)."""
    try:
        if strategy.startswith("Hammer Fib"):
            # e.g. "Hammer Fib (4H)" -> "4H"
            if "(" in strategy and ")" in strategy:
                return strategy.split("(")[1].split(")")[0]
        if "1H/4H" in timeframe_str:
            return "1H/4H"
        if "1H/15m" in timeframe_str:
            return "1H/15m"
        if timeframe_str.startswith("S/R on"):
            return "1H/4H"
    except Exception:
        pass
    return None


def build_and_save_chart_snapshot(result: dict, signal_id: int) -> Optional[dict]:
    """Builds the analysis snapshot for a signal (if the strategy attached
    visual data) and persists it to disk for later replay. Returns the
    snapshot dict on success, or None if unavailable/failed — callers must
    treat None as "no chart for this signal" and continue normally."""
    if not CHART_FEATURE_AVAILABLE or not signal_id:
        return None
    visual = result.get("visual")
    if not visual or not visual.get("candles"):
        return None
    try:
        candles = visual["candles"]
        entry_tf = visual.get("entry_tf", result.get("timeframe", ""))
        structure_tf = _parse_structure_timeframe(result.get("strategy", ""), result.get("timeframe", ""))
        # Store only the drawing-object fields in the snapshot's "visual" —
        # the raw candle list is stored once, at the snapshot's top level,
        # by build_snapshot() itself (no need to duplicate it here).
        visual_for_storage = {k: v for k, v in visual.items() if k not in ("candles",)}
        result_for_snapshot = dict(result)
        result_for_snapshot["visual"] = visual_for_storage
        snapshot = _analysis_snapshot.build_snapshot(
            result_for_snapshot, candles,
            entry_timeframe=entry_tf, structure_timeframe=structure_tf,
            trigger_idx=visual.get("trigger_idx"),
        )
        _analysis_snapshot.save_snapshot(signal_id, snapshot, db_path=DB_FILE)
        return snapshot
    except Exception as e:
        log.error(f"build_and_save_chart_snapshot error for signal_id={signal_id}: {e}")
        return None


def render_signal_chart(snapshot: dict) -> Optional[bytes]:
    """Renders a PNG from an already-built snapshot. Never raises — chart
    rendering must be optional, per spec."""
    if not CHART_FEATURE_AVAILABLE or not snapshot:
        return None
    try:
        return _chart_renderer.render_chart(snapshot)
    except Exception as e:
        log.error(f"render_signal_chart error: {e}")
        return None


def get_signal_notification_row(signal_id: int) -> Optional[dict]:
    """خواندن ردیف اعلان مرتبط با یک سیگنال از دیتابیس."""
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT * FROM signal_notifications WHERE signal_id=?", (signal_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def mark_notification_sent(signal_id: int, event: str, trade_closed: bool = False):
    """علامت‌گذاری ارسال اعلان برای جلوگیری از ارسال مجدد. event: 'entry'|'tp1'|'tp2'|'sl'|'missed'"""
    col_map = {"entry": "entry_notified", "tp1": "tp1_notified", "tp2": "tp2_notified",
               "sl": "sl_notified", "missed": "missed_notified"}
    col = col_map.get(event)
    if not col:
        return
    closed_val = 1 if trade_closed else 0
    conn = get_db_connection()
    try:
        conn.execute(
            f"UPDATE signal_notifications SET {col}=1, trade_closed=? WHERE signal_id=?",
            (closed_val, signal_id)
        )
        conn.commit()
    except Exception as e:
        log.error(f"mark_notification_sent error: {e}")
    finally:
        conn.close()

# ─── Pass 1 audit fixes: candle-window replay + intra-candle ambiguity ───────
# FIX (Pass 1 audit, high severity): the old code fetched exactly ONE closed
# 1m candle per cycle and judged Entry/TP1/TP2/SL only against it. If a scan
# cycle takes longer than 60s — near-certain with TOP_N_COINS symbols, rate
# limiting, and Telegram calls — any candle in between was never fetched, so
# a TP/SL touch inside it was silently lost forever: the trade would either
# sit open indefinitely or get resolved later against an unrelated price.
# Fix: fetch a bounded window of recent closed candles and replay them in
# chronological order every cycle (idempotent — replaying already-applied
# candles reproduces the same transitions and is a no-op).
OUTCOME_CANDLE_LOOKBACK = 30  # minutes of 1m candles re-checked per cycle
LONG_RUNNING_TRADE_HOURS = 8  # bug audit #6: trades still ENTERED/TP1_HIT past this
                               # age (but under the 24h hard expiry) get a logged
                               # keep-monitoring vs recommend-closing advisory,
                               # reusing the existing regime/volatility analysis
                               # functions. Informational only — never changes
                               # status, never adds a DB field/state.
# BUG FIX (monitoring audit): candles closing at or before a signal's opened_at
# must never be replayed as post-signal outcome evidence (see the filter in
# update_trade_outcomes below). CLOCK_SKEW_BUFFER_MS absorbs ordinary NTP-level
# drift between the bot server's clock (source of opened_at) and Binance's
# server clock (source of candle close_time) so that drift can never let a
# pre-signal candle slip back in at the boundary.
CLOCK_SKEW_BUFFER_MS = 2000  # 2 seconds

def _resolve_intracandle_priority(open_price: float, candidates: list[tuple[float, str]]) -> str:
    """
    FIX (Pass 1 audit, high severity): a single candle's high/low range can
    straddle more than one key level at once (Entry+SL, or TP1+TP2+SL on a
    wide stop-hunt wick). OHLC data alone can't prove which was actually
    touched first. The previous code always resolved these ties in the same
    fixed order (e.g. TP2 checked before SL, Entry checked before SL/TP2),
    which silently biased every ambiguous candle toward the best-looking
    outcome and inflated reported win-rate/PnL — particularly relevant here
    since the Stop Hunter strategy specifically targets this kind of wick.
    We instead assume whichever level sits nearest the candle's OPEN price
    was reached first (price moves away from its open), the standard
    non-repainting approximation used by backtest engines restricted to
    OHLC data.
    """
    candidates = sorted(candidates, key=lambda c: abs(open_price - c[0]))
    return candidates[0][1]

def _simulate_outcome_step(local: dict, candle: dict, candle_iso: str,
                           entry: float, stop: float, tp1: float, tp2: float,
                           direction: str, risk_pct: float) -> Optional[tuple[str, Optional[float]]]:
    """
    Advance one trade's local state by a single candle. Mutates `local`
    (status, tp1_hit, tp2_hit, entered_at, tp1_hit_at, tp2_hit_at, closed_at,
    close_price, pnl_percent, rr_multiple) in place. Returns
    (event_name, pnl_for_notification) if a transition happened this candle,
    else None. 24h expiry is judged by the caller on wall-clock time after all
    candles in the window are replayed — it doesn't belong to any one candle.
    """
    high, low, open_ = candle["high"], candle["low"], candle["open"]
    status = local["status"]

    entry_touched = low <= entry <= high
    hit_tp2 = low <= tp2 <= high
    hit_tp1 = low <= tp1 <= high
    hit_sl  = low <= stop <= high

    if status in ("PENDING", "OPEN"):
        candidates = []
        if entry_touched:
            candidates.append((entry, "ENTER"))
        if hit_tp2:
            candidates.append((tp2, "MISS"))
        if hit_sl:
            candidates.append((stop, "MISS"))
        if not candidates:
            return None
        winner = _resolve_intracandle_priority(open_, candidates)
        if winner == "ENTER":
            local["status"] = "ENTERED"
            local["entered_at"] = candle_iso
            return "entry", None
        local["status"] = "MISSED"
        local["closed_at"] = candle_iso
        return "missed", None

    elif status == "ENTERED":
        candidates = []
        if hit_tp2:
            candidates.append((tp2, "TP2"))
        if hit_tp1 and not local["tp1_hit"]:
            candidates.append((tp1, "TP1"))
        if hit_sl:
            candidates.append((stop, "SL"))
        if not candidates:
            return None
        winner = _resolve_intracandle_priority(open_, candidates)
        if winner == "TP2":
            tp2_pnl = ((tp2 - entry) / entry * 100) if direction == "BUY" else ((entry - tp2) / entry * 100)
            local["tp1_hit"], local["tp2_hit"] = 1, 1
            local["tp1_hit_at"] = local["tp2_hit_at"] = candle_iso
            local["status"] = "TP2_HIT"
            local["closed_at"] = candle_iso
            local["close_price"] = candle["close"]
            local["pnl_percent"] = round(tp2_pnl, 2)
            local["rr_multiple"] = round((tp2_pnl / risk_pct) if risk_pct > 0 else 0, 3)
            return "tp2", local["pnl_percent"]
        if winner == "TP1":
            local["tp1_hit"] = 1
            local["tp1_hit_at"] = candle_iso
            local["status"] = "TP1_HIT"
            tp1_pnl = ((tp1 - entry) / entry * 100) if direction == "BUY" else ((entry - tp1) / entry * 100)
            return "tp1", round(tp1_pnl, 2)
        # winner == "SL"
        # FIX (Pass 1 audit, medium severity): previously priced off whatever
        # the last fetched candle's close happened to be, not off the actual
        # stop level — so SL losses were measured on a different basis than
        # TP wins (always priced at the exact target), skewing avg RR, profit
        # factor and expectancy. Price both consistently at the exact level.
        sl_pnl = ((stop - entry) / entry * 100) if direction == "BUY" else ((entry - stop) / entry * 100)
        local["status"] = "SL_HIT"
        local["closed_at"] = candle_iso
        local["close_price"] = candle["close"]
        local["pnl_percent"] = round(sl_pnl, 2)
        local["rr_multiple"] = round((sl_pnl / risk_pct) if risk_pct > 0 else 0, 3)
        return "sl_before_tp1", local["pnl_percent"]

    elif status == "TP1_HIT":
        candidates = []
        if hit_tp2:
            candidates.append((tp2, "TP2"))
        if hit_sl:
            candidates.append((stop, "SL"))
        if not candidates:
            return None
        winner = _resolve_intracandle_priority(open_, candidates)
        if winner == "TP2":
            tp2_pnl = ((tp2 - entry) / entry * 100) if direction == "BUY" else ((entry - tp2) / entry * 100)
            local["tp2_hit"] = 1
            local["tp2_hit_at"] = candle_iso
            local["status"] = "TP2_HIT"
            local["closed_at"] = candle_iso
            local["close_price"] = candle["close"]
            local["pnl_percent"] = round(tp2_pnl, 2)
            local["rr_multiple"] = round((tp2_pnl / risk_pct) if risk_pct > 0 else 0, 3)
            return "tp2", local["pnl_percent"]
        # winner == "SL": TP1 already banked, price returned to stop (break-even
        # style close) — priced at TP1, unchanged from previous behaviour.
        tp1_pnl = ((tp1 - entry) / entry * 100) if direction == "BUY" else ((entry - tp1) / entry * 100)
        local["status"] = "TP1_TOUCHSL"
        local["closed_at"] = candle_iso
        local["close_price"] = candle["close"]
        local["pnl_percent"] = round(tp1_pnl, 2)
        local["rr_multiple"] = round((tp1_pnl / risk_pct) if risk_pct > 0 else 0, 3)
        return "sl_after_tp1", local["pnl_percent"]

    return None

async def _log_long_running_trade_advisory(session: aiohttp.ClientSession, row: sqlite3.Row,
                                            local: dict, age_hours: float) -> None:
    """
    Bug audit fix #6 (long-running trades): a trade that's still ENTERED /
    TP1_HIT well past LONG_RUNNING_TRADE_HOURS but hasn't yet hit the 24h
    hard mark-to-market expiry gets a logged advisory on whether to keep
    monitoring it or recommend closing it manually. This reuses the existing
    trend/volatility analysis functions (calc_adx, calc_atr_regime,
    is_volatility_abnormal) already used elsewhere in the bot — it does not
    add any new status, DB field, or architecture, and it never changes the
    trade's status itself; monitoring continues exactly as before via the
    normal state machine.
    """
    try:
        htf_candles = await get_candles(session, row["symbol"], "1h", limit=80)
        if len(htf_candles) < 65:
            log.debug(
                f"Long-running trade advisory skipped for signal_id={row['id']} "
                f"({row['symbol']}): not enough 1h history yet"
            )
            return
        adx = calc_adx(htf_candles)
        atr_regime = calc_atr_regime(htf_candles)
        vol_abnormal = is_volatility_abnormal(htf_candles)
        weak_or_stalled = adx < 20 or atr_regime == "LOW" or vol_abnormal
        recommendation = "RECOMMEND CLOSING (weak/stalled trend or abnormal volatility)" \
            if weak_or_stalled else "KEEP MONITORING (trend/volatility still supportive)"
        log.info(
            f"LONG-RUNNING TRADE ADVISORY | signal_id={row['id']} signal_uid={row['signal_uid']} "
            f"{row['symbol']} status={local['status']} age={age_hours:.1f}h adx={adx:.1f} "
            f"atr_regime={atr_regime} vol_abnormal={vol_abnormal} -> {recommendation}"
        )
    except Exception as e:
        log.debug(f"Long-running trade advisory failed for {row['symbol']}: {e}")

async def update_trade_outcomes(session: aiohttp.ClientSession):
    """
    Patch #2/#4/#6: Full trade state machine — VALIDATED TRANSITIONS ONLY.

    ══════════ TESTED SCENARIOS ══════════
    Scenario A — Normal TP2 win:
      PENDING → ENTERED → TP1_HIT → TP2_HIT (closed)
    Scenario B — SL after TP1 (break-even):
      PENDING → ENTERED → TP1_HIT → TP1_TOUCHSL (closed, PnL = TP1)
    Scenario C — SL without TP1:
      PENDING → ENTERED → SL_HIT (closed, loss)
    Scenario D — Never entered, price expired:
      PENDING → EXPIRED (closed, no PnL)
    Scenario E — Price went to TP/SL before Entry:
      PENDING → MISSED (closed, not counted in stats)
    Scenario F — Direct TP2 without TP1 separately:
      PENDING → ENTERED → TP2_HIT (TP1 implicitly set, closed)

    DUPLICATE-COUNT PREVENTION:
    - DB query uses WHERE status IN ('PENDING','OPEN','ENTERED','TP1_HIT')
      → terminal states (TP2_HIT, SL_HIT, MISSED, EXPIRED, TP1_TOUCHSL) never re-processed.
    - Each row produces at most ONE new_status update per call.
    - _db_lock ensures only one coroutine writes at a time.
    - can_signal() deduplicates signals by (symbol + strategy + direction + cooldown window).

    INVALID TRANSITIONS BLOCKED:
    - TP2_HIT → anything  (terminal, never in WHERE clause)
    - SL_HIT → anything   (terminal)
    - EXPIRED → anything  (terminal)
    - MISSED → anything   (terminal)
    - TP1_HIT → ENTERED   (no backward transitions)
    - ENTERED → PENDING   (no backward transitions)

    Legacy status aliases kept for backward compat: TP1=TP1_HIT, TP2=TP2_HIT, SL=SL_HIT.

    PASS 1 AUDIT FIXES (see helper functions above for full rationale):
    - Replays a bounded window of recent closed candles (OUTCOME_CANDLE_LOOKBACK)
      instead of only the single latest one, so a slow scan cycle can no longer
      silently skip a candle that touched Entry/TP/SL.
    - Same-candle ambiguity (a candle that touches more than one key level) is
      resolved by proximity to the candle's open instead of a fixed check order,
      removing a systematic optimistic bias.
    - SL fills are priced at the exact stop level, same convention as TP fills
      (previously priced at whatever the last close happened to be).
    - An ENTERED/TP1_HIT trade that hits the 24h timeout is now mark-to-market
      closed with real PnL, instead of silently closing with no PnL at all.
    """
    async with _db_lock:
        conn = get_db_connection()
        try:
            open_rows = conn.execute("""
                SELECT s.id, s.symbol, s.direction, s.entry, s.stop, s.tp1, s.tp2, s.opened_at, s.signal_uid,
                       r.status, r.tp1_hit, r.tp2_hit, r.entered_at, r.price_fail_count
                FROM signals s JOIN results r ON r.signal_id = s.id
                WHERE r.status IN ('PENDING','OPEN','ENTERED','TP1_HIT')
            """).fetchall()
        finally:
            conn.close()

        if not open_rows:
            return load_trades()

        now = datetime.now(timezone.utc)
        updates = []              # (signal_id, new_status, entered_at, tp1_hit, tp2_hit, tp1_hit_at, tp2_hit_at,
                                  #  closed_at, close_price, pnl_pct, rr, price_fail_count, close_reason)
        notification_events = []  # (signal_id, event, pnl_for_notification, symbol, direction) — chronological
        # FIX (bug audit): جلوگیری از orphan trades هنگام شکست مکرر fetch قیمت
        fail_count_updates = []   # (signal_id, new_fail_count) — فقط افزایش شمارنده، بدون تغییر status
        fail_count_resets  = []   # (signal_id,) — شمارنده را صفر کن چون fetch موفق بود

        for row in open_rows:
            opened = datetime.fromisoformat(row["opened_at"])
            age_hours = (now - opened).total_seconds() / 3600
            price_fail_count = row["price_fail_count"] if "price_fail_count" in row.keys() and row["price_fail_count"] is not None else 0

            # LOGGING (bug audit #7): monitoring-cycle start for this signal.
            # signal_uid included for full lifecycle traceability (Signal ID feature).
            log.debug(
                f"Monitoring started | signal_id={row['id']} signal_uid={row['signal_uid']} {row['symbol']} "
                f"status={row['status']} age={age_hours:.2f}h"
            )

            try:
                # FIX (Pass 1 audit): fetch a bounded window, not a single candle —
                # see OUTCOME_CANDLE_LOOKBACK comment above.
                candles = await get_candles(session, row["symbol"], "1m", limit=OUTCOME_CANDLE_LOOKBACK)
                if not candles:
                    raise ValueError(f"Empty candle response for {row['symbol']}")
                # BUG FIX (root cause of false ENTRY TOUCHED / TRADE MISSED):
                # OUTCOME_CANDLE_LOOKBACK pulls the last N minutes of 1m candles
                # unconditionally, with no floor on how far back that window can
                # reach relative to when THIS signal was actually created. On the
                # very first monitoring pass after a signal (as little as one
                # cycle later), that window can still contain candles from
                # BEFORE opened_at — including the exact sweep/breakout candle a
                # strategy used to generate the signal in the first place, since
                # entry is frequently set to that same candle's own close. Replaying
                # those pre-signal candles through the state machine judges price
                # action that happened before the trade existed as if it were new
                # movement, producing ENTRY TOUCHED or TRADE MISSED (target/stop
                # reached "before" entry) for moves the trade was never actually
                # exposed to. Only candles that closed after the signal was opened
                # can count toward its outcome.
                # HARDENING (audit request #1/#4): opened_at is stamped from the
                # bot server's own clock (datetime.now(timezone.utc)), while
                # candle close_time comes from Binance's server clock. Any small
                # clock drift between the two (a few seconds of NTP skew is
                # normal and expected) could let a candle that Binance closed
                # slightly before the signal existed slip past a bare
                # `close_time > opened_ms` check if our local clock is even a
                # couple seconds slow. A fixed safety buffer absorbs that skew.
                # This only ever makes the cutoff MORE conservative (delays
                # counting a legitimate boundary candle by at most one extra
                # cycle) — it can never cause a real Entry/TP/SL to be missed
                # or double counted, since the SAME candle is simply re-checked
                # on the next cycle once it clears the buffer.
                opened_ms = opened.timestamp() * 1000
                candles = [c for c in candles
                           if c.get("close_time") and c["close_time"] > opened_ms + CLOCK_SKEW_BUFFER_MS]
                if not candles:
                    continue
            except Exception as e:
                log.debug(f"Price fetch failed for {row['symbol']}: {e}")
                new_fail_count = price_fail_count + 1
                if new_fail_count >= MAX_PRICE_FETCH_FAILURES:
                    # FIX (bug audit): فیل‌سیف — بعد از حداکثر شکست متوالی، معامله را
                    # اجباراً به EXPIRED می‌بریم تا برای همیشه orphan/stuck نماند.
                    log.warning(
                        f"signal_id={row['id']} signal_uid={row['signal_uid']} ({row['symbol']}): "
                        f"{new_fail_count} consecutive price-fetch failures — force-closing as "
                        f"EXPIRED (price_unavailable)"
                    )
                    updates.append((
                        row["id"], "EXPIRED", row["entered_at"], row["tp1_hit"], row["tp2_hit"], None, None,
                        now.isoformat(), None, None, None, new_fail_count, "price_unavailable"
                    ))
                else:
                    fail_count_updates.append((row["id"], new_fail_count))
                continue

            if price_fail_count != 0:
                fail_count_resets.append((row["id"],))

            entry, direction = row["entry"], row["direction"]
            risk_pct = abs(entry - row["stop"]) / entry * 100

            local = {
                "status": row["status"], "tp1_hit": row["tp1_hit"], "tp2_hit": row["tp2_hit"],
                "entered_at": row["entered_at"], "tp1_hit_at": None, "tp2_hit_at": None,
                "closed_at": None, "close_price": None, "pnl_percent": None, "rr_multiple": None,
            }
            start_status = local["status"]
            TERMINAL = ("TP2_HIT", "SL_HIT", "TP1_TOUCHSL", "MISSED")

            # FIX (Pass 1 audit): replay every closed candle in the fetched window,
            # oldest to newest, instead of only the most recent one.
            for candle in candles:
                candle_iso = (
                    datetime.fromtimestamp(candle["close_time"] / 1000, tz=timezone.utc).isoformat()
                    if candle.get("close_time") else now.isoformat()
                )
                # FIX (Pass 1 audit): a single wide candle can span more than one
                # transition at once (e.g. ENTERED -> TP1_HIT -> TP2_HIT all
                # within one candle's range). Re-apply the step function against
                # the SAME candle until it stops advancing, so a multi-level move
                # inside one candle fully resolves this cycle instead of
                # trickling out one state per cycle.
                for _ in range(4):  # state machine has at most 4 forward hops
                    step = _simulate_outcome_step(
                        local, candle, candle_iso, entry, row["stop"], row["tp1"], row["tp2"], direction, risk_pct
                    )
                    if not step:
                        break
                    event, notif_pnl = step
                    notification_events.append((row["id"], event, notif_pnl, row["symbol"], direction))
                    # LOGGING (bug audit #7): explicit per-event log at the moment
                    # each transition is detected (Entry/TP1/TP2/SL/Missed).
                    log.info(
                        f"{event.upper()} detected | signal_id={row['id']} signal_uid={row['signal_uid']} "
                        f"{row['symbol']} {direction} candle_close={candle_iso} new_status={local['status']}"
                        + (f" pnl={notif_pnl:.2f}%" if notif_pnl is not None else "")
                    )
                    if local["status"] in TERMINAL:
                        break
                if local["status"] in TERMINAL:
                    break  # this signal is fully resolved — later candles in the window don't matter

            # Bug audit #6 (long-running trades): still-open positions that
            # haven't resolved this cycle but have been running a long time
            # get a logged keep/close advisory, reusing existing analysis
            # functions. Purely informational — doesn't affect `local` at all.
            if local["status"] in ("ENTERED", "TP1_HIT") and LONG_RUNNING_TRADE_HOURS <= age_hours < 24:
                await _log_long_running_trade_advisory(session, row, local, age_hours)

            # Wall-clock expiry is judged once, after replay, using real "now" —
            # it isn't a property of any single historical candle.
            if local["status"] not in TERMINAL and age_hours >= 24:
                if local["status"] in ("PENDING", "OPEN"):
                    # Never entered → no market exposure → no PnL to report.
                    local["status"] = "EXPIRED"
                    local["closed_at"] = now.isoformat()
                elif local["status"] == "ENTERED":
                    # FIX (Pass 1 audit, high severity): an ENTERED trade was a
                    # live position when it timed out. The previous code
                    # force-closed it with NO pnl_percent/rr_multiple at all,
                    # which made calc_strategy_stats() silently drop its real
                    # gain/loss from win-rate, profit factor, and drawdown —
                    # a live position simply vanished from the numbers. Mark it
                    # to market at the latest available close instead.
                    last_close = candles[-1]["close"]
                    mtm_pnl = ((last_close - entry) / entry * 100) if direction == "BUY" \
                              else ((entry - last_close) / entry * 100)
                    local["status"] = "EXPIRED"
                    local["closed_at"] = now.isoformat()
                    local["close_price"] = last_close
                    local["pnl_percent"] = round(mtm_pnl, 2)
                    local["rr_multiple"] = round((mtm_pnl / risk_pct) if risk_pct > 0 else 0, 3)
                # else: local["status"] == "TP1_HIT" — FIX (root cause): TP1_HIT
                # is an already-successful outcome, not a "no result" trade. The
                # previous code fell into the same branch as ENTERED and
                # overwrote a real TP1 win with EXPIRED at the 24h mark, which
                # then rendered as "No Result" in reports/history/backtest and
                # permanently discarded the TP1 result. TP1_HIT trades are left
                # untouched here: status/closed_at/pnl stay as they are, so the
                # TP1 result is preserved and the row stays in the
                # PENDING/OPEN/ENTERED/TP1_HIT re-processing query to still
                # catch a later TP2/SL close.

            if local["status"] != start_status:
                updates.append((
                    row["id"], local["status"], local["entered_at"], local["tp1_hit"], local["tp2_hit"],
                    local["tp1_hit_at"], local["tp2_hit_at"], local["closed_at"], local["close_price"],
                    local["pnl_percent"], local["rr_multiple"],
                    0, None  # price fetch succeeded → reset fail counter, no close_reason
                ))

        if updates:
            conn = get_db_connection()
            try:
                for u in updates:
                    conn.execute(
                        "UPDATE results SET status=?, entered_at=?, tp1_hit=?, tp2_hit=?, "
                        "tp1_hit_at=?, tp2_hit_at=?, closed_at=?, close_price=?, "
                        "pnl_percent=?, rr_multiple=?, price_fail_count=?, close_reason=? WHERE signal_id=?",
                        (u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[0])
                    )
                conn.commit()
                # LOGGING (bug audit #7): confirm the DB write actually happened,
                # with per-row new status for traceability.
                log.info(
                    f"Database update | {len(updates)} result row(s) updated: "
                    + ", ".join(f"signal_id={u[0]}->{u[1]}" for u in updates)
                )
            finally:
                conn.close()

        # FIX (bug audit): شمارنده شکست fetch قیمت را بدون تغییر status به‌روزرسانی می‌کنیم
        if fail_count_updates or fail_count_resets:
            conn = get_db_connection()
            try:
                for signal_id, new_fail_count in fail_count_updates:
                    conn.execute(
                        "UPDATE results SET price_fail_count=? WHERE signal_id=?",
                        (new_fail_count, signal_id)
                    )
                for (signal_id,) in fail_count_resets:
                    conn.execute(
                        "UPDATE results SET price_fail_count=0 WHERE signal_id=?",
                        (signal_id,)
                    )
                conn.commit()
            finally:
                conn.close()

        # FIX (Pass 2 audit, medium severity): the notification-sending loop and
        # the final load_trades() read used to run INSIDE this lock, meaning
        # _db_lock stayed held for the entire duration of every outbound
        # Telegram call for every closed trade this cycle. update_trade_outcomes
        # is invoked both by scan_loop() and by daily_report_loop() as two
        # independent concurrent background tasks (see main()) — if one call is
        # slow to reach Telegram (rate limiting, network hiccup, Telegram
        # downtime), the other would be blocked from even reading/updating the
        # DB for that whole time, even though there's no data-race reason to
        # block it. The lock only needs to cover the DB read + replay + DB
        # write section above; network I/O is moved outside it below.

    # ── ارسال reply اعلان‌های Entry/TP1/TP2/SL/MISSED، به ترتیب زمانی وقوع ──
    # FIX (Pass 1 audit): now driven off notification_events (built during
    # replay, in chronological order) instead of `updates`, so a signal that
    # went e.g. ENTERED → TP1_HIT → TP2_HIT across several replayed candles
    # in one cycle still gets every notification, in the right order,
    # instead of only its single final state.
    for signal_id, event, notif_pnl, symbol, direction in notification_events:
        try:
            if event == "entry":
                await send_outcome_reply(session, signal_id, symbol, direction, "entry", None)
            elif event == "missed":
                await send_outcome_reply(session, signal_id, symbol, direction, "missed", None)
            elif event == "tp1":
                await send_outcome_reply(session, signal_id, symbol, direction, "tp1", notif_pnl)
            elif event == "tp2":
                await send_outcome_reply(session, signal_id, symbol, direction, "tp2", notif_pnl)
            elif event == "sl_before_tp1":
                await send_outcome_reply(session, signal_id, symbol, direction, "sl_before_tp1", notif_pnl)
            elif event == "sl_after_tp1":
                await send_outcome_reply(session, signal_id, symbol, direction, "sl_after_tp1", notif_pnl)
        except Exception as e:
            log.error(f"Outcome reply error for signal_id={signal_id}: {e}")

    return load_trades()

def get_trades_for_report(trades: list, report_date_str: str) -> list:
    """Redesigned Daily Report (feature): returns every signal that belongs in
    `report_date_str`'s (Asia/Tehran calendar day) report — ordered by SEND
    time (opened_at), never by closing time, per spec. Includes:
      - every signal opened that Tehran calendar day, in ANY status
        (pending/entered/closed) — pending ones are shown with an empty
        result instead of being hidden.
      - signals opened on an earlier Tehran day that CLOSED on this Tehran
        day, so a trade that spans the Tehran midnight boundary still
        appears (tagged Opened/Closed Today in the renderer), instead of
        vanishing from every report entirely.
    CORRECTION: opened_at/closed_at are stored as UTC timestamps, but the
    date boundary used here is now the Asia/Tehran calendar day (via
    tehran_date_of_iso()), not the UTC calendar day.
    """
    result = []
    for t in trades:
        opened_date = tehran_date_of_iso(t.get("opened_at"))
        closed_date = tehran_date_of_iso(t.get("closed_at")) if t.get("closed_at") else None
        if opened_date == report_date_str or closed_date == report_date_str:
            result.append(t)
    result.sort(key=lambda t: t.get("opened_at") or "")
    return result

def build_daily_report(trades: list, report_date_str: str) -> str:
    """Redesigned Daily Report (feature). One message for the whole day:
      - order = send time (opened_at), never reordered by closing time.
      - every signal from that day stays visible even while still pending
        (shown with an empty/pending result instead of being hidden).
      - a signal opened the previous day but closed today keeps its original
        place (by send time) and gets an "Opened: YYYY-MM-DD" / "Closed Today"
        note instead of being silently dropped or duplicated.
      - a separator line between every signal (not just once at the bottom).
    This message is meant to be sent once per day and then edited in place as
    results come in — see sync_daily_report(), which is what actually decides
    when to send vs. edit; this function only builds the text.
    """
    todays = get_trades_for_report(trades, report_date_str)

    OPEN_RAW_STATUSES  = {"OPEN", "PENDING", "ENTERED", "TP1_HIT"}
    WIN_STATUSES   = {"TP1", "TP2", "TP1_HIT", "TP2_HIT", "TP1_TOUCHSL"}
    LOSS_STATUSES  = {"SL", "SL_HIT"}
    CLOSED_STATUSES = WIN_STATUSES | LOSS_STATUSES

    RESULT_LABEL = {
        "PENDING": "⏳ PENDING",
        "ENTERED": "🟢 ACTIVE — waiting TP/SL",
        "TP1": "✅ TP1 TOUCHED", "TP1_HIT": "✅ TP1 TOUCHED",
        "TP1_TOUCHSL": "🔄 TP1 THEN BREAK EVEN",
        "TP2": "✅✅ TP2 TOUCHED", "TP2_HIT": "✅✅ TP2 TOUCHED",
        "SL": "❌ STOP HIT", "SL_HIT": "❌ STOP HIT",
        "EXPIRED": "⏳ NO RESULT (expired)",
        "MISSED": "⚠️ MISSED (entry not touched)",
    }

    header = ["📊 <b>AFEE TRADER Daily Report</b>", f"🗓 Date: {report_date_str}", ""]

    if not todays:
        return "\n".join(header) + "No signals for this day yet.\n\n#AFEE_DAILY_REPORT"

    lines = list(header)
    total_pnl = 0.0
    win_count = 0
    closed_count = 0

    for t in todays:
        raw_st = t.get("raw_status", t.get("status", ""))
        opened_date = tehran_date_of_iso(t.get("opened_at"))
        closed_date = tehran_date_of_iso(t.get("closed_at")) if t.get("closed_at") else None
        direction_label = "🟢 LONG" if t.get("direction") == "BUY" else "🔴 SHORT" if t.get("direction") == "SELL" else "—"
        signal_uid = t.get("signal_uid")

        lines.append(f"#{signal_uid}" if signal_uid else f"#{t.get('id', '?')}")
        lines.append(f"💎 {t['symbol']}")
        lines.append(f"{direction_label} | {t['strategy']}")

        if raw_st in OPEN_RAW_STATUSES:
            lines.append(RESULT_LABEL.get(raw_st, "⏳ PENDING"))
        else:
            pnl = t.get("pnl_percent", 0) or 0
            total_pnl += pnl
            pnl_sign = "🟢+" if pnl >= 0 else "🔴"
            lines.append(RESULT_LABEL.get(raw_st, raw_st))
            lines.append(f"{pnl_sign}{abs(pnl):.2f}%")
            if raw_st in CLOSED_STATUSES:
                closed_count += 1
                if raw_st in WIN_STATUSES:
                    win_count += 1

        # Cross-day trade: opened before this report's day, closed on it.
        if opened_date and opened_date != report_date_str:
            lines.append(f"Opened: {opened_date}")
            if closed_date == report_date_str:
                lines.append("Closed Today")

        lines.append("━━━━━━━━━━━━")

    win_rate = (win_count / closed_count * 100) if closed_count > 0 else 0
    total_sign = "🟢+" if total_pnl >= 0 else "🔴"
    lines.append(f"📈 <b>Total PnL (closed):</b> {total_sign}{abs(total_pnl):.2f}%")
    lines.append(f"🎯 <b>Win Rate (TP/SL):</b> {win_rate:.1f}% ({win_count} of {closed_count})")
    lines.append(f"📦 <b>Total signals today:</b> {len(todays)}")
    lines.append("")
    lines.append("#AFEE_DAILY_REPORT")

    return "\n".join(lines)

def build_period_report(trades: list, report_title: str, period_label: str, hashtag: str) -> str:
    """Weekly/Monthly Signal Performance Report — reuses build_daily_report()'s
    EXACT visual layout (header shape, per-signal block format/emojis,
    separator line, footer stats lines) verbatim; only the header
    title/period text and the trailing hashtag differ, per spec. This is a
    separate function (not a modification of build_daily_report()) so the
    Daily Report's own code/behavior is left completely untouched.

    Unlike build_daily_report(), `trades` here is expected to already be
    scoped to the report's period by the caller (see
    _load_closed_trades_by_range(), which filters by closed_at within
    [start, end] at the DB level) — so no per-signal day-string filtering
    is re-applied; every trade passed in is included, ordered by send time
    (opened_at), same as the Daily Report.
    """
    ordered = sorted(trades, key=lambda t: t.get("opened_at") or "")

    OPEN_RAW_STATUSES  = {"OPEN", "PENDING", "ENTERED", "TP1_HIT"}
    WIN_STATUSES   = {"TP1", "TP2", "TP1_HIT", "TP2_HIT", "TP1_TOUCHSL"}
    LOSS_STATUSES  = {"SL", "SL_HIT"}
    CLOSED_STATUSES = WIN_STATUSES | LOSS_STATUSES

    RESULT_LABEL = {
        "PENDING": "⏳ PENDING",
        "ENTERED": "🟢 ACTIVE — waiting TP/SL",
        "TP1": "✅ TP1 TOUCHED", "TP1_HIT": "✅ TP1 TOUCHED",
        "TP1_TOUCHSL": "🔄 TP1 THEN BREAK EVEN",
        "TP2": "✅✅ TP2 TOUCHED", "TP2_HIT": "✅✅ TP2 TOUCHED",
        "SL": "❌ STOP HIT", "SL_HIT": "❌ STOP HIT",
        "EXPIRED": "⏳ NO RESULT (expired)",
        "MISSED": "⚠️ MISSED (entry not touched)",
    }

    header = [f"📊 <b>{report_title}</b>", f"🗓 {period_label}", ""]

    if not ordered:
        return "\n".join(header) + f"No signals for this period.\n\n{hashtag}"

    lines = list(header)
    total_pnl = 0.0
    win_count = 0
    closed_count = 0

    for t in ordered:
        raw_st = t.get("raw_status", t.get("status", ""))
        opened_date = tehran_date_of_iso(t.get("opened_at"))
        direction_label = "🟢 LONG" if t.get("direction") == "BUY" else "🔴 SHORT" if t.get("direction") == "SELL" else "—"
        signal_uid = t.get("signal_uid")

        lines.append(f"#{signal_uid}" if signal_uid else f"#{t.get('id', '?')}")
        lines.append(f"💎 {t['symbol']}")
        lines.append(f"{direction_label} | {t['strategy']}")

        if raw_st in OPEN_RAW_STATUSES:
            lines.append(RESULT_LABEL.get(raw_st, "⏳ PENDING"))
        else:
            pnl = t.get("pnl_percent", 0) or 0
            total_pnl += pnl
            pnl_sign = "🟢+" if pnl >= 0 else "🔴"
            lines.append(RESULT_LABEL.get(raw_st, raw_st))
            lines.append(f"{pnl_sign}{abs(pnl):.2f}%")
            if raw_st in CLOSED_STATUSES:
                closed_count += 1
                if raw_st in WIN_STATUSES:
                    win_count += 1

        # Multi-day period (unlike the single-day Daily Report), so always
        # show the opened date for context rather than only on cross-day
        # trades.
        if opened_date:
            lines.append(f"Opened: {opened_date}")

        lines.append("━━━━━━━━━━━━")

    win_rate = (win_count / closed_count * 100) if closed_count > 0 else 0
    total_sign = "🟢+" if total_pnl >= 0 else "🔴"
    lines.append(f"📈 <b>Total PnL (closed):</b> {total_sign}{abs(total_pnl):.2f}%")
    lines.append(f"🎯 <b>Win Rate (TP/SL):</b> {win_rate:.1f}% ({win_count} of {closed_count})")
    lines.append(f"📦 <b>Total signals:</b> {len(ordered)}")
    lines.append("")
    lines.append(hashtag)

    return "\n".join(lines)



# Telegram error substrings that mean the message genuinely no longer exists
# / can never be edited again — anything else (timeouts, rate limits, a
# dropped connection) is treated as transient and must NOT trigger sending a
# brand-new report message.
_EDIT_GONE_MARKERS = (
    "message to edit not found",
    "message can't be edited",
    "message_id_invalid",
    "chat not found",
    "bot was blocked",
    "message to edit not found",
)

async def _publish_daily_report_for_day(session: aiohttp.ClientSession, state: dict,
                                         trades: list, report_date_str: str):
    """Broadcasts ONE brand-new Daily Report message for `report_date_str` (an
    Asia/Tehran calendar day that has already fully ended) and remembers its
    message id(s) so later cross-day result updates can edit it in place.
    Only ever called from check_daily_report_rollover() below, at the moment
    the Tehran date rolls over — never mid-day."""
    try:
        report_text = build_daily_report(trades, report_date_str)
    except Exception as e:
        log.error(f"_publish_daily_report_for_day: build_daily_report failed: {e}")
        return
    async with _daily_report_sync_lock:
        try:
            sent_ids = await broadcast_signal(session, report_text, state)
            if sent_ids:
                state["daily_report_msg_ids"] = sent_ids
                state["daily_report_msg_date"] = report_date_str
                state["daily_report_last_text"] = report_text
                save_state(state)
                log.info(f"Daily Report published for {report_date_str}.")
        except Exception as e:
            log.error(f"_publish_daily_report_for_day: send failed: {e}")


async def check_daily_report_rollover(session: aiohttp.ClientSession, state: dict, trades: list):
    """Publishes the automatic Daily Report exactly once per day, at the
    00:00 Asia/Tehran rollover. CORRECTION: the report now represents one
    COMPLETE Asia/Tehran calendar day (00:00 -> 23:59:59.999 Tehran), not a
    UTC calendar day — see tehran_date_of_iso(), get_trades_for_report(),
    build_daily_report(). Both the WALL-CLOCK MOMENT this function decides
    to publish AND the day boundary of what gets published are the Asia/
    Tehran midnight boundary (state["daily_report_collecting_day"] tracks
    the Tehran calendar date this function is currently collecting for):
      - The bot may start at any time of day. The very first time this is
        called (state["daily_report_collecting_day"] is still unset), it
        silently starts tracking the current Tehran day and returns WITHOUT
        publishing anything — no report on startup, no report on the first
        signal.
      - While the Tehran date hasn't changed since the last check, this is a
        pure no-op: no Telegram call at all, data is simply kept internally
        (via the DB, as normal) for whenever the day's report is built.
      - The instant the Tehran date rolls over (00:00 Asia/Tehran), the
        Tehran calendar day that was just being collected (`collecting_day`
        itself — the complete Tehran day that has just ended) gets exactly
        one Daily Report published (see _publish_daily_report_for_day), and
        tracking immediately moves on to the new Tehran day. This also
        naturally prevents duplicate Daily Reports after a bot restart or
        repeated scheduler cycles: the state flip happens before the publish
        call, and re-checks within the same Tehran day are simple no-ops.
    """
    today_tehran = tehran_date_str()
    collecting_day = state.get("daily_report_collecting_day")

    if collecting_day is None:
        # First check ever (bot just started, possibly mid-day) — begin
        # silently collecting; publish nothing.
        state["daily_report_collecting_day"] = today_tehran
        save_state(state)
        return

    if collecting_day == today_tehran:
        return  # Still the same Tehran day in progress — nothing to publish yet.

    # The Tehran date has advanced: publish the Tehran calendar day that was
    # just being collected (`collecting_day`) — that IS the complete Tehran
    # day that has just ended. Move tracking to the new Tehran day first (so
    # a slow/failed publish can never cause this to fire twice), then publish.
    ended_day = collecting_day
    state["daily_report_collecting_day"] = today_tehran
    save_state(state)
    await _publish_daily_report_for_day(session, state, trades, ended_day)


async def daily_report_midnight_scheduler(session: aiohttp.ClientSession, state: dict):
    """Dedicated, self-contained clock for the automatic Daily Report — this
    is what actually guarantees publication happens AT 00:00:00 Asia/Tehran,
    not "whenever the next scan/signal/user action happens to run". It never
    waits on scan_loop, a new signal, or any Telegram interaction: it sleeps
    for exactly the number of seconds until the next Asia/Tehran midnight,
    wakes up, and immediately runs the rollover check (which builds +
    publishes the Asia/Tehran day that just ended's report — see
    check_daily_report_rollover()). Even if the bot is completely idle from
    23:59:59 Tehran onward (no scan, no signal, no admin action), the report
    still goes out the moment the Tehran clock hits 00:00:00.
    Runs forever, recomputing "seconds until next Tehran midnight" fresh
    after each wake — so it never drifts, and a single slow/failed publish
    can't push the next day's timing off."""
    from datetime import timedelta
    tehran_tz = timezone(timedelta(hours=3, minutes=30))
    while True:
        now = datetime.now(tehran_tz)
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        sleep_s = max((next_midnight - now).total_seconds(), 1.0)
        log.info(f"Daily Report midnight scheduler: sleeping {sleep_s:.0f}s until {next_midnight.isoformat()} (Asia/Tehran).")
        await asyncio.sleep(sleep_s)
        try:
            trades = await update_trade_outcomes(session)
            await check_daily_report_rollover(session, state, trades)
        except Exception as e:
            log.error(f"Daily Report midnight scheduler error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 📊 WEEKLY / MONTHLY SIGNAL PERFORMANCE REPORT (feature)
# ─────────────────────────────────────────────────────────────────────────────
# Two NEW automatic reports, built on top of the EXISTING Signal Performance
# Report machinery (_load_closed_trades_by_range → _summarize_trades →
# build_backtest_report — see their definitions further down) instead of a
# separate calculation system, per spec. Same project timezone as the Daily
# Report (Asia/Tehran, hardcoded the same way daily_report_midnight_scheduler
# above already does — REPORT_TIMEZONE is left untouched, it governs a
# different, unrelated report). Duplicate-prevention across restarts/
# redeployments uses the exact same technique as the Daily Report's
# state["daily_report_collecting_day"]: a small marker in `state`, persisted
# via save_state() to bot_runtime.json — no new database/table needed.
# ═══════════════════════════════════════════════════════════════════════════════

def get_previous_calendar_week_range_tehran():
    """Calendar-week boundary for the Weekly Signal Performance Report, per
    spec: one COMPLETE Saturday 00:00:01 → Friday 23:59:59 week, Asia/Tehran
    wall-clock.

    BUGFIX: the previous version assumed "now" IS the boundary instant (it
    floored "now" to today's start and treated that as the week's end,
    using a 6-day offset back to the preceding Saturday). That only holds
    if this is evaluated exactly at Friday 00:00:00 — at that moment Friday
    itself hasn't happened yet, so the computed "week" silently excluded
    all of Friday's signals. It also meant the function was USELESS for
    restart catch-up (see item 3): called on any other weekday it produced
    a bogus, non-week-aligned range.

    Fixed shape: always resolve to the most recently COMPLETED
    Saturday→Friday week, regardless of what day/time "now" happens to be.
    The end boundary is the most recent Saturday 00:00:00 Tehran (today's,
    if today IS Saturday; otherwise the closest one in the past) — that is
    exactly the instant the last full week finished and the current
    (possibly still in-progress) week began. Start is exactly 7 days
    earlier, one second past that prior Saturday's midnight. This makes the
    function correct whether it's called by the dedicated midnight
    scheduler (fires exactly at Saturday 00:00), the 5-minute safety-net
    loop (fires anytime), or right after a restart (fires anytime) — no
    weekday gating required by callers anymore.

    Returns (start_tehran, end_tehran, week_id) — week_id is the Saturday
    start date ("YYYY-MM-DD"), a stable identifier for duplicate-send
    tracking (state["weekly_report_last_week_id"]) that only changes once
    a full week has actually elapsed, so it's safe against restarts.
    """
    from datetime import timedelta
    tehran_tz = timezone(timedelta(hours=3, minutes=30))
    now = datetime.now(tehran_tz)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # Python weekday(): Mon=0 ... Sat=5 ... Sun=6.
    days_since_saturday = (today_start.weekday() - 5) % 7
    end = today_start - timedelta(days=days_since_saturday)
    start = end - timedelta(days=7) + timedelta(seconds=1)
    week_id = start.strftime("%Y-%m-%d")
    return start, end, week_id


def get_previous_calendar_month_range_tehran():
    """Calendar-month boundary for the Monthly Signal Performance Report,
    per spec: complete previous calendar month, Asia/Tehran wall-clock
    (e.g. previous month = August → "2026-08-01 00:00:01" →
    "2026-09-01 00:00:00"). Handles every month length (28/29/30/31 days)
    and the December→January year rollover via plain month arithmetic —
    never a fixed day count.

    Meant to be evaluated AT (or shortly after) the 1st-of-month 00:00:00
    Tehran rollover — "now" floored to the start of today's month IS the
    end boundary; start is the 1st of the PREVIOUS month plus one second.

    Returns (start_tehran, end_tehran, month_id) — month_id is the covered
    month as "YYYY-MM", the duplicate-send tracking key
    (state["monthly_report_last_month_id"]).
    """
    from datetime import timedelta
    tehran_tz = timezone(timedelta(hours=3, minutes=30))
    now = datetime.now(tehran_tz)
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if this_month_start.month == 1:
        prev_month_start = this_month_start.replace(year=this_month_start.year - 1, month=12)
    else:
        prev_month_start = this_month_start.replace(month=this_month_start.month - 1)
    start = prev_month_start + timedelta(seconds=1)
    end = this_month_start
    month_id = prev_month_start.strftime("%Y-%m")
    return start, end, month_id


# Guards against the dedicated midnight scheduler and the 5-minute
# safety-net loop (or a restart-triggered immediate check) racing each
# other: both call _check_and_send_weekly_report/_check_and_send_monthly_report
# independently, and each does "check state -> await network sends ->
# update state", which has an await gap a second, overlapping caller could
# slip through before the first one finishes. The lock plus a re-check of
# state right after acquiring it (double-checked locking) makes the whole
# check-send-mark sequence atomic w.r.t. these callers, so the same
# week/month can never be sent twice.
_weekly_report_lock = asyncio.Lock()
_monthly_report_lock = asyncio.Lock()


async def _check_and_send_weekly_report(session: aiohttp.ClientSession, state: dict):
    """Idempotent: computes the most recently COMPLETED calendar week and
    sends the Weekly Signal Performance Report for it exactly once. Safe to
    call repeatedly and from any weekday/time (primary scheduler + 5-min
    safety-net loop + right after a restart/redeploy) —
    get_previous_calendar_week_range_tehran() always resolves to the last
    full Saturday→Friday week regardless of "now", and
    state["weekly_report_last_week_id"] is only updated AFTER a confirmed
    successful send, so a bot restart, a slow Telegram outage, or an
    overlapping call is always a no-op once a given week has actually gone
    out — and still retryable if it hasn't. Reuses the exact same
    trade-loading logic as the existing (hours-based) Signal Performance
    Report, and the exact same message layout as the Daily Report (see
    build_period_report()).

    FIRST-RUN GUARD: on a brand-new deploy (or the first run ever after
    this feature was added) state["weekly_report_last_week_id"] is None —
    that must NOT be read as "no week has ever been sent, so send one now"
    (which would blast out an old report on every fresh/normal startup).
    Same pattern as check_daily_report_rollover()'s collecting_day
    bootstrap above: the very first time this runs, it silently adopts the
    current latest-completed week_id as the baseline and returns without
    sending. Only once that baseline exists does a *later*, genuinely new
    week_id (the boundary having actually advanced, e.g. after downtime
    that spanned a Saturday rollover) trigger a send."""
    try:
        start, end, week_id = get_previous_calendar_week_range_tehran()
    except Exception as e:
        log.error(f"Weekly report: boundary calc failed: {e}")
        return

    if state.get("weekly_report_last_week_id") is None:
        # First run ever: adopt the current period as baseline, don't send.
        state["weekly_report_last_week_id"] = week_id
        save_state(state)
        log.info(f"Weekly Signal Performance Report: first-run baseline set to week {week_id} "
                 f"(no report sent for it — startup, not a missed boundary).")
        return

    if state.get("weekly_report_last_week_id") == week_id:
        return  # قبلاً برای این هفته ارسال شده — هیچ کاری نکن (جلوگیری از تکرار)

    async with _weekly_report_lock:
        # Re-check: another caller (scheduler vs. safety loop) may have
        # already sent+marked this exact week while we waited for the lock.
        if state.get("weekly_report_last_week_id") == week_id:
            return
        try:
            trades = _load_closed_trades_by_range(start, end)
            period_label = f"Weekly Report — {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')} (Asia/Tehran)"
            report_text = build_period_report(trades, "AFEE TRADER Weekly Report", period_label,
                                                "#AFEE_WEEKLY_REPORT")
            # broadcast_signal() never raises on a Telegram failure (send_telegram
            # retries internally and returns None on exhaustion); it just
            # returns an empty/partial {chat_id: message_id} dict. So success
            # must be checked explicitly — otherwise a fully failed send would
            # still fall through and get marked as "sent", losing the report.
            sent_ids = await broadcast_signal(session, report_text, state)
            if not sent_ids:
                log.error(f"Weekly Signal Performance Report: Telegram delivery failed for "
                          f"week {week_id}; leaving unmarked so it is retried.")
                return  # NOT marked as sent -> next scheduler/safety-loop tick retries
            # Mark as sent AFTER a confirmed successful send, so a mid-send
            # crash/restart or delivery failure simply retries the whole
            # thing (never silently skips a week).
            state["weekly_report_last_week_id"] = week_id
            save_state(state)
            log.info(f"Weekly Signal Performance Report sent for week {week_id} ({len(trades)} signals).")
        except Exception as e:
            log.error(f"Weekly Signal Performance Report send failed: {e}")


async def _check_and_send_monthly_report(session: aiohttp.ClientSession, state: dict):
    """Idempotent counterpart of _check_and_send_weekly_report() for the
    Monthly Signal Performance Report — same duplicate-prevention pattern
    via state["monthly_report_last_month_id"], same first-run baseline
    guard, same reused trade-loading logic and Daily-Report-style message
    layout, calendar-month boundary instead of calendar week.

    get_previous_calendar_month_range_tehran() already always resolves to
    the complete PREVIOUS calendar month regardless of what day "now" is
    (it's derived from the start of the current month via plain month
    arithmetic, not from an assumption that "now" is the 1st) — so, like
    the weekly check above, this needs no weekday/day-of-month gating to
    be safe for the 5-minute safety-net loop or a post-restart catch-up
    call. A bot that was offline through the 1st and comes back up on,
    say, the 5th, must still be able to send the missed previous month's
    report immediately rather than waiting for the next 1st."""
    try:
        start, end, month_id = get_previous_calendar_month_range_tehran()
    except Exception as e:
        log.error(f"Monthly report: boundary calc failed: {e}")
        return

    if state.get("monthly_report_last_month_id") is None:
        # First run ever: adopt the current period as baseline, don't send.
        state["monthly_report_last_month_id"] = month_id
        save_state(state)
        log.info(f"Monthly Signal Performance Report: first-run baseline set to month {month_id} "
                 f"(no report sent for it — startup, not a missed boundary).")
        return

    if state.get("monthly_report_last_month_id") == month_id:
        return  # قبلاً برای این ماه ارسال شده — هیچ کاری نکن (جلوگیری از تکرار)

    async with _monthly_report_lock:
        if state.get("monthly_report_last_month_id") == month_id:
            return
        try:
            trades = _load_closed_trades_by_range(start, end)
            period_label = f"Monthly Report — {month_id} (Asia/Tehran)"
            report_text = build_period_report(trades, "AFEE TRADER Monthly Report", period_label,
                                                "#AFEE_MONTHLY_REPORT")
            sent_ids = await broadcast_signal(session, report_text, state)
            if not sent_ids:
                log.error(f"Monthly Signal Performance Report: Telegram delivery failed for "
                          f"month {month_id}; leaving unmarked so it is retried.")
                return  # NOT marked as sent -> next scheduler/safety-loop tick retries
            state["monthly_report_last_month_id"] = month_id
            save_state(state)
            log.info(f"Monthly Signal Performance Report sent for month {month_id} ({len(trades)} signals).")
        except Exception as e:
            log.error(f"Monthly Signal Performance Report send failed: {e}")


async def weekly_report_midnight_scheduler(session: aiohttp.ClientSession, state: dict):
    """PRIMARY trigger for the Weekly Signal Performance Report — dedicated
    clock, same shape as daily_report_midnight_scheduler() above. Sleeps
    precisely until the next Saturday 00:00:00 Asia/Tehran, wakes, and
    sends the report for the Saturday→Friday week that just ended. Fires
    independently of scan_loop/signals/user actions. Runs forever,
    recomputing the next Saturday fresh after each wake so it never drifts.

    SCHEDULE CHANGE (boundary bugfix, item 1): this used to fire at Friday
    00:00:00, which is BEFORE Friday's own trading day has happened, so the
    "week" it sent was actually only Saturday→Thursday — Friday's signals
    were silently left out of every Weekly report. A report can only
    include a full Saturday→Friday week once Friday itself has finished,
    i.e. at the following Saturday 00:00:00. The trigger is moved to
    Saturday accordingly, one calendar day later than before — this is a
    deliberate part of the fix, not an incidental drift, and is called out
    here (and in the audit summary) rather than left implicit."""
    from datetime import timedelta
    tehran_tz = timezone(timedelta(hours=3, minutes=30))
    while True:
        now = datetime.now(tehran_tz)
        # Next Saturday 00:00:00 strictly in the future: start from
        # tomorrow's midnight and step forward a day at a time until it's a
        # Saturday (weekday() == 5). Always > now, so this can never re-fire
        # "today" even if now happens to already be exactly Saturday 00:00:00.
        candidate = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        while candidate.weekday() != 5:
            candidate += timedelta(days=1)
        sleep_s = max((candidate - now).total_seconds(), 1.0)
        log.info(f"Weekly Report scheduler: sleeping {sleep_s:.0f}s until {candidate.isoformat()} (Asia/Tehran).")
        await asyncio.sleep(sleep_s)
        try:
            await _check_and_send_weekly_report(session, state)
        except Exception as e:
            log.error(f"Weekly Report midnight scheduler error: {e}")


async def monthly_report_midnight_scheduler(session: aiohttp.ClientSession, state: dict):
    """PRIMARY trigger for the Monthly Signal Performance Report — same
    shape as weekly_report_midnight_scheduler() above. Sleeps precisely
    until the 1st-of-next-month 00:00:00 Asia/Tehran (plain month/year
    arithmetic — correctly handles every month length and the December→
    January rollover), wakes, and sends the report for the calendar month
    that just ended."""
    from datetime import timedelta
    tehran_tz = timezone(timedelta(hours=3, minutes=30))
    while True:
        now = datetime.now(tehran_tz)
        if now.month == 12:
            next_month_start = now.replace(year=now.year + 1, month=1, day=1,
                                            hour=0, minute=0, second=0, microsecond=0)
        else:
            next_month_start = now.replace(month=now.month + 1, day=1,
                                            hour=0, minute=0, second=0, microsecond=0)
        sleep_s = max((next_month_start - now).total_seconds(), 1.0)
        log.info(f"Monthly Report scheduler: sleeping {sleep_s:.0f}s until {next_month_start.isoformat()} (Asia/Tehran).")
        await asyncio.sleep(sleep_s)
        try:
            await _check_and_send_monthly_report(session, state)
        except Exception as e:
            log.error(f"Monthly Report midnight scheduler error: {e}")


async def weekly_monthly_report_safety_loop(session: aiohttp.ClientSession, state: dict):
    """Secondary safety net for BOTH new reports — same role as
    daily_report_loop() plays for the Daily Report: re-checks on a 5-minute
    cadence purely in case a midnight scheduler task above ever dies, or the
    bot (e.g. Railway) was down across a rollover — even for multiple days —
    so the week's/month's report still goes out (slightly late, exactly
    once) rather than never at all. This loop's first iteration runs
    immediately on startup, before its first sleep, so a restart itself
    triggers the catch-up check right away. Both checks are idempotent (see
    _check_and_send_weekly_report/_check_and_send_monthly_report) and now
    always resolve to the latest COMPLETED week/month regardless of what
    day it is (item 1/3 fix), so this reliably catches up after downtime of
    any length without needing to land on a specific weekday, never sends
    anything the midnight schedulers wouldn't have, and — via the locks in
    those functions — never sends a duplicate even if it races a midnight
    scheduler."""
    while True:
        try:
            await _check_and_send_weekly_report(session, state)
            await _check_and_send_monthly_report(session, state)
        except Exception as e:
            log.error(f"Weekly/Monthly report safety loop error: {e}")
        await asyncio.sleep(300)


async def sync_cross_day_report_edits(session: aiohttp.ClientSession, state: dict, trades: list):
    """Handles a signal that was SENT on an already-published day but whose
    result only comes in later (e.g. sent 23:58, closes 00:03 the next day —
    after that day's report already went out): edits the previously
    published message in place to fill in the result, keeping the original
    signal order and never moving it into a different day's report or
    creating a duplicate. Never touches the current, still-collecting
    (unpublished) day.
    FIX (duplicate-report bug, still applies here): a plain rate limit or
    other transient edit failure is never treated as proof the message is
    gone; only an explicit Telegram "gone" response triggers a resend."""
    published_day = state.get("daily_report_msg_date")
    stored_ids = state.get("daily_report_msg_ids") or {}
    if not published_day or not stored_ids:
        return  # Nothing published yet for any day — nothing to keep in sync.

    try:
        report_text = build_daily_report(trades, published_day)
    except Exception as e:
        log.error(f"sync_cross_day_report_edits: build_daily_report failed: {e}")
        return

    async with _daily_report_sync_lock:
        # Re-read after acquiring the lock in case a rollover just replaced
        # the published day/ids while we were building the text above.
        if state.get("daily_report_msg_date") != published_day:
            return
        stored_ids = state.get("daily_report_msg_ids") or {}
        last_text = state.get("daily_report_last_text")

        if last_text == report_text:
            return  # Nothing changed since publish/last edit — no Telegram call.

        any_ok = False
        provably_gone = False
        for chat_id_str, msg_id in list(stored_ids.items()):
            try:
                resp = await edit_msg(session, chat_id_str, msg_id, report_text)
                desc = (resp or {}).get("description", "") if resp else ""
                if resp and resp.get("ok"):
                    any_ok = True
                elif "not modified" in desc.lower():
                    any_ok = True  # Telegram already has this exact text — fine.
                elif any(marker in desc.lower() for marker in _EDIT_GONE_MARKERS):
                    provably_gone = True
                # else: transient failure (timeout, unclear error) — neither
                # ok nor provably gone; leave stored ids as-is and just retry
                # the edit next sync, no resend.
            except Exception as e:
                log.debug(f"sync_cross_day_report_edits: edit failed for chat {chat_id_str}: {e}")

        if any_ok:
            state["daily_report_last_text"] = report_text
            save_state(state)
            return

        if provably_gone:
            # Telegram itself confirms the published message is gone — this
            # is the only case where re-sending that day's (already-ended)
            # report is correct, so the cross-day result doesn't just vanish.
            try:
                sent_ids = await broadcast_signal(session, report_text, state)
                if sent_ids:
                    state["daily_report_msg_ids"] = sent_ids
                    state["daily_report_msg_date"] = published_day
                    state["daily_report_last_text"] = report_text
                    save_state(state)
            except Exception as e:
                log.error(f"sync_cross_day_report_edits: resend after provably-gone failed: {e}")
            return

        log.debug("sync_cross_day_report_edits: edit transiently failed for all chats; will retry next sync")


async def sync_daily_report(session: aiohttp.ClientSession, state: dict, trades: list):
    """Redesigned Daily Report (feature): the automatic report always
    represents one COMPLETE Asia/Tehran calendar day. This is the single
    entry point every existing call site (scan_once, per-signal, manual
    overrides, the safety-net loop) already calls after signals/results
    change; it now does two things and nothing else:
      1. check_daily_report_rollover() — publishes the day that just ended,
         exactly once, the moment 00:00 Asia/Tehran is crossed. Never
         publishes the still-in-progress day, never publishes on startup,
         never publishes on the first signal.
      2. sync_cross_day_report_edits() — if a signal sent on an already-
         published day closes later, edits that day's report in place
         instead of leaving it stale or creating a new message.
    Data for the current (unpublished) day is never sent anywhere by this
    function — it simply stays in the database, exactly as recorded, until
    the day ends and step 1 builds/publishes it."""
    await check_daily_report_rollover(session, state, trades)
    await sync_cross_day_report_edits(session, state, trades)

# ─── ADVANCED ANALYTICS (/stats) ───────────────────────────────────────────────
def calc_strategy_stats(strategy: str, days: Optional[int] = None) -> dict:
    """
    آمار کامل یک استراتژی را از دیتابیس محاسبه می‌کند:
    winrate, loss rate, TP1 rate, TP2 rate, average RR, expectancy,
    max drawdown, profit factor, total trades.
    اگر days مشخص شود، فقط معاملات بسته‌شده در آن بازه (آخرین N روز) لحاظ می‌شوند.
    """
    conn = get_db_connection()
    try:
        # FIX v3.2: exclude all open/pending states from stats; فقط معاملات واقعاً بسته‌شده
        query = """
            SELECT r.status, r.pnl_percent, r.rr_multiple, r.closed_at
            FROM signals s JOIN results r ON r.signal_id = s.id
            WHERE s.strategy = ?
              AND r.status NOT IN ('OPEN','PENDING','ENTERED','TP1_HIT')
              AND r.closed_at IS NOT NULL
              -- FIX v3.4: TP1_TOUCHSL بسته شده و در آمار حساب می‌شود (به عنوان TP1)
        """
        params = [strategy]
        if days is not None:
            cutoff = (datetime.now(timezone.utc) - __import__('datetime').timedelta(days=days)).isoformat()
            query += " AND r.closed_at >= ?"
            params.append(cutoff)
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    total = len(rows)
    if total == 0:
        return {
            "strategy": strategy, "total_trades": 0, "winrate": 0, "loss_rate": 0,
            "tp1_rate": 0, "tp2_rate": 0, "avg_rr": 0, "expectancy": 0,
            "max_drawdown": 0, "profit_factor": 0,
        }

    # FIX (bug audit): MISSED دیگر از آمار کلاً حذف نمی‌شود — باید در total_trades
    # شمارش شود (یک سیگنال صادرشده و بسته‌شده است)، اما نه به عنوان win نه loss.
    # (wins/losses پایین به‌طور صریح فقط TP/SL را شامل می‌شوند، پس MISSED خودبه‌خود
    # در decisive trades حساب نمی‌شود.)

    # FIX v3.4: TP1_TOUCHSL = win (TP1 زده شد؛ SL بعدی = Break Even، نه ضرر)
    wins = [r for r in rows if r["status"] in ("TP1", "TP2", "TP1_HIT", "TP2_HIT", "TP1_TOUCHSL")]
    losses = [r for r in rows if r["status"] in ("SL", "SL_HIT")]
    tp1_count = len([r for r in rows if r["status"] in ("TP1", "TP1_HIT", "TP1_TOUCHSL")])
    tp2_count = len([r for r in rows if r["status"] in ("TP2", "TP2_HIT")])

    decisive = len(wins) + len(losses)  # EXPIRED را از وین‌ریت/لاس‌ریت کنار می‌گذاریم
    winrate = (len(wins) / decisive * 100) if decisive > 0 else 0
    loss_rate = (len(losses) / decisive * 100) if decisive > 0 else 0
    tp1_rate = (tp1_count / total * 100) if total > 0 else 0
    tp2_rate = (tp2_count / total * 100) if total > 0 else 0

    rr_values = [r["rr_multiple"] for r in rows if r["rr_multiple"] is not None]
    avg_rr = sum(rr_values) / len(rr_values) if rr_values else 0

    # Expectancy = (winrate × avg_win_R) - (lossrate × avg_loss_R)
    win_rr = [r["rr_multiple"] for r in wins if r["rr_multiple"] is not None]
    loss_rr = [abs(r["rr_multiple"]) for r in losses if r["rr_multiple"] is not None]
    avg_win_r = sum(win_rr) / len(win_rr) if win_rr else 0
    avg_loss_r = sum(loss_rr) / len(loss_rr) if loss_rr else 0
    win_prob = (len(wins) / decisive) if decisive > 0 else 0
    loss_prob = (len(losses) / decisive) if decisive > 0 else 0
    expectancy = (win_prob * avg_win_r) - (loss_prob * avg_loss_r)

    # Profit Factor = مجموع سود / مجموع ضرر (بر اساس pnl_percent)
    gross_profit = sum(r["pnl_percent"] for r in rows if r["pnl_percent"] and r["pnl_percent"] > 0)
    gross_loss = abs(sum(r["pnl_percent"] for r in rows if r["pnl_percent"] and r["pnl_percent"] < 0))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0)

    # Max Drawdown — بر اساس دنباله pnl_percent به ترتیب بسته‌شدن (equity curve فرضی جمعی)
    rows_sorted = sorted(rows, key=lambda r: r["closed_at"] or "")
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in rows_sorted:
        pnl = r["pnl_percent"] or 0
        equity += pnl
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

    return {
        "strategy": strategy, "total_trades": total,
        "winrate": round(winrate, 1), "loss_rate": round(loss_rate, 1),
        "tp1_rate": round(tp1_rate, 1), "tp2_rate": round(tp2_rate, 1),
        "avg_rr": round(avg_rr, 2), "expectancy": round(expectancy, 2),
        "max_drawdown": round(max_dd, 2), "profit_factor": round(profit_factor, 2),
    }

def build_stats_report() -> str:
    """Full advanced statistics report for all strategies, for the /stats command."""
    lines = ["📈 <b>Advanced Strategy Statistics</b>", ""]

    weights = get_all_strategy_weights()

    for strat_name, _ in STRATEGIES:
        all_time = calc_strategy_stats(strat_name)
        last7 = calc_strategy_stats(strat_name, days=7)
        last30 = calc_strategy_stats(strat_name, days=30)
        w = weights.get(strat_name, 1.0)
        w_tag = f" | ⚖️ Weight: {w:.2f}" if w != 1.0 else ""

        if all_time["total_trades"] == 0:
            lines.append(f"▫️ <b>{strat_name}</b> — No closed trades yet{w_tag}")
            lines.append("")
            continue

        lines.append(f"▫️ <b>{strat_name}</b>{w_tag}")
        lines.append(f"   📦 Total Trades: {all_time['total_trades']}")
        lines.append(f"   🎯 Win Rate: {all_time['winrate']}%  |  Loss Rate: {all_time['loss_rate']}%")
        lines.append(f"   ✅ TP1 Rate: {all_time['tp1_rate']}%  |  ✅✅ TP2 Rate: {all_time['tp2_rate']}%")
        lines.append(f"   📐 Average RR: {all_time['avg_rr']}  |  Expectancy: {all_time['expectancy']}")
        lines.append(f"   📉 Max Drawdown: {all_time['max_drawdown']}%  |  Profit Factor: {all_time['profit_factor']}")
        lines.append(f"   🗓 Last 7 Days: {last7['total_trades']} trades, win rate {last7['winrate']}%")
        lines.append(f"   🗓 Last 30 Days: {last30['total_trades']} trades, win rate {last30['winrate']}%")
        lines.append("")

    return "\n".join(lines)

# ─── REPLAY MODE (دستور /replay SYMBOL) ────────────────────────────────────────
def get_signal_history(symbol: str, limit: int = 10) -> list:
    """آخرین N سیگنال صادرشده برای یک نماد را از دیتابیس برمی‌گرداند (جدیدترین اول)."""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy, s.entry, s.stop, s.tp1, s.tp2,
                   s.score, s.opened_at, s.signal_uid, r.status, r.closed_at, r.close_price, r.pnl_percent, r.rr_multiple
            FROM signals s LEFT JOIN results r ON r.signal_id = s.id
            WHERE s.symbol = ?
            ORDER BY s.id DESC
            LIMIT ?
        """, (symbol, limit)).fetchall()
        return [_row_to_trade_dict(row) for row in rows]
    finally:
        conn.close()

STRATEGY_ENTRY_REASON = {
    "Stop Hunter":         "Fake breakout of an S/R zone on a lower timeframe, a correction, then a break of the last small-wave low/high in the opposite direction of the first breakout",
    "Hammer Fib (4H)":     "Hammer/shooting-star candle forms on the 4H timeframe, with entry at the 0.618 Fibonacci level of the next wave",
    "Hammer Fib (1H)":     "Hammer/shooting-star candle forms on the 1H timeframe, with entry at the 0.618 Fibonacci level of the next wave",
    "HB":                  "Strong spike candle (high volume/body) against the trend at a key zone, entry at the 0.5 level of that same candle",
    "Trigger Fibonacci":   "Three consecutive candle conditions in the direction of the main trend + entry at the 0.618 Fibonacci correction level",
    "Exhaustion":          "Steady weakening of body and volume of trend-aligned candles near a key zone (a sign the trend is running out of strength)",
    "RSI Divergence":      "RSI reverses from overbought (>70)/oversold (<30) territory, along with a possible price-RSI divergence",
}

def build_replay_report(symbol: str) -> str:
    """Replay Mode report: a symbol's most recent signals, entry reason, strategy, score, and final result."""
    history = get_signal_history(symbol, limit=10)
    if not history:
        return f"🎬 <b>Replay {symbol}</b>\n\nNo signals recorded in the history for this symbol."

    status_fa = {
        "OPEN": "⏳ Still open (no result yet)",
        "TP1": "✅ TP1 hit",
        "TP1_TOUCHSL": "🔄 TP1 hit, then Break Even (Touch SL)",  # FIX v3.4
        "TP2": "✅✅ TP2 hit",
        "SL": "❌ Stop hit",
        "EXPIRED": "⏳ No result (24 hours passed)",
        "MISSED": "⚠️ Missed (entry price not touched)",  # FIX v3.4
        "PENDING": "⏳ Waiting entry",
        "ENTERED": "⏳ Entered — waiting TP/SL",
    }

    lines = [f"🎬 <b>Replay {symbol}</b>", f"Last {len(history)} recorded signals:", ""]

    for t in history:
        decimals = 8 if t["entry"] < 0.01 else (4 if t["entry"] < 10 else 2)
        emoji = "🟢" if t["direction"] == "BUY" else "🔴"
        opened_dt = t["opened_at"][:16].replace("T", " ")
        reason = STRATEGY_ENTRY_REASON.get(t["strategy"], "No entry reason recorded")

        lines.append(f"{emoji} <b>{t['strategy']}</b> — {t['direction']}  |  🕒 {opened_dt}")
        if t.get("signal_uid"):
            lines.append(f"   🆔 <code>{t['signal_uid']}</code>")
        lines.append(f"   📝 Entry Reason: {reason}")
        lines.append(f"   💰 Entry: {t['entry']:.{decimals}f}  |  🛑 Stop: {t['stop']:.{decimals}f}")
        lines.append(f"   🎯 TP1: {t['tp1']:.{decimals}f}  |  🎯 TP2: {t['tp2']:.{decimals}f}")
        lines.append(f"   ⭐️ Score: {t.get('score', '-')}/100")
        # BUG FIX (history/replay showing missing or wrong result, e.g. TAOUSDT):
        # t['status'] is the legacy COMPAT status (_row_to_trade_dict folds
        # PENDING+ENTERED both into "OPEN", and TP1_TOUCHSL into "TP1"), so a
        # lookup keyed on it can never match the more specific "PENDING",
        # "ENTERED", or "TP1_TOUCHSL" entries in status_fa above — those
        # trades silently showed the wrong/generic label instead of their
        # actual recorded monitoring result. raw_status is the real,
        # unmodified status stored by update_trade_outcomes; look that up
        # first and only fall back to compat status if it's somehow absent.
        _lookup_status = t.get("raw_status") or t["status"]
        lines.append(f"   📌 Result: {status_fa.get(_lookup_status, status_fa.get(t['status'], _lookup_status))}")
        if t.get("pnl_percent") is not None:
            sign = "🟢+" if t["pnl_percent"] >= 0 else "🔴"
            lines.append(f"   📊 Final Result: {sign}{abs(t['pnl_percent']):.2f}%")
        lines.append("")

    return "\n".join(lines)


MIN_TRADES_FOR_WEIGHTING = 10   # حداقل تعداد معامله بسته‌شده برای اینکه وزن واقعی بگیرد (وگرنه neutral=1.0)
WEIGHT_MIN = 0.5                # حداقل وزن ممکن (پنالتی شدید برای استراتژی ضعیف)
WEIGHT_MAX = 1.5                # حداکثر وزن ممکن (بوست برای استراتژی قوی)

def get_all_strategy_weights() -> dict:
    """تمام وزن‌های ذخیره‌شده را از دیتابیس می‌خواند؛ برای استراتژی‌های بدون رکورد، 1.0 (نوترال) برمی‌گرداند."""
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT strategy, weight FROM strategy_weights").fetchall()
        weights = {row["strategy"]: row["weight"] for row in rows}
    finally:
        conn.close()
    return {name: weights.get(name, 1.0) for name, _ in STRATEGIES}

# FIX (Pass 4 audit, performance): get_strategy_weight()/get_filter_config() were
# called once per candidate signal during scan_symbol — each call opened a brand
# new sqlite3 connection (connect + 3 PRAGMAs + query + close) purely to read a
# value that changes at most once a week (strategy weights) or only when an
# admin explicitly runs /config (filter config). A short TTL cache cuts that
# down to one real DB hit per key per cache window, while writes explicitly
# invalidate their own key so an admin's change is visible on the very next read
# (never stale beyond the write itself).
_CONFIG_CACHE_TTL = 30  # seconds
_strategy_weight_cache: dict = {}   # strategy -> (weight, expires_at)
_filter_config_cache: dict = {}     # key -> (value, expires_at)

def get_strategy_weight(strategy: str) -> float:
    """وزن فعلی یک استراتژی خاص (برای استفاده لحظه‌ای هنگام محاسبه Score)."""
    now = time.time()
    cached = _strategy_weight_cache.get(strategy)
    if cached and cached[1] > now:
        return cached[0]
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT weight FROM strategy_weights WHERE strategy = ?", (strategy,)).fetchone()
    finally:
        conn.close()
    weight = row["weight"] if row else 1.0
    _strategy_weight_cache[strategy] = (weight, now + _CONFIG_CACHE_TTL)
    return weight

# ─── CONFIGURABLE FILTER PANEL ────────────────────────────────────────────────
def get_filter_config(key: str, default=None):
    """خواندن یک پارامتر فیلتر از دیتابیس. مقدار برگشتی رشته است — تبدیل نوع در محل استفاده."""
    now = time.time()
    cached = _filter_config_cache.get(key)
    if cached and cached[1] > now:
        return cached[0]
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT value FROM filter_config WHERE key=?", (key,)).fetchone()
    finally:
        conn.close()
    value = row["value"] if row else default
    _filter_config_cache[key] = (value, now + _CONFIG_CACHE_TTL)
    return value

def set_filter_config(key: str, value):
    """ذخیره یک پارامتر فیلتر به صورت runtime (بدون نیاز به ری‌استارت ربات)."""
    now = datetime.now(timezone.utc).isoformat()
    old_value = get_filter_config(key)
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT INTO filter_config (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, str(value), now)
        )
        conn.commit()
    finally:
        conn.close()
    _filter_config_cache.pop(key, None)  # FIX (Pass 4 audit): make the change visible immediately, not after TTL
    log.info(f"Filter config changed | {key}: {old_value} -> {value}")

def get_adx_threshold() -> float:
    return float(get_filter_config("adx_threshold", ADX_THRESHOLD_DEFAULT))

def get_atr_multiplier() -> float:
    return float(get_filter_config("atr_multiplier", ATR_MULTIPLIER_DEFAULT))

def get_session_filters() -> dict:
    raw = get_filter_config("session_filters", json.dumps(SESSION_FILTERS_DEFAULT))
    try:
        return json.loads(raw)
    except Exception:
        return SESSION_FILTERS_DEFAULT.copy()

# ─── PER-STRATEGY SETTINGS (Strategy Settings admin section) ─────────────────
# هر استراتژی حالا پیکربندی فیلترهای خودش را جداگانه دارد (بجای فیلترهای Global).
# مقادیر پیش‌فرض هر فیلتر جدید/قدیمی برای یک استراتژی که هنوز تنظیم نشده.
STRATEGY_FILTER_DEFAULTS = {
    # ── فیلترهای قدیمی (منتقل‌شده از «فیلترهای پیشرفته» و «فیلتر حجم کندل ورود») ──
    "adx_threshold": ADX_THRESHOLD_DEFAULT,
    "atr_multiplier": ATR_MULTIPLIER_DEFAULT,
    "session_filters": SESSION_FILTERS_DEFAULT.copy(),
    "entry_volume_filter_enabled": True,
    "entry_volume_ma_period": 20,
    "entry_volume_multiplier": 1.5,
    # ── فیلترهای جدید ──
    "oi_filter_enabled": False,          # Open Interest Filter — قدرت پول ورودی
    "oi_threshold": 5.0,                 # حداقل درصد تغییر Open Interest لازم
    "funding_filter_enabled": False,     # Funding Rate Filter — حذف معاملات شلوغ
    "funding_rate_limit": 0.05,          # حداکثر |funding rate| مجاز (٪)
    "btc_trend_filter_enabled": False,   # BTC Trend Filter — هم‌جهت با روند بیت‌کوین
    "btc_trend_timeframe": "4h",         # تایم‌فریم تشخیص روند بیت‌کوین
    "news_filter_enabled": False,        # News Filter — پرهیز از معامله نزدیک اخبار مهم
    "news_before_minutes": 30,           # دقیقه قبل از خبر که معامله بلاک می‌شود
    "news_after_minutes": 30,            # دقیقه بعد از خبر که معامله بلاک می‌شود
    # NOTE: The "Minimum RR Filter" (min_rr_filter_enabled / min_rr_value) has
    # been permanently removed from the system (audit finding: every strategy
    # derives TP1 from calc_targets() as entry + 1.5x risk, so the RR it was
    # checking was a fixed constant of 1.5 — the filter could never pass
    # against its own 2.5 default and was not a real quality gate). Do not
    # re-add these keys; see calc_targets()/run_live_filters() for the
    # relevant audit notes.
    # ── فیلترهای جدید (Task 2: EMA Filter / ADX Max / ATR Range / Volume MA Type) ──
    # همه پیش‌فرض خاموش/بدون محدودیت هستند تا رفتار پیکربندی‌های موجود کاملاً حفظ شود.
    "ema_filter_enabled": False,          # EMA Filter — قیمت بالا/پایین یک EMA مشخص
    "ema_filter_period": 50,              # دوره EMA برای این فیلتر
    "ema_filter_direction": "above",      # "above" (فقط بالای EMA) یا "below" (فقط زیر EMA) — فقط در حالت manual
    "ema_filter_mode": "manual",          # "manual" (Above/Below ثابت، رفتار قبلی) یا "auto" (Auto EMA Trend Filter)
    "ema_filter_auto_atr_enabled": True,  # ON/OFF برای ناحیه خنثی مبتنی بر ATR در حالت Auto (فقط مخصوص EMA Auto — جدا از atr_multiplier سراسری)
    "ema_filter_auto_atr_multiplier": 1.0,# ضریب ATR اختصاصی ناحیه خنثی Auto EMA: EMA ± (ATR × این ضریب)
    "adx_max": None,                      # سقف ADX — None یعنی بدون سقف (فقط adx_threshold به‌عنوان کف اعمال می‌شود)
    "adx_threshold_enabled": True,        # ON/OFF برای فیلتر موجود ADX Threshold (کف ADX) — پیش‌فرض True یعنی رفتار قبلی حفظ می‌شود
    "atr_range_filter_enabled": False,    # فیلتر بازه ATR (نسبت به قیمت٪) — جدا از atr_multiplier (که برای جهش ناگهانی نوسان است)
    "atr_min_pct": None,                  # حداقل ATR به‌صورت درصدی از قیمت (None = بدون حداقل)
    "atr_max_pct": None,                  # حداکثر ATR به‌صورت درصدی از قیمت (None = بدون حداکثر)
    "volume_ma_filter_enabled": False,    # Volume Filter — حجم کندل ورود بالاتر از SMA20 یا EMA20 حجم
    "volume_ma_type": "sma",              # "sma" یا "ema"
    # ── AI Trade Intelligence (ATIE) — NEW, purely additive trade-management
    # monitor. Does not affect signal generation, scoring, TP/SL, or filters
    # in any way. Independently toggleable per strategy. Default: Disabled.
    "ai_trade_intelligence_enabled": False,
    # ── ATR Stop Buffer (configurable per strategy) ──────────────────────────
    # ON (default True, preserves current live behavior on upgrade): stop =
    # reference level (wick/swing/signal-candle/Fib-swing per each strategy's
    # own spec) +/- ATR(14) x atr_stop_multiplier.
    # OFF: stop = the exact reference level, no buffer added, per the
    # strategy's literal spec ("behind the wick", "behind the low", etc.).
    "atr_stop_buffer_enabled": True,
    "atr_stop_multiplier": 1.5,
    # ── Global Breakout Validation (configurable, not removed) ──────────────
    # Minimum fraction of a breakout/CHOCH/pivot candle's body that must
    # close outside the zone/reference level for the breakout to count.
    "breakout_min_body_ratio": 0.6,
    # ── HB strategy — score-component thresholds (audit fix: these were
    # hardcoded literals inside strategy_hb() and could not be tuned from the
    # Admin Panel; now wired through get_strategy_settings() like every other
    # per-strategy knob). Only HB reads these three; other strategies ignore
    # them. Defaults match the previous hardcoded values, so existing live
    # behavior is unchanged until an admin actually adjusts them. ──
    "hb_zone_tolerance": 0.008,          # near-S/R tolerance used by is_near_level() for HB
    "hb_spike_dominance_multiplier": 3.0,  # spike candle body must be >= this x the 20-candle avg body
    "hb_tight_level_touch_ratio": 0.5,   # "tight touch" = price within (hb_zone_tolerance x this ratio) of the level
    # ── Per-Strategy Minimum Score (replaces the old global-only /setscore) ──
    # None = this strategy has never had its own value set yet, so it falls
    # back to the legacy global min_signal_score (get_min_signal_score(),
    # itself defaulting to _MIN_SIGNAL_SCORE_DEFAULT) — preserves existing
    # live behavior for every strategy until an admin explicitly sets a
    # per-strategy value via the new /setscore panel. Once set, an int here
    # ALWAYS wins for that strategy, in both live scanning and Historical
    # Backtest/Replay (see get_strategy_min_score()).
    "min_signal_score": None,
}

BTC_TREND_TIMEFRAMES = ["15m", "1h", "4h", "1d"]

def get_strategy_settings(state: dict, strategy: str) -> dict:
    """تنظیمات فیلترهای مخصوص یک استراتژی را برمی‌گرداند. اولین بار برای هر استراتژی،
    مقادیر قدیمی Global (اگر تنظیم شده بودند) به عنوان مقدار شروع کپی می‌شوند تا
    پیکربندی فعلی کاربر از دست نرود؛ فیلترهای جدید با مقدار پیش‌فرض اضافه می‌شوند."""
    all_settings = state.setdefault("strategy_settings", {})
    if strategy not in all_settings:
        all_settings[strategy] = {
            "adx_threshold": state.get("adx_threshold", ADX_THRESHOLD_DEFAULT),
            "atr_multiplier": state.get("atr_multiplier", ATR_MULTIPLIER_DEFAULT),
            "session_filters": (state.get("session_filters") or SESSION_FILTERS_DEFAULT).copy(),
            "entry_volume_filter_enabled": state.get("entry_volume_filter_enabled", True),
            "entry_volume_ma_period": state.get("entry_volume_ma_period", 20),
            "entry_volume_multiplier": state.get("entry_volume_multiplier", 1.5),
        }
    cfg = all_settings[strategy]
    for k, v in STRATEGY_FILTER_DEFAULTS.items():
        if k not in cfg:
            cfg[k] = v.copy() if isinstance(v, dict) else v
    return cfg

def set_strategy_setting(state: dict, strategy: str, key: str, value):
    cfg = get_strategy_settings(state, strategy)
    old_value = cfg.get(key)
    cfg[key] = value
    log.info(f"Strategy setting changed | strategy={strategy} {key}: {old_value} -> {value}")
    return cfg

def backfill_all_strategy_settings(state: dict) -> None:
    """Ensures EVERY strategy in STRATEGIES has a complete settings dict in
    state["strategy_settings"] — including strategies the user has never
    opened in the Strategy Settings UI, and any filter keys that were added
    to STRATEGY_FILTER_DEFAULTS (e.g. atr_stop_buffer_enabled,
    atr_stop_multiplier, breakout_min_body_ratio) since the user's last visit
    to that particular strategy's panel.

    get_strategy_settings() only ever fills in *missing* keys with their
    default — it never overwrites a value the user actually set — so this is
    safe to call at any time. It exists purely so /exportconfig (Section 6)
    and /importconfig (Section 8, backward compatibility) can guarantee
    bot_config.json always reflects every setting the live engine actually
    reads, instead of relying on each strategy's panel having been opened at
    least once since the field was introduced.

    Also actively PURGES the retired Minimum RR Filter keys
    (min_rr_filter_enabled / min_rr_value) from every strategy's persisted
    settings, since the defaults dict only ever backfills *missing* keys and
    would otherwise leave those two stranded forever in any bot_config.json
    saved before the filter was removed."""
    for strat_name, _fn in STRATEGIES:
        cfg = get_strategy_settings(state, strat_name)
        cfg.pop("min_rr_filter_enabled", None)
        cfg.pop("min_rr_value", None)

# ─── SESSION DETECTION ────────────────────────────────────────────────────────
def get_current_session() -> str:
    """تشخیص session فعلی بر اساس UTC. سشن‌ها: London 07-16 UTC، NY 13-22 UTC، Asian 00-09 UTC.

    REPLAY FIX (item 2 audit): during a Historical Strategy Replay, "current"
    must mean the SIMULATED historical time, not real wall-clock time —
    otherwise every historical signal would be session-filtered against
    today's time-of-day instead of the time-of-day it actually occurred at,
    which is not what "reproduce live behavior in the past" means. Uses the
    same _replay_ctx contextvar as the get_candles() shim — live scanning
    never sets it, so live behavior (real wall-clock time) is unaffected.
    """
    _cache = _replay_ctx.get()
    if _cache is not None and _cache.sim_time_ms:
        h = datetime.fromtimestamp(_cache.sim_time_ms / 1000, tz=timezone.utc).hour
    else:
        h = datetime.now(timezone.utc).hour
    if 7 <= h < 16:
        return "london"
    elif 13 <= h < 22:
        return "ny"
    elif h < 9 or h >= 22:
        return "asian"
    return "ny"  # overlap

def is_session_allowed(state: dict) -> bool:
    """بررسی اینکه آیا session فعلی توسط کاربر مجاز دانسته شده."""
    session_filters = state.get("session_filters", SESSION_FILTERS_DEFAULT)
    current = get_current_session()
    return session_filters.get(current, True)

def _normalize_metric(value: float, worst: float, best: float) -> float:
    """مقدار یک معیار را به بازه ۰ تا ۱ نرمال می‌کند (برای ترکیب چند معیار با واحد متفاوت)."""
    if best == worst:
        return 0.5
    v = (value - worst) / (best - worst)
    return max(0.0, min(1.0, v))

def calc_strategy_weight(strategy: str) -> float:
    """
    وزن جدید یک استراتژی را بر اساس عملکرد ۳۰ روز اخیر محاسبه می‌کند:
    ترکیبی وزن‌دار از Win Rate + Expectancy + Profit Factor.
    اگر تعداد معاملات کافی نباشد (آماری غیرقابل‌اعتماد)، وزن نوترال 1.0 برمی‌گردد.
    خروجی همیشه بین WEIGHT_MIN و WEIGHT_MAX محدود می‌شود.
    """
    stats = calc_strategy_stats(strategy, days=30)
    if stats["total_trades"] < MIN_TRADES_FOR_WEIGHTING:
        return 1.0

    # نرمال‌سازی هر معیار به بازه ۰-۱ با بازه‌های واقع‌گرایانه برای این نوع استراتژی‌ها
    norm_winrate = _normalize_metric(stats["winrate"], worst=30, best=70)        # 30%→0 , 70%→1
    norm_expectancy = _normalize_metric(stats["expectancy"], worst=-1.0, best=1.5)  # -1R→0 , +1.5R→1
    norm_pf = _normalize_metric(stats["profit_factor"], worst=0.5, best=2.5)     # 0.5→0 , 2.5→1

    # ترکیب وزن‌دار: Win Rate و Expectancy اهمیت بیشتری دارند تا Profit Factor (که می‌تواند نویزی باشد)
    composite = (norm_winrate * 0.4) + (norm_expectancy * 0.4) + (norm_pf * 0.2)

    # تبدیل امتیاز ترکیبی (۰ تا ۱) به بازه وزن (WEIGHT_MIN تا WEIGHT_MAX)
    weight = WEIGHT_MIN + composite * (WEIGHT_MAX - WEIGHT_MIN)
    return round(weight, 2)

def update_all_strategy_weights() -> dict:
    """وزن همه استراتژی‌ها را بازمحاسبه و در دیتابیس ذخیره می‌کند. خروجی: دیکشنری وزن‌های جدید."""
    now = datetime.now(timezone.utc).isoformat()
    new_weights = {}
    conn = get_db_connection()
    try:
        for strat_name, _ in STRATEGIES:
            w = calc_strategy_weight(strat_name)
            new_weights[strat_name] = w
            conn.execute(
                "INSERT INTO strategy_weights (strategy, weight, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(strategy) DO UPDATE SET weight=excluded.weight, updated_at=excluded.updated_at",
                (strat_name, w, now)
            )
        conn.commit()
    finally:
        conn.close()
    _strategy_weight_cache.clear()  # FIX (Pass 4 audit): drop stale cached weights after bulk reweight
    return new_weights

def build_weight_update_report(old_weights: dict, new_weights: dict) -> str:
    """Weekly strategy weight change report text, sent to admins."""
    lines = ["⚖️ <b>Weekly Strategy Weight Update</b>", ""]
    any_change = False
    for strat_name, _ in STRATEGIES:
        old_w = old_weights.get(strat_name, 1.0)
        new_w = new_weights.get(strat_name, 1.0)
        if abs(old_w - new_w) < 0.01:
            continue
        any_change = True
        arrow = "📈" if new_w > old_w else "📉"
        tag = "Boosted" if new_w > 1.0 else ("Penalized" if new_w < 1.0 else "Neutral")
        lines.append(f"{arrow} <b>{strat_name}</b>: {old_w:.2f} → {new_w:.2f}  ({tag})")
    if not any_change:
        lines.append("No meaningful change in strategy weights (or not enough data to calculate).")
    return "\n".join(lines)

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
async def send_telegram(session: aiohttp.ClientSession, text: str, chat_id: str = None, reply_to_message_id: int = None, reply_markup: dict = None) -> Optional[int]:
    """
    FIX (Pass 2 audit, high severity): previously had NO retry logic at all —
    a single failed call (Telegram 429 rate limit, a transient 5xx, or a
    network blip) logged an error and silently dropped the message forever.
    Since this is the only function every signal/TP1/TP2/SL/MISSED
    notification goes through, that meant real trade outcomes could vanish
    from the channel with nothing but a log line the user never sees. Now
    retries a bounded number of times, honoring Telegram's own `retry_after`
    hint on 429 responses instead of guessing a backoff.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id or TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup)

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            async with session.post(url, json=payload, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                if data.get("ok"):
                    return data.get("result", {}).get("message_id")

                # Telegram signals rate limiting via ok=False + error_code=429 +
                # parameters.retry_after (seconds to wait before retrying).
                retry_after = (data.get("parameters") or {}).get("retry_after")
                if data.get("error_code") == 429 and retry_after is not None and attempt < max_attempts:
                    wait_s = min(float(retry_after), 30) + 0.5
                    log.warning(f"Telegram rate-limited (429); retrying in {wait_s:.1f}s (attempt {attempt}/{max_attempts})")
                    await asyncio.sleep(wait_s)
                    continue

                log.error(f"Telegram error: {data}")
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_attempts:
                backoff = 1.5 * attempt
                log.warning(f"Telegram send failed ({e}); retrying in {backoff:.1f}s (attempt {attempt}/{max_attempts})")
                await asyncio.sleep(backoff)
                continue
            log.error(f"Telegram send failed after {max_attempts} attempts: {e}")
            return None
        except Exception as e:
            log.error(f"Telegram send failed: {e}")
            return None
    return None

async def broadcast_signal(session: aiohttp.ClientSession, text: str, state: dict, reply_markup: dict = None) -> dict:
    """سیگنال رو به چت اصلی + همه کانال/گروه‌های فعال ارسال میکند.
    اگر چت اصلی (TELEGRAM_CHAT_ID) خودش هم در لیست channels ثبت شده باشد (یعنی با /stop
    داخل همان چت غیرفعال شده باشد)، دیگر به آن ارسال نمی‌شود — نه به‌صورت پیش‌فرض هاردکد.

    Returns a dict {chat_id_str: message_id} for every chat the text was
    successfully sent to (previously returned only the main chat's
    message_id as a bare int — widened so the chart-image feature can reply
    to the correct signal message in EVERY channel, not just the main chat;
    every existing caller either ignores the return value or only ever
    needed the main chat's id, which is still available at
    result[str(TELEGRAM_CHAT_ID)])."""
    channels = state.get("channels", [])
    main_chat_entry = next((c for c in channels if str(c["id"]) == str(TELEGRAM_CHAT_ID)), None)

    sent_message_ids: dict = {}

    # چت اصلی را فقط در صورتی بفرست که یا اصلاً در لیست channels ثبت نشده (یعنی هنوز
    # کنترل نشده و به‌صورت پیش‌فرض فعال است)، یا ثبت شده ولی active=True باشد.
    if main_chat_entry is None or main_chat_entry.get("active", True):
        mid = await send_telegram(session, text, TELEGRAM_CHAT_ID, reply_markup=reply_markup)
        if mid:
            sent_message_ids[str(TELEGRAM_CHAT_ID)] = mid

    for ch in channels:
        if str(ch["id"]) == str(TELEGRAM_CHAT_ID):
            continue  # همین الان بالاتر مدیریت شد، دوباره نفرستیم
        if ch.get("active", True):
            mid = await send_telegram(session, text, str(ch["id"]), reply_markup=reply_markup)
            if mid:
                sent_message_ids[str(ch["id"])] = mid
            await asyncio.sleep(0.1)

    return sent_message_ids

async def send_telegram_photo(session: aiohttp.ClientSession, photo_bytes: bytes, caption: str,
                               chat_id: str = None, reply_markup: dict = None,
                               reply_to_message_id: int = None) -> Optional[int]:
    """Sends a PNG chart with an optional caption + inline buttons, mirroring
    send_telegram()'s retry-on-429 behavior. Returns the sent message_id, or
    None on failure — callers must treat None as "photo send failed" and
    fall back to the existing text-only broadcast_signal(), never blocking
    signal delivery. `reply_to_message_id` lets the photo reply to the
    original signal text message, matching Telegram's native reply-thread
    UI (same mechanism send_telegram() already uses for TP/SL/MISSED
    notifications)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            form = aiohttp.FormData()
            form.add_field("chat_id", str(chat_id or TELEGRAM_CHAT_ID))
            if caption:
                form.add_field("caption", caption)
                form.add_field("parse_mode", "HTML")
            if reply_to_message_id is not None:
                form.add_field("reply_to_message_id", str(reply_to_message_id))
            if reply_markup is not None:
                form.add_field("reply_markup", json.dumps(reply_markup))
            form.add_field("photo", photo_bytes, filename="chart.png", content_type="image/png")
            async with session.post(url, data=form, proxy=PROXY,
                                    timeout=aiohttp.ClientTimeout(total=20)) as r:
                data = await r.json()
                if data.get("ok"):
                    return data.get("result", {}).get("message_id")
                retry_after = (data.get("parameters") or {}).get("retry_after")
                if data.get("error_code") == 429 and retry_after is not None and attempt < max_attempts:
                    wait_s = min(float(retry_after), 30) + 0.5
                    log.warning(f"Telegram photo rate-limited (429); retrying in {wait_s:.1f}s")
                    await asyncio.sleep(wait_s)
                    continue
                log.error(f"Telegram sendPhoto error: {data}")
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_attempts:
                await asyncio.sleep(1.5 * attempt)
                continue
            log.error(f"Telegram sendPhoto failed after {max_attempts} attempts: {e}")
            return None
        except Exception as e:
            log.error(f"Telegram sendPhoto failed: {e}")
            return None
    return None

async def broadcast_signal_photo(session: aiohttp.ClientSession, photo_bytes: bytes, caption: str,
                                  state: dict, reply_markup: dict = None) -> Optional[int]:
    """Photo equivalent of broadcast_signal(): sends the chart+caption+button
    to the main chat + all active channels/groups."""
    channels = state.get("channels", [])
    main_chat_entry = next((c for c in channels if str(c["id"]) == str(TELEGRAM_CHAT_ID)), None)

    main_message_id = None
    if main_chat_entry is None or main_chat_entry.get("active", True):
        main_message_id = await send_telegram_photo(session, photo_bytes, caption, TELEGRAM_CHAT_ID,
                                                      reply_markup=reply_markup)

    for ch in channels:
        if str(ch["id"]) == str(TELEGRAM_CHAT_ID):
            continue
        if ch.get("active", True):
            await send_telegram_photo(session, photo_bytes, caption, str(ch["id"]), reply_markup=reply_markup)
            await asyncio.sleep(0.1)

    return main_message_id

async def send_outcome_reply(session: aiohttp.ClientSession, signal_id: int, symbol: str,
                              direction: str, event: str, pnl_percent: Optional[float]):
    """
    ارسال reply به پیام اصلی سیگنال هنگام Entry Touch، TP1، TP2، Stop Loss، یا MISSED.
    event: 'entry' | 'tp1' | 'tp2' | 'sl_after_tp1' | 'sl_before_tp1' | 'missed'
    """
    notif = get_signal_notification_row(signal_id)
    if not notif:
        log.debug(f"send_outcome_reply: no notification row for signal_id={signal_id}")
        return
    tg_message_id = notif.get("tg_message_id")
    if not tg_message_id:
        log.debug(f"send_outcome_reply: no tg_message_id for signal_id={signal_id}")
        return

    # بررسی ارسال نشدن قبلی برای جلوگیری از duplicate
    if event != "entry" and notif.get("trade_closed"):
        return
    if event == "entry" and notif.get("entry_notified"):
        return
    if event == "tp1" and notif.get("tp1_notified"):
        return
    if event == "tp2" and notif.get("tp2_notified"):
        return
    if event in ("sl_after_tp1", "sl_before_tp1") and notif.get("sl_notified"):
        return
    if event == "missed" and notif.get("missed_notified"):
        return

    signal_emoji = "🟢" if direction == "BUY" else "🔴"
    pnl_str = ""
    if pnl_percent is not None:
        sign = "+" if pnl_percent >= 0 else ""
        pnl_str = f"{sign}{pnl_percent:.2f}%"

    if event == "entry":
        text = (
            f"{signal_emoji} {symbol}\n\n"
            f"📍 ENTRY TOUCHED\n\n"
            f"Trade is now ACTIVE.\n"
            f"Waiting for TP1 / TP2 / Stop Loss."
        )
    elif event == "missed":
        text = (
            f"{signal_emoji} {symbol}\n\n"
            f"⚠️ TRADE MISSED\n\n"
            f"Price reached target before entry."
        )
    elif event == "tp1":
        text = (
            f"{signal_emoji} {symbol}\n\n"
            f"✅ TP1 TOUCHED\n\n"
            f"Profit: {pnl_str}\n\n"
            f"Trade is still OPEN.\n"
            f"Waiting for TP2 or Stop Loss."
        )
    elif event == "tp2":
        text = (
            f"{signal_emoji} {symbol}\n\n"
            f"🎯 TP2 TOUCHED\n\n"
            f"Final Profit: {pnl_str}\n\n"
            f"Trade CLOSED successfully."
        )
    elif event == "sl_after_tp1":
        text = (
            f"{signal_emoji} {symbol}\n\n"
            f"⚠️ STOP LOSS TOUCHED\n\n"
            f"Trade CLOSED after TP1.\n\n"
            f"Final Result:\n"
            f"{pnl_str}"
        )
    elif event == "sl_before_tp1":
        sl_emoji = "🔴"
        text = (
            f"{sl_emoji} {symbol}\n\n"
            f"❌ STOP LOSS TOUCHED\n\n"
            f"Trade CLOSED.\n\n"
            f"Loss:\n"
            f"{pnl_str}"
        )
    else:
        return

    # Append the signal's existing Signal ID (never generated/changed here)
    # at the very end, with two blank lines before it, so the user can
    # always identify exactly which signal this follow-up belongs to.
    sig_row = get_signal_by_id(signal_id)
    sig_uid = sig_row.get("signal_uid") if sig_row else None
    text += f"\n\n🆔 <code>{sig_uid if sig_uid else signal_id}</code>"

    try:
        await send_telegram(session, text, TELEGRAM_CHAT_ID, reply_to_message_id=tg_message_id)
        trade_closed = event in ("tp2", "sl_after_tp1", "sl_before_tp1", "missed")
        if event == "entry":
            event_key = "entry"
        elif event == "missed":
            event_key = "missed"
        elif event == "tp1":
            event_key = "tp1"
        elif event == "tp2":
            event_key = "tp2"
        else:
            event_key = "sl"
        mark_notification_sent(signal_id, event_key, trade_closed=trade_closed)
        log.info(f"Outcome reply sent: signal_id={signal_id} symbol={symbol} event={event}")
    except Exception as e:
        log.error(f"send_outcome_reply failed for signal_id={signal_id}: {e}")

# ─── PENDING SIGNAL MANAGEMENT + OVERRIDE (admin, manual result entry) ────────
# Both features funnel through the SAME apply_manual_trade_result() below,
# which writes to the exact same `results` columns, using the exact same
# status vocabulary and pnl/rr formulas, as update_trade_outcomes() (the
# automatic monitor) — so calc_strategy_stats(), the daily report, and
# /replay all treat a manual result identically to a real one. The manual
# actions deliberately reuse the pre-existing LEGACY terminal statuses
# ("TP1"/"TP2"/"SL") rather than the monitor's own fine-grained internal
# ones ("TP1_HIT"/"TP2_HIT"/"SL_HIT") for one important reason: the
# monitor's re-processing query is `WHERE status IN ('PENDING','OPEN',
# 'ENTERED','TP1_HIT')` — using the legacy aliases means a manually-closed
# signal is automatically excluded from ever being re-picked-up and
# overwritten by a later real price movement, with zero extra code needed.
# "TP1 THEN BREAK EVEN" and "TRADE MISSED" already have dedicated statuses
# (TP1_TOUCHSL / MISSED) that mean exactly this, so those are reused as-is.
MANUAL_RESULT_ACTIONS = {
    "TP1":         {"label": "✅ TP1 TOUCHED",          "raw_status": "TP1",         "event": "tp1"},
    "TP2":         {"label": "✅✅ TP2 TOUCHED",         "raw_status": "TP2",         "event": "tp2"},
    "SL":          {"label": "❌ STOP LOSS TOUCHED",    "raw_status": "SL",          "event": "sl_before_tp1"},
    "MISSED":      {"label": "⚠️ TRADE MISSED",         "raw_status": "MISSED",      "event": "missed"},
    "TP1_TOUCHSL": {"label": "🔄 TP1 THEN BREAK EVEN",  "raw_status": "TP1_TOUCHSL", "event": "sl_after_tp1"},
}
# Any status (pending or already closed) can be looked up for /override —
# only Pending Signal Management restricts the *listing* to open ones.
OPEN_RAW_STATUSES = {"OPEN", "PENDING", "ENTERED", "TP1_HIT"}

def get_pending_signals(limit: int = 30) -> list[dict]:
    """Pending Signal Management (feature): every signal with no final result
    yet, oldest first (per spec) — same OPEN_RAW_STATUSES the rest of the bot
    already uses to mean 'still open'."""
    conn = get_db_connection()
    try:
        placeholders = ",".join("?" for _ in OPEN_RAW_STATUSES)
        rows = conn.execute(f"""
            SELECT s.id, s.symbol, s.direction, s.strategy, s.opened_at, s.signal_uid, r.status
            FROM signals s JOIN results r ON r.signal_id = s.id
            WHERE r.status IN ({placeholders})
            ORDER BY s.id ASC
            LIMIT ?
        """, (*OPEN_RAW_STATUSES, limit)).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()

def get_signal_by_uid(signal_uid: str) -> Optional[dict]:
    """Override (feature): resolves a human-facing Signal ID (e.g. 250803-03)
    back to its DB row, for ANY signal regardless of status."""
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy, s.entry, s.stop, s.tp1, s.tp2,
                   s.opened_at, s.signal_uid, r.status, r.pnl_percent, r.closed_at
            FROM signals s JOIN results r ON r.signal_id = s.id
            WHERE s.signal_uid = ?
        """, (signal_uid,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def get_signal_by_id(signal_id: int) -> Optional[dict]:
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy, s.entry, s.stop, s.tp1, s.tp2,
                   s.opened_at, s.signal_uid, r.status, r.pnl_percent, r.closed_at
            FROM signals s JOIN results r ON r.signal_id = s.id
            WHERE s.id = ?
        """, (signal_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

async def apply_manual_trade_result(session: aiohttp.ClientSession, signal_id: int, action_key: str,
                                     admin_uid: int, state: dict) -> Optional[dict]:
    """Pending Signal Management / Override (feature): applies a manual final
    result through EXACTLY the same DB columns, status vocabulary, and
    pnl/rr formulas update_trade_outcomes() uses for automatic results —
    see MANUAL_RESULT_ACTIONS above for why. The only difference is the
    audit columns (manual_result=1, manual_by=<admin telegram id>,
    manual_at=<timestamp>), so a manual intervention is always traceable
    without affecting how stats/reports/strategy performance read it.
    Works on ANY signal (open or already closed — /override), not just
    pending ones. Returns a summary dict, or None if the signal doesn't
    exist or the action is invalid."""
    action = MANUAL_RESULT_ACTIONS.get(action_key)
    if not action:
        return None

    async with _db_lock:
        conn = get_db_connection()
        try:
            row = conn.execute("""
                SELECT s.id, s.symbol, s.direction, s.entry, s.stop, s.tp1, s.tp2, s.signal_uid,
                       r.entered_at
                FROM signals s JOIN results r ON r.signal_id = s.id WHERE s.id = ?
            """, (signal_id,)).fetchone()
            if not row:
                return None

            entry, stop, tp1, tp2, direction = row["entry"], row["stop"], row["tp1"], row["tp2"], row["direction"]
            risk_pct = abs(entry - stop) / entry * 100 if entry else 0
            now_iso = datetime.now(timezone.utc).isoformat()

            def pnl_for(level: float) -> float:
                return ((level - entry) / entry * 100) if direction == "BUY" else ((entry - level) / entry * 100)

            raw_status = action["raw_status"]
            tp1_hit, tp2_hit = 0, 0
            close_price = None
            pnl_percent = None
            rr_multiple = None
            entered_at = row["entered_at"]

            if raw_status == "TP1":
                tp1_hit, close_price = 1, tp1
                pnl_percent = round(pnl_for(tp1), 2)
                entered_at = entered_at or now_iso
            elif raw_status == "TP2":
                tp1_hit, tp2_hit, close_price = 1, 1, tp2
                pnl_percent = round(pnl_for(tp2), 2)
                entered_at = entered_at or now_iso
            elif raw_status == "SL":
                close_price = stop
                pnl_percent = round(pnl_for(stop), 2)
                entered_at = entered_at or now_iso
            elif raw_status == "TP1_TOUCHSL":
                tp1_hit, close_price = 1, tp1
                pnl_percent = round(pnl_for(tp1), 2)
                entered_at = entered_at or now_iso
            # raw_status == "MISSED": no entry, no pnl — matches automatic MISSED exactly.

            if pnl_percent is not None:
                rr_multiple = round((pnl_percent / risk_pct) if risk_pct > 0 else 0, 3)

            conn.execute(
                "UPDATE results SET status=?, entered_at=?, tp1_hit=?, tp2_hit=?, "
                "closed_at=?, close_price=?, pnl_percent=?, rr_multiple=?, close_reason=?, "
                "manual_result=1, manual_by=?, manual_at=? WHERE signal_id=?",
                (raw_status, entered_at, tp1_hit, tp2_hit, now_iso, close_price,
                 pnl_percent, rr_multiple, "manual_override", str(admin_uid), now_iso, signal_id)
            )
            conn.commit()
        finally:
            conn.close()

    log.info(
        f"Manual result applied | signal_id={signal_id} signal_uid={row['signal_uid']} "
        f"{row['symbol']} {direction} -> {raw_status} by admin_uid={admin_uid}"
    )

    # Same notification path automatic results use — send_outcome_reply()
    # already guards against duplicate/already-closed notifications on its
    # own, so this is safe to call even for a signal that was previously
    # closed automatically and is now being corrected via /override.
    try:
        await send_outcome_reply(session, signal_id, row["symbol"], direction, action["event"], pnl_percent)
    except Exception as e:
        log.error(f"Manual result notification failed for signal_id={signal_id}: {e}")

    # Keep the live daily report in sync immediately, same as an automatic close would.
    try:
        await sync_daily_report(session, state, load_trades())
    except Exception as e:
        log.error(f"sync_daily_report error (manual result): {e}")

    return {
        "signal_id": signal_id, "signal_uid": row["signal_uid"], "symbol": row["symbol"],
        "direction": direction, "new_status": raw_status, "pnl_percent": pnl_percent,
        "label": action["label"],
    }

def pending_signals_kb(rows: list[dict]) -> dict:
    kb_rows = []
    for r in rows:
        dir_emoji = "🟢" if r["direction"] == "BUY" else "🔴"
        label = f"{dir_emoji} {r['symbol']} · {r['strategy']} · #{r.get('signal_uid') or r['id']}"
        kb_rows.append([{"text": label, "callback_data": f"psig_open:{r['id']}"}])
    kb_rows.append([{"text": "🔄 Refresh", "callback_data": "pending_signals_menu"}])
    kb_rows.append([{"text": "◀️ Back", "callback_data": "main_menu"}])
    return {"inline_keyboard": kb_rows}

def manual_result_actions_kb(signal_id: int, back_callback: str) -> dict:
    kb_rows = [[{"text": a["label"], "callback_data": f"mres:{signal_id}:{key}"}]
               for key, a in MANUAL_RESULT_ACTIONS.items()]
    kb_rows.append([{"text": "◀️ Back", "callback_data": back_callback}])
    return {"inline_keyboard": kb_rows}

def build_message(
    symbol: str,
    direction: str,          # "BUY" or "SELL"
    strategy: str,
    price: float,
    entry: float,
    stop: float,
    tp1: float,
    tp2: float,
    timeframe: str,
    score: int,
    rsi: Optional[float] = None,
    market_regime: Optional[str] = None,
    tp2_potential: int = None,
    tp2_unlikely: bool = False,
    signal_uid: Optional[str] = None,
) -> tuple:
    """Builds the outbound signal text. VISUAL LAYOUT ONLY was redesigned here
    (per the new reference mockup) — every field is still the exact same
    data, computed the exact same way, from the exact same inputs as before.
    Nothing is added or removed: fields are simply grouped two-per-line
    (Symbol/Signal, Strategy/Price, Regime/Timeframe, Entry/Stop, TP1/TP2,
    TP2 Potential/Score) to match the compact card look of the reference
    layout, since Telegram messages don't support real columns/backgrounds —
    this is the closest approximation achievable with Telegram's HTML subset.
    `signal_uid` stays wrapped in <code> so tapping it in Telegram copies it
    directly."""
    now_utc = datetime.now(timezone.utc)
    time_str = now_utc.strftime("%Y-%m-%d %H:%M UTC")

    signal_emoji = "🟢" if direction == "BUY" else "🔴"
    signal_text  = "𝗕𝗨𝗬 𝗦𝗜𝗚𝗡𝗔𝗟" if direction == "BUY" else "𝗦𝗘𝗟𝗟 𝗦𝗜𝗚𝗡𝗔𝗟"
    # BUG FIX (Problem 4/chart-market mismatch): "BINANCE:{symbol}" on
    # TradingView resolves to the Binance SPOT market. This bot trades and
    # monitors Binance USDⓈ-M Futures (BINANCE_BASE = fapi.binance.com) exclusively,
    # so the chart link must open the matching Futures perpetual market
    # (TradingView symbol suffix ".P"), not Spot.
    tv_link = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P"

    decimals = 8 if price < 0.01 else (4 if price < 10 else 2)

    def fmt(v): return f"{v:.{decimals}f}"

    # ── Optional metadata rows (UI-only fix): reference mockup pairs
    # Market Regime with Timeframe on one line, not RSI with Regime — same
    # two data points as before, just regrouped. RSI (when present) now gets
    # its own separate optional line above the Regime/Timeframe line instead
    # of being merged into it. No data added/removed/changed, only grouping. ──
    rsi_line = f"📐 <b>RSI:</b> {rsi}\n" if rsi is not None else ""
    if market_regime:
        regime_timeframe_line = f"🌐 <b>Market Regime:</b> {market_regime}   📈 <b>Timeframe:</b> {timeframe}\n"
    else:
        regime_timeframe_line = f"📈 <b>Timeframe:</b> {timeframe}\n"

    # ── TP2 Potential + Signal Score: paired on one line for the compact
    # single-line variant (matches the reference layout); the "High
    # probability..." / "Only TP1 is targeted" variants carry their own
    # second line, so they stay full-width with Score directly under them —
    # same text, same wording, same conditions as before. ──
    score_field = f"⭐️ <b>Signal Score:</b> {score}/100"
    if tp2_unlikely:
        tp2_score_block = (
            f"⚠️ <b>TP2 Potential Low</b> — Only TP1 is targeted.\n"
            f"{score_field}\n"
        )
    elif tp2_potential is not None:
        if tp2_potential >= TP2_POTENTIAL_THRESHOLD:
            tp2_stars = "🚀" if tp2_potential >= 80 else "⚡️"
            tp2_score_block = (
                f"{tp2_stars} <b>TP2 Potential:</b> {tp2_potential}/100\n"
                f"High probability of reaching TP2\n"
                f"{score_field}\n"
            )
        else:
            tp2_score_block = f"🎯 <b>TP2 Potential:</b> {tp2_potential}/100   {score_field}\n"
    else:
        tp2_score_block = f"{score_field}\n"

    signal_id_block = f"🆔 <b>Signal ID</b>\n<code>{signal_uid}</code>\n\n" if signal_uid else ""

    msg = (
        f"📡 <b>Auto Trading Signal | AI Analysis 🤖</b>\n"
        f"🐍 <b>𝐀𝐈 𝐀𝐅𝐄𝐄 𝐓𝐑𝐀𝐃𝐄𝐑</b> 🐍\n\n"
        f"💎 <b>Symbol:</b> {symbol}   {signal_emoji} <b>Signal:</b> {signal_text}\n"
        f"📊 <b>Strategy:</b> {strategy}   🔰 <b>Price:</b> {fmt(price)} USDT\n"
        f"{rsi_line}"
        f"{regime_timeframe_line}\n"
        f"💰 <b>Entry:</b> {fmt(entry)}   🛑 <b>Stop:</b> {fmt(stop)}\n"
        f"🎯 <b>TP1:</b> {fmt(tp1)} (1.5R)   🎯 <b>TP2:</b> {fmt(tp2)} (3R)\n\n"
        f"{tp2_score_block}\n"
        f"{signal_id_block}"
        f"🕒 <b>Time:</b> {time_str}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<blockquote>🤖 This signal is automatically generated by the advanced AI trading robot "
        f"𝐀𝐅𝐄𝐄 𝐓𝐑𝐀𝐃𝐄𝐑 based on real-time data analysis.</blockquote>\n\n"
        f"@AFEETRADER"
    )
    return msg, tv_link

# ─── BINANCE DATA ─────────────────────────────────────────────────────────────
# FIX (rate-limit centralization): every Binance REST call in this bot now goes
# through binance_request() below — ONE centralized wrapper instead of each
# function independently duplicating (or, in several places, entirely
# skipping) 429/418 handling. Workers coordinate via one shared asyncio.Event
# ("the gate"): a rate-limit/ban response clears the gate, every in-flight and
# future call awaits it (so nothing "floods" Binance during a pause instead of
# just quietly no-op-ing), exactly one warning is logged per pause episode
# (concurrent 429/418s arriving moments later just silently extend the same
# deadline), and a single background watcher re-opens the gate automatically
# once the cooldown elapses. Binance's own Retry-After header is honored when
# present; otherwise the pause grows exponentially (5s, 10s, 20s, ... capped at
# 300s) with consecutive failures, instead of a flat guess.
#
# Nothing below changes what data any caller receives on success (same parsed
# JSON shapes as before) or how any strategy/filter/scoring function
# interprets that data — this is purely the transport layer underneath them.
_binance_pause_event = asyncio.Event()
_binance_pause_event.set()          # not paused initially
_binance_backoff_until = 0.0        # epoch seconds; informational + resume-watcher target
_binance_consecutive_failures = 0   # drives exponential backoff when no Retry-After header present

def _binance_backoff_active() -> bool:
    """Informational read of pause state (e.g. for status/log messages).
    binance_request() below is what actually gates requests — this is not
    itself a synchronization primitive."""
    return time.time() < _binance_backoff_until

async def _binance_resume_watcher():
    """Spawned exactly once per pause episode (only on the not-paused ->
    paused transition — see _register_binance_rate_limit). Re-checks the
    deadline instead of sleeping once, so a later 429/418 arriving while
    already paused can extend the cooldown without spawning a second
    watcher or logging a second warning."""
    while True:
        remaining = _binance_backoff_until - time.time()
        if remaining <= 0:
            break
        await asyncio.sleep(min(remaining, 5.0))
    _binance_pause_event.set()
    log.info("Binance rate-limit/ban cooldown elapsed — resuming requests.")

def _register_binance_rate_limit(retry_after_header) -> None:
    """Called the instant any centralized Binance request sees 429/418.
    Fully synchronous (no `await` inside), so it's atomic with respect to
    every other coroutine under asyncio's cooperative scheduling — no lock
    needed. Honors Retry-After when Binance sends it; otherwise applies
    exponential backoff based on consecutive rate-limit hits."""
    global _binance_backoff_until, _binance_consecutive_failures
    wait_s = None
    if retry_after_header is not None:
        try:
            wait_s = float(retry_after_header)
        except (TypeError, ValueError):
            wait_s = None

    if wait_s is None:
        # No Retry-After header — back off exponentially instead of guessing
        # a single flat duration: 5s, 10s, 20s, 40s, 80s, 160s, capped at 300s.
        _binance_consecutive_failures += 1
        wait_s = 5.0 * (2 ** (_binance_consecutive_failures - 1))
    else:
        # Exchange gave us an explicit deadline — trust it and reset our own
        # escalation counter so a single isolated rate-limit hit doesn't
        # permanently inflate future backoffs.
        _binance_consecutive_failures = 0

    wait_s = max(5.0, min(wait_s, 300.0))  # bound between 5s and 5min — never wait forever, never ignore it
    until = time.time() + wait_s

    if until > _binance_backoff_until:
        was_already_paused = not _binance_pause_event.is_set()
        _binance_backoff_until = until
        if not was_already_paused:
            # ONE warning per pause episode. Any other request that hits
            # 429/418 while we're already paused just silently extends
            # `_binance_backoff_until` above — the running resume watcher
            # picks up the new deadline on its next check, no duplicate log,
            # no duplicate task.
            log.warning(f"Binance rate-limit/ban response — pausing ALL Binance calls for {wait_s:.0f}s")
            _binance_pause_event.clear()
            asyncio.create_task(_binance_resume_watcher())

async def binance_request(session: aiohttp.ClientSession, url: str, params: dict = None,
                           timeout: int = 10):
    """
    THE single entry point for every Binance REST call in this bot (klines,
    tickers, book ticker, open interest, funding rate, symbol validation —
    every one of them). Before issuing a request, every caller awaits the
    shared pause gate: if Binance is currently rate-limiting/banning this IP,
    every caller here simply waits (does not proceed, does not retry-hammer)
    until the cooldown lifts, then proceeds automatically.

    Returns the parsed JSON body on success, or None on any failure (network
    error, non-200 status, 429/418, malformed JSON) — every existing caller
    already treats "no data" as its normal transient-failure case, so
    centralizing this doesn't change any calling/strategy code's behavior
    beyond removing duplicated (or, previously, entirely missing) status
    handling.
    """
    await _binance_pause_event.wait()
    try:
        async with session.get(url, params=params, proxy=PROXY,
                                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status in (429, 418):
                _register_binance_rate_limit(r.headers.get("Retry-After"))
                return None
            if r.status != 200:
                return None
            try:
                return await r.json()
            except Exception:
                return None
    except Exception as e:
        log.debug(f"Binance request error ({url}): {e}")
        return None

async def get_live_futures_price(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    """
    BUG FIX (Problem 3, price mismatch): every strategy function set the
    "price" field of its result dict to the close of whatever higher
    timeframe candle it happened to be inspecting (e.g. the 1H or 4H candle
    used for S/R or pattern detection on that strategy's own timeframe) —
    NOT the actual current market price at the moment the signal fires. That
    close can legitimately be minutes to hours old by the time the signal is
    generated and sent, which is exactly the kind of "bot Price vs chart
    Price" mismatch reported. This is a pure display value only — it is not
    persisted (log_trade/signals table has no price column) and is not read
    by any entry/stop/TP/filter/scoring/monitoring logic — so replacing it
    with a fresh read here changes nothing about strategy behavior. Uses the
    same fapi (Futures) ticker/price endpoint as every other price source in
    this bot, keeping signal generation and the displayed price on the same
    exchange/market/source.
    """
    data = await binance_request(session, f"{BINANCE_BASE}/ticker/price", params={"symbol": symbol}, timeout=10)
    if not isinstance(data, dict) or "price" not in data:
        return None
    try:
        return float(data["price"])
    except (TypeError, ValueError):
        return None

async def get_top_symbols(session: aiohttp.ClientSession, n: int = 100) -> list[str]:
    """Return top N USDT perpetual futures pairs by 24h quote volume, excluding stablecoins.
    Uses Binance Futures API (fapi/v1). Symbols are plain BTCUSDT format
    (Binance REST does NOT use .P suffix; .P is a TradingView convention only).
    """
    STABLE = {"USDC", "BUSD", "TUSD", "DAI", "FDUSD", "USDP", "USDD", "USDT"}
    data = await binance_request(session, f"{BINANCE_BASE}/ticker/24hr", timeout=15)
    if data is None:
        return []
    try:
        if not isinstance(data, list):
            log.error(f"Unexpected /ticker/24hr response type: {type(data)} — {str(data)[:200]}")
            return []
        usdt = [
            d for d in data
            if isinstance(d, dict)
            and d.get("symbol", "").endswith("USDT")
            and not any(d.get("symbol", "").startswith(s) for s in STABLE)
            and float(d.get("quoteVolume", 0) or 0) > 0
        ]
        usdt.sort(key=lambda x: float(x.get("quoteVolume", 0) or 0), reverse=True)
        return [d["symbol"] for d in usdt[:n]]
    except Exception as e:
        log.error(f"get_top_symbols failed: {e}")
        return []

# ─── SYMBOL QUALITY RANKING (مورد ۶) ───────────────────────────────────────────
async def get_symbol_spread_quality(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    کیفیت اسپرد: فاصله bid/ask نسبت به قیمت میانی. هرچه اسپرد کمتر (نسبت به قیمت)،
    کیفیت بالاتر. خروجی بین ۰ تا ۱ (۱ = بهترین، اسپرد نزدیک صفر).
    """
    try:
        # Futures API: /fapi/v1/ticker/bookTicker — same field names as spot
        data = await binance_request(session, f"{BINANCE_BASE}/ticker/bookTicker",
                                      params={"symbol": symbol}, timeout=8)
        if not isinstance(data, dict) or "bidPrice" not in data:
            return 0.3
        bid, ask = float(data["bidPrice"]), float(data["askPrice"])
        mid = (bid + ask) / 2
        if mid == 0:
            return 0.5
        spread_pct = (ask - bid) / mid * 100
        # نرمال‌سازی: اسپرد ۰٪ → امتیاز ۱.۰  |  اسپرد ۰.5٪ یا بیشتر → امتیاز ۰.۰
        return max(0.0, min(1.0, 1.0 - (spread_pct / 0.5)))
    except Exception:
        return 0.3  # در صورت خطا، امتیاز محافظه‌کارانه پایین (نه صفر، نه کامل)

def calc_structure_cleanliness(candles: list[dict]) -> float:
    """
    نظم ساختاری بازار: تعداد سطوح S/R معتبر (با حداقل ۲ برخورد) که در داده تشخیص داده می‌شود.
    بازاری با چند سطح S/R واضح، «تمیزتر» و قابل تحلیل‌تر از بازاری با نویز کامل یا بدون هیچ سطحی است.
    خروجی بین ۰ تا ۱.
    """
    if len(candles) < 30:
        return 0.5
    levels = find_sr_levels(candles, lookback=min(50, len(candles)))
    # ۲ تا ۶ سطح S/R معتبر ایده‌آل است؛ صفر سطح (بی‌ساختار) یا تعداد بیش‌ازحد (نویزی) امتیاز کمتر می‌گیرد
    count = len(levels)
    if count == 0:
        return 0.2
    elif 2 <= count <= 6:
        return 1.0
    elif count == 1 or count == 7:
        return 0.7
    else:
        return 0.4

def calc_trend_clarity(candles: list[dict]) -> float:
    """
    وضوح روند: بر اساس ADX. روند خیلی واضح (قوی) یا رنج خیلی واضح (بدون روند) هر دو قابل تحلیل‌اند؛
    ابهام‌برانگیزترین حالت، ADX میانه (نه قوی نه ضعیف، حدود ۲۰-۲۵) است که نه استراتژی ترند نه ریورسال
    مطمئن کار می‌کند. خروجی بین ۰ تا ۱.
    """
    if len(candles) < 30:
        return 0.5
    adx = calc_adx(candles, 14)
    if adx >= 25 or adx <= 15:
        return 1.0   # روند واضح یا رنج واضح — هر دو قابل تحلیل
    elif 15 < adx < 20 or 25 > adx >= 22:
        return 0.7
    else:
        return 0.4    # ناحیه خاکستری ۲۰-۲۲ — مبهم‌ترین حالت

def calc_volatility_quality(candles: list[dict]) -> float:
    """
    کیفیت نوسان: نه بازار خیلی بی‌حرکت (سیگنال‌های کم‌سود)، نه خیلی پرنوسان (SL راحت می‌خورد).
    بر اساس ATR نسبت به قیمت (ATR درصدی). خروجی بین ۰ تا ۱.
    """
    if len(candles) < 20:
        return 0.5
    atr = calc_atr(candles, 14)
    price = candles[-1]["close"]
    if price == 0:
        return 0.5
    atr_pct = (atr / price) * 100
    # محدوده ایده‌آل نوسان: ۰.۵٪ تا ۳٪ ATR نسبت به قیمت (بسته به نوع کوین، نسبی است)
    if 0.5 <= atr_pct <= 3.0:
        return 1.0
    elif 0.2 <= atr_pct < 0.5 or 3.0 < atr_pct <= 5.0:
        return 0.6
    else:
        return 0.25   # خیلی بی‌حرکت یا خیلی پرنوسان

async def calc_symbol_quality_score(session: aiohttp.ClientSession, symbol: str) -> dict:
    """
    امتیاز کیفیت کلی یک نماد، ترکیبی از ۴ معیار:
    spread quality, volatility quality, structure cleanliness, trend clarity.
    خروجی: دیکشنری شامل امتیاز نهایی (۰ تا ۱۰۰) و جزئیات هر معیار.
    """
    candles = await get_candles(session, symbol, "1h", 100)
    if len(candles) < 30:
        return {"symbol": symbol, "quality_score": 0, "valid": False}

    spread_q = await get_symbol_spread_quality(session, symbol)
    structure_q = calc_structure_cleanliness(candles)
    trend_q = calc_trend_clarity(candles)
    volatility_q = calc_volatility_quality(candles)

    # ترکیب وزن‌دار: ساختار و وضوح روند برای استراتژی‌های تکنیکال این ربات اهمیت بیشتری دارند
    composite = (spread_q * 0.20) + (volatility_q * 0.25) + (structure_q * 0.30) + (trend_q * 0.25)
    score = round(composite * 100)

    return {
        "symbol": symbol, "quality_score": score, "valid": True,
        "spread_quality": round(spread_q, 2), "volatility_quality": round(volatility_q, 2),
        "structure_cleanliness": round(structure_q, 2), "trend_clarity": round(trend_q, 2),
    }

async def get_quality_ranked_symbols(session: aiohttp.ClientSession, final_n: int = 100,
                                      pool_size: int = 250) -> list[str]:
    """
    انتخاب نمادها در دو مرحله (مورد ۶ - Symbol Quality Ranking):
    ۱. ابتدا `pool_size` نماد برتر بر اساس حجم معاملات ۲۴ ساعته انتخاب می‌شوند (نامزدهای اولیه).
    ۲. سپس برای همین نامزدها، امتیاز کیفیت (spread/volatility/structure/trend) محاسبه می‌شود
       و فقط `final_n` نماد با بالاترین کیفیت برای اسکن نهایی انتخاب می‌شوند.
    """
    candidates = await get_top_symbols(session, pool_size)

    semaphore = asyncio.Semaphore(PARALLEL_WORKERS)
    async def scored(symbol):
        async with semaphore:
            return await calc_symbol_quality_score(session, symbol)

    results = await asyncio.gather(*[scored(s) for s in candidates], return_exceptions=True)
    valid_results = [r for r in results if isinstance(r, dict) and r.get("valid")]
    valid_results.sort(key=lambda r: r["quality_score"], reverse=True)

    return [r["symbol"] for r in valid_results[:final_n]]

# ═══════════════════════════════════════════════════════════════════════════
# HISTORICAL STRATEGY REPLAY — candle-provider shim
# ═══════════════════════════════════════════════════════════════════════════
# `_replay_ctx` is a contextvars.ContextVar, NOT a global/module-level
# variable — setting it only affects the current asyncio Task (and tasks
# created from it afterward), never sibling tasks already running. This is
# what makes it safe to run a Replay Backtest job concurrently with live
# scanning: live scanning's tasks never call `_replay_ctx.set(...)`, so
# `_replay_ctx.get()` is always None for them, and get_candles() always takes
# the normal live Binance path below — this cannot leak historical data into
# a live scan, and cannot be affected by one running.
_replay_ctx: contextvars.ContextVar[Optional["HistoricalCandleCache"]] = contextvars.ContextVar(
    "_replay_ctx", default=None
)

class HistoricalCandleCache:
    """
    Per-replay-job cache of pre-fetched candle series, keyed by (symbol,
    interval). Fetched ONCE per (symbol, interval) for the whole job — every
    strategy needing the same timeframe for the same symbol (e.g. Stop
    Hunter, HB, and Trigger Fibonacci all need "1h") reads the same cached
    array, never re-fetches.

    `sim_time_ms` is advanced by the walk-forward loop as it steps through
    simulated time. get_candles_as_of() returns exactly the candles a live
    get_candles(symbol, interval, limit) call would have returned "as of"
    that moment — i.e. only candles whose close_time <= sim_time_ms, most
    recent `limit` of them — using a monotonically-advancing index pointer
    per (symbol, interval) for O(1) amortized lookups instead of re-scanning.
    """
    def __init__(self):
        self.series: dict[tuple[str, str], list[dict]] = {}
        self._pointer: dict[tuple[str, str], int] = {}
        self.sim_time_ms: int = 0

    def load(self, symbol: str, interval: str, candles: list[dict]):
        key = (symbol, interval)
        self.series[key] = candles
        self._pointer[key] = 0

    def get_candles_as_of(self, symbol: str, interval: str, limit: int) -> list[dict]:
        key = (symbol, interval)
        arr = self.series.get(key)
        if not arr:
            return []
        n = len(arr)
        p = self._pointer.get(key, 0)
        # Advance the pointer forward past any candle that has now closed
        # (monotonic — sim_time_ms only ever increases within one job run).
        while p < n and arr[p]["close_time"] is not None and arr[p]["close_time"] <= self.sim_time_ms:
            p += 1
        self._pointer[key] = p
        # arr[:p] are all candles closed by sim_time_ms, in chronological order.
        return arr[max(0, p - limit):p]


async def get_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    limit: int = 200,
) -> list[dict]:
    """
    Fetch klines and return list of OHLCV dicts, containing ONLY closed candles.

    BUG FIX (audit, critical): Binance's /klines endpoint always returns the
    currently-forming (unclosed) candle as the last element in the response.
    Its close/high/low/volume are still changing in real time. Every strategy
    in this file reads candles[-1] as "the last completed candle" for entry
    logic, pattern detection (is_hammer/is_shooting_star), close confirmation,
    volume checks, and trend direction. Feeding it a live, incomplete candle
    causes repainting: a hammer/breakout/close-above-level that appears valid
    mid-candle can silently disappear or flip by the time the candle actually
    closes. This is very likely the main source of inconsistent win rates
    across symbols, since which symbols get "caught mid-candle" at scan time
    is effectively random timing noise, not a strategy signal.

    Fix: request one extra candle from Binance and drop the last one
    (guaranteed unclosed), plus a defensive close_time check.

    Now goes through binance_request() — ONE centralized wrapper shared by
    every Binance call in the bot — which coordinates a shared pause gate
    across all concurrent workers instead of each call independently
    checking/guessing, and applies exponential backoff when Binance doesn't
    send a Retry-After header.

    HISTORICAL REPLAY SHIM: if a HistoricalCandleCache is active in this
    task's context (only true inside a Replay Backtest job — see
    `_replay_ctx` above), candles are served from the pre-fetched historical
    series "as of" the job's simulated time instead of hitting Binance. Every
    strategy function calls this same get_candles() unmodified, so replay
    runs the REAL strategy logic — this shim is the only thing that changes.
    """
    _cache = _replay_ctx.get()
    if _cache is not None:
        return _cache.get_candles_as_of(symbol, interval, limit)

    fetch_limit = limit + 1  # +1 to compensate for dropping the unclosed candle
    params = {"symbol": symbol, "interval": interval, "limit": fetch_limit}
    try:
        raw = await binance_request(session, f"{BINANCE_BASE}/klines", params=params, timeout=10)
        if not raw:
            return []

        now_ms = int(time.time() * 1000)
        candles = []
        for k in raw:
            close_time_ms = k[6] if len(k) > 6 else None
            candles.append({
                "open_time":  k[0],
                "open":       float(k[1]),
                "high":       float(k[2]),
                "low":        float(k[3]),
                "close":      float(k[4]),
                "volume":     float(k[5]),
                "close_time": close_time_ms,
            })

        # Drop the currently-forming candle. Binance guarantees the last
        # element is unclosed, but we double-check via close_time as a
        # safety net (defensive against exchange edge cases).
        if candles and (candles[-1]["close_time"] is None or candles[-1]["close_time"] > now_ms):
            candles = candles[:-1]

        return candles[-limit:]
    except Exception as e:
        log.debug(f"Candle fetch error {symbol} {interval}: {e}")
        return []

# ─── TECHNICAL HELPERS ────────────────────────────────────────────────────────
def find_sr_levels(candles: list[dict], lookback: int = 50, tolerance: float = 0.003) -> list[float]:
    """
    Find S&R levels from highs/lows with at least 2 touches.
    Also detects simple Order Block regions.
    """
    highs = [c["high"] for c in candles[-lookback:]]
    lows  = [c["low"]  for c in candles[-lookback:]]
    levels: list[float] = []

    def is_pivot_high(i):
        return highs[i] == max(highs[max(0,i-5):i+6])

    def is_pivot_low(i):
        return lows[i] == min(lows[max(0,i-5):i+6])

    pivots: list[float] = []
    for i in range(5, len(highs)-5):
        if is_pivot_high(i): pivots.append(highs[i])
        if is_pivot_low(i):  pivots.append(lows[i])

    # cluster pivots that are within tolerance
    used = [False]*len(pivots)
    for i, p in enumerate(pivots):
        if used[i]: continue
        cluster = [p]
        for j in range(i+1, len(pivots)):
            if not used[j] and abs(pivots[j]-p)/p < tolerance:
                cluster.append(pivots[j])
                used[j] = True
        if len(cluster) >= 2:
            levels.append(sum(cluster)/len(cluster))
        used[i] = True

    return sorted(levels)

# ─── S&R WEIGHT SYSTEM (Patch #2) ─────────────────────────────────────────────
SR_SCORE_3_TOUCHES   = 90
SR_SCORE_ORDER_BLOCK = 85
SR_SCORE_SUPPLY_DEMAND = 80
SR_SCORE_2_TOUCHES   = 65

def apply_zone_confluence(scored_levels_a: list[dict], scored_levels_b: list[dict],
                           tolerance: float = 0.008, bonus: int = 10) -> list[dict]:
    """
    Multi-timeframe zone-quality confluence (item 5, additive only — per
    instruction, never a hard rejection gate). If a level found on timeframe
    A sits within `tolerance` of a level independently found on timeframe B,
    that's real structural confluence — both timeframes' price action
    respects roughly the same level — so it gets a bounded sr_score bonus.

    Mutates and returns `scored_levels_a` with `sr_score` boosted (capped at
    100) and an added `confluence` flag where applicable; `scored_levels_b`
    is read-only. Does not remove, merge, or reject any level — every level
    from `scored_levels_a` is still returned, just re-scored. Strategies
    that don't call this behave exactly as before (fully opt-in).

    Uses get_nearest_sr_scored() (defined below — Python resolves this at
    call time, so the forward reference is safe).
    """
    for lvl in scored_levels_a:
        lvl.setdefault("confluence", False)
        match = get_nearest_sr_scored(lvl["price"], scored_levels_b, tol=tolerance)
        if match is not None:
            lvl["sr_score"] = min(100, lvl["sr_score"] + bonus)
            lvl["confluence"] = True
    return scored_levels_a

def find_sr_levels_scored(candles: list[dict], lookback: int = 50, tolerance: float = 0.003) -> list[dict]:
    """
    Enhanced S&R detection that returns scored levels.
    Each level dict: {"price": float, "sr_score": int, "type": str, "touches": int,
                       "freshness": float, "consumed": bool}
    Score: 3+ touches=90, Order Block=85, Supply/Demand=80, 2 touches=65,
    then adjusted by freshness/consumption (see below).

    ZONE-QUALITY AUDIT (item 4): freshness and consumption were previously not
    tracked at all — a zone last touched 2 candles ago and one last touched
    48 candles ago scored identically, and a zone price has already closed
    through repeatedly (degraded/"used up") scored the same as one that's
    never been broken. Added as bounded, additive score adjustments only —
    no new hard-reject rule, per "do not introduce arbitrary rejection rules".
    """
    highs = [c["high"] for c in candles[-lookback:]]
    lows  = [c["low"]  for c in candles[-lookback:]]
    closes= [c["close"] for c in candles[-lookback:]]
    opens = [c["open"]  for c in candles[-lookback:]]
    n = len(highs)
    scored_levels: list[dict] = []

    def is_pivot_high(i):
        return highs[i] == max(highs[max(0,i-5):i+6])

    def is_pivot_low(i):
        return lows[i] == min(lows[max(0,i-5):i+6])

    pivots: list[tuple[float, int]] = []  # (price, candle_index) — index needed for freshness
    for i in range(5, len(highs)-5):
        if is_pivot_high(i): pivots.append((highs[i], i))
        if is_pivot_low(i):  pivots.append((lows[i], i))

    used = [False]*len(pivots)
    for i, (p, p_idx) in enumerate(pivots):
        if used[i]: continue
        cluster = [p]
        cluster_idxs = [p_idx]
        for j in range(i+1, len(pivots)):
            pj, j_idx = pivots[j]
            if not used[j] and abs(pj-p)/p < tolerance:
                cluster.append(pj)
                cluster_idxs.append(j_idx)
                used[j] = True
        if len(cluster) >= 2:
            level_price = sum(cluster)/len(cluster)
            touches = len(cluster)
            # Detect order block: strong opposite candle before the level
            is_ob = False
            for k in range(5, len(highs)-5):
                if abs(highs[k] - level_price)/level_price < tolerance or abs(lows[k] - level_price)/level_price < tolerance:
                    body = abs(closes[k] - opens[k])
                    avg_b = sum(abs(closes[m]-opens[m]) for m in range(max(0,k-10),k)) / max(1, min(k,10))
                    if avg_b > 0 and body >= 2 * avg_b:
                        is_ob = True
                        break
            # FIX (strategy audit): Supply/Demand zone detection was missing —
            # SR_SCORE_SUPPLY_DEMAND was defined and documented above but never
            # assigned, so the S/R detector could only ever return Order Block,
            # 3+ Touches, or 2 Touches levels even though the spec explicitly
            # lists Supply & Demand as one of the required zone types. A
            # supply/demand zone is a narrow-range "base" candle at the level
            # immediately followed by a strong impulsive candle departing from
            # it (the classic base-and-breakout footprint).
            is_sd = False
            if not is_ob:
                for k in range(5, len(highs)-6):
                    if abs(highs[k] - level_price)/level_price < tolerance or abs(lows[k] - level_price)/level_price < tolerance:
                        base_body = abs(closes[k] - opens[k])
                        avg_b = sum(abs(closes[m]-opens[m]) for m in range(max(0,k-10),k)) / max(1, min(k,10))
                        next_body = abs(closes[k+1] - opens[k+1])
                        if avg_b > 0 and base_body <= 0.6 * avg_b and next_body >= 2 * avg_b:
                            is_sd = True
                            break
            if is_ob:
                sr_score = SR_SCORE_ORDER_BLOCK
                sr_type  = "Order Block"
            elif is_sd:
                sr_score = SR_SCORE_SUPPLY_DEMAND
                sr_type  = "Supply/Demand"
            elif touches >= 3:
                sr_score = SR_SCORE_3_TOUCHES
                sr_type  = "3+ Touches"
            else:
                sr_score = SR_SCORE_2_TOUCHES
                sr_type  = "2 Touches"

            # ── Freshness: how recently (within lookback) was this level
            # actually touched? A level whose last touch was near the edge
            # of the lookback window is "stale" — the market may have moved
            # on. Bounded +/-6 adjustment, never enough to flip a level's
            # tier on its own.
            most_recent_touch_idx = max(cluster_idxs)
            recency_frac = most_recent_touch_idx / max(1, n - 1)  # 0=oldest, 1=most recent candle
            freshness_adj = round((recency_frac - 0.5) * 12)  # range: -6 .. +6

            # ── Consumption: how many candles CLOSED decisively beyond this
            # level (by more than `tolerance`) after it was first formed? A
            # level repeatedly closed through is a "used up" zone — price no
            # longer respects it the way an intact zone would. Bounded
            # penalty, capped so it can't zero out a strong zone alone.
            first_touch_idx = min(cluster_idxs)
            # Proxy: count later candles whose full body crossed cleanly
            # through the level (open and close on opposite sides) — each
            # such crossing is one "use" of the zone.
            crossings = 0
            for k in range(first_touch_idx + 1, n - 1):
                if (opens[k] - level_price) * (closes[k] - level_price) < 0:
                    crossings += 1
            consumed = crossings >= 3
            consumption_adj = -min(8, crossings * 2)  # -2 per crossing, capped at -8

            sr_score = max(10, sr_score + freshness_adj + consumption_adj)
            scored_levels.append({"price": level_price, "sr_score": sr_score,
                                   "type": sr_type, "touches": touches,
                                   "freshness": round(recency_frac, 3), "consumed": consumed})
        used[i] = True

    return sorted(scored_levels, key=lambda x: x["price"])

def get_nearest_sr_scored(price: float, scored_levels: list[dict], tol: float = 0.008) -> Optional[dict]:
    """Return the nearest scored S&R level within tolerance, or None."""
    for sl in scored_levels:
        if abs(price - sl["price"]) / sl["price"] < tol:
            return sl
    return None

def is_near_level(price: float, levels: list[float], tol: float = 0.005) -> Optional[float]:
    for lv in levels:
        if abs(price - lv) / lv < tol:
            return lv
    return None

def swing_high(candles: list[dict], n: int = 5) -> float:
    return max(c["high"] for c in candles[-n:])

def swing_low(candles: list[dict], n: int = 5) -> float:
    return min(c["low"] for c in candles[-n:])

def avg_body(candles: list[dict], n: int = 20) -> float:
    return sum(abs(c["close"]-c["open"]) for c in candles[-n:]) / n

def avg_volume(candles: list[dict], n: int = 20) -> float:
    return sum(c["volume"] for c in candles[-n:]) / n

def has_sufficient_entry_volume(candles: list[dict], ma_period: int = 20, multiplier: float = 1.5) -> bool:
    """
    فیلتر حجم کندل ورود (مورد ۱۰ — مستقل از فیلتر حجم مشکوک):
    فقط زمانی True برمی‌گرداند که حجم آخرین کندل (کندل ورود) حداقل `multiplier` برابر
    میانگین حجم `ma_period` کندل اخیر باشد. شرط پیش‌فرض: Volume >= 1.5 × SMA(Volume, 20).
    اگر داده کافی نباشد، محافظه‌کارانه True برمی‌گرداند (فیلتر اعمال نمی‌شود).
    """
    if len(candles) < ma_period + 1:
        return True
    baseline = candles[-(ma_period + 1):-1]  # ma_period کندل قبل از کندل آخر، بدون خود کندل آخر
    try:
        avg_vol = avg_volume(baseline, ma_period)
        entry_volume = candles[-1]["volume"]
    except (KeyError, TypeError):
        # malformed candle data (missing 'volume') — same conservative
        # default as the insufficient-data case above: don't block on it.
        return True
    if avg_vol == 0:
        return True
    return entry_volume >= (multiplier * avg_vol)

def entry_volume_ratio(candles: list[dict], ma_period: int = 20) -> float:
    """نسبت حجم کندل ورود به میانگین — برای استفاده در امتیازدهی (Score) سیگنال."""
    if len(candles) < ma_period + 1:
        return 1.0
    baseline = candles[-(ma_period + 1):-1]
    avg_vol = avg_volume(baseline, ma_period)
    if avg_vol == 0:
        return 1.0
    return candles[-1]["volume"] / avg_vol

def is_strong_candle(c: dict, prev_candles: list[dict]) -> bool:
    """
    Strong candle (per spec, exact wording):
        (big body >= 2x prior-20 avg AND small opposite wick)
        OR
        (volume >= 1.5x average)
        OR
        (full engulfing of the previous candle's range)

    Any ONE of the three qualifies — this is an OR of independent triggers,
    not a threshold count. A candle that is strong on volume alone, or on
    engulfing alone, must qualify even with a normal body/wick.

    FIX (audit #2): the previous implementation required "at least 2 of 4"
    sub-flags (treating body/wick as two separate flags instead of one
    combined clause), which silently rejected candles that satisfied only
    the volume clause or only the engulfing clause on their own — directly
    contradicting the spec's OR wording. Rewritten as a literal OR of the
    three spec clauses.
    """
    body = abs(c["close"] - c["open"])
    if body == 0:
        return False
    ab = avg_body(prev_candles)
    av = avg_volume(prev_candles)
    if ab == 0:
        return False

    big_body   = body >= 2 * ab
    big_volume = av > 0 and c["volume"] >= 1.5 * av

    is_bullish = c["close"] > c["open"]
    opposite_wick = (c["high"] - max(c["open"], c["close"])) if is_bullish \
        else (min(c["open"], c["close"]) - c["low"])
    small_opposite_wick = opposite_wick < 0.30 * body

    prev = prev_candles[-1] if prev_candles else None
    engulfs = False
    if prev:
        prev_range = prev["high"] - prev["low"]
        engulfs = (c["high"] >= prev["high"] and c["low"] <= prev["low"]) if prev_range > 0 else False

    return (big_body and small_opposite_wick) or big_volume or engulfs

def is_body_engulfing(c: dict, prev: dict) -> bool:
    """
    HB strategy trigger (per clarified spec): a Bullish/Bearish Engulfing
    candle, defined strictly by candle BODY (open/close), not wicks.

    `c` must be the opposite color to `prev`, and `c`'s body must fully
    cover `prev`'s body on both ends (open and close). Wick engulfing is
    explicitly NOT required.
    """
    c_body_hi, c_body_lo = max(c["open"], c["close"]), min(c["open"], c["close"])
    p_body_hi, p_body_lo = max(prev["open"], prev["close"]), min(prev["open"], prev["close"])
    if c_body_hi == c_body_lo:
        return False
    c_bullish = c["close"] > c["open"]
    p_bullish = prev["close"] > prev["open"]
    if c_bullish == p_bullish:
        return False  # must be opposite color to the previous candle
    return c_body_hi >= p_body_hi and c_body_lo <= p_body_lo

def has_suspicious_opposing_volume(candles: list[dict], direction: str, lookback: int = 5) -> bool:
    """
    فیلتر «حجم مشکوک»: بررسی می‌کند آیا در چند کندل آخر، یک حرکت قوی با حجم بالا
    دقیقاً در جهت مخالف سیگنال اتفاق افتاده است (نشانه ورود نقدینگی بزرگ مخالف ما،
    که معمولاً به استاپ خوردن سیگنال منجر می‌شود).
    اگر چنین حرکتی پیدا شود، True برمی‌گرداند یعنی سیگنال باید رد (فیلتر) شود.
    """
    if len(candles) < lookback + 20:
        return False
    recent = candles[-lookback:]
    baseline = candles[-(lookback + 20):-lookback]
    avg_vol = avg_volume(baseline, 20)
    if avg_vol == 0:
        return False

    for c in recent:
        body = abs(c["close"] - c["open"])
        is_bearish = c["close"] < c["open"]
        is_bullish = c["close"] > c["open"]
        high_volume = c["volume"] >= 1.8 * avg_vol  # حجم به‌طور غیرعادی بالا

        if not high_volume or body == 0:
            continue

        # سیگنال BUY است ولی حرکت قوی نزولی با حجم بالا دیده شده → مشکوک
        if direction == "BUY" and is_bearish:
            return True
        # سیگنال SELL است ولی حرکت قوی صعودی با حجم بالا دیده شده → مشکوک
        if direction == "SELL" and is_bullish:
            return True

    return False

def is_hammer(c: dict) -> bool:
    """Bullish hammer: small body at top, long lower shadow >= 2x body."""
    body   = abs(c["close"] - c["open"])
    total  = c["high"] - c["low"]
    if total == 0 or body == 0: return False
    lower_shadow = min(c["open"], c["close"]) - c["low"]
    upper_shadow = c["high"] - max(c["open"], c["close"])
    return (lower_shadow >= 2 * body and upper_shadow <= body * 0.5
            and c["close"] > c["open"])

def is_shooting_star(c: dict) -> bool:
    """Bearish shooting star: small body at bottom, long upper shadow >= 2x body."""
    body   = abs(c["close"] - c["open"])
    if body == 0: return False
    upper_shadow = c["high"] - max(c["open"], c["close"])
    lower_shadow = min(c["open"], c["close"]) - c["low"]
    return (upper_shadow >= 2 * body and lower_shadow <= body * 0.5
            and c["close"] < c["open"])

def calc_rsi(closes: list[float], period: int = 14) -> list[float]:
    """محاسبه RSI استاندارد (Wilder's smoothing) - برمیگرداند لیست RSI هم‌طول closes (مقادیر اول None)."""
    if len(closes) < period + 1:
        return [None] * len(closes)

    rsis: list[Optional[float]] = [None] * len(closes)
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
    rsis[period] = 100 - (100 / (1 + rs)) if avg_loss != 0 else 100.0

    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0)
        loss = max(-diff, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
        rsis[i] = 100 - (100 / (1 + rs)) if avg_loss != 0 else 100.0

    return rsis

def fibonacci_level(high: float, low: float, ratio: float) -> float:
    # FIX (Fibonacci orientation bug): TradingView's Fib Retracement always
    # anchors 0% at the LOW and 100% at the HIGH, regardless of which point
    # was the start vs. end of the price swing (confirmed against a live
    # TradingView chart: 0=low, 1=high, 0.618 sits close to the high). The
    # previous formula (high - (high-low)*ratio) put the 0.618 level close
    # to the LOW instead — backwards. This affected only the SELL-side
    # Fibonacci callers (Hammer Fib SELL, Trigger Fibonacci SELL), which are
    # the two callers of this function; BUY already computed the correct
    # point directly (low + (high-low)*ratio) without going through this
    # helper, and is unchanged.
    return low + (high - low) * ratio

def calc_score(factors: list[bool], sr_score: int = None) -> int:
    """
    Score based on confirmation factors + optional S&R zone weight (Patch #2).
    If sr_score provided, it influences the final score proportionally.

    FIX v3.2: sr_score is now properly propagated from find_sr_levels_scored()
    into every strategy call via get_nearest_sr_scored(). Previously sr_score
    was never passed so the S&R weight had zero effect on the final score.
    """
    base = 50
    per_factor = 50 // max(len(factors), 1)
    raw = min(100, base + sum(factors) * per_factor)
    if sr_score is not None:
        # Blend raw score with sr_score weight (60/40 split)
        raw = round(raw * 0.6 + sr_score * 0.4)
    return min(100, max(1, raw))

# ─── MIN SIGNAL SCORE (runtime configurable via /setscore) ─────────────────────
_MIN_SIGNAL_SCORE_DEFAULT = 65

def get_min_signal_score(cfg: dict = None) -> int:
    """خواندن حداقل score سیگنال از bot_config.json (در صورت تنظیم /setscore) —
    همان مکانیزم پایدارسازی permanent settings که Strategy Settings استفاده
    می‌کند (نه filter_config دیتابیس جدا). Automatically restored after every
    restart, exactly like TOP_N_COINS / Dynamic Score thresholds / Strategy
    Settings.

    REPRODUCIBILITY FIX (root cause D): optional `cfg` override, same
    pattern as get_dynamic_score_threshold() — None (live scanning) reads
    bot_config.json fresh as before; Replay passes its frozen per-job
    snapshot instead.
    """
    cfg = cfg if cfg is not None else load_bot_config()
    raw = cfg.get("min_signal_score", _MIN_SIGNAL_SCORE_DEFAULT)
    try:
        return max(1, min(100, int(raw)))
    except Exception:
        return _MIN_SIGNAL_SCORE_DEFAULT

def set_min_signal_score(value: int) -> None:
    """ذخیره دائمی min_signal_score در bot_config.json — دقیقاً همان الگوی
    atomic-write که /config برای TOP_N_COINS و بقیه تنظیمات دائمی استفاده
    می‌کند، تا با ری‌استارت ربات از دست نرود.

    NOTE: this is now only the legacy GLOBAL fallback value, used by a
    strategy only until that strategy has its own min_signal_score set via
    /setscore (see get_strategy_min_score() / set_strategy_min_score()
    below, which is what the live scanner and Historical Backtest actually
    read per-strategy)."""
    cfg_now = load_bot_config()
    cfg_now["min_signal_score"] = int(value)
    tmp_cfg_file = BOT_CONFIG_FILE + ".tmp"
    with open(tmp_cfg_file, "w", encoding="utf-8") as _cf:
        json.dump(cfg_now, _cf, indent=2, ensure_ascii=False)
    os.replace(tmp_cfg_file, BOT_CONFIG_FILE)


# ─── PER-STRATEGY MIN SIGNAL SCORE (runtime configurable via /setscore panel) ──
# Task 3 replacement: /setscore is now a per-strategy interactive panel
# instead of one global number. The value lives inside each strategy's own
# entry in state["strategy_settings"][strategy]["min_signal_score"] (see
# STRATEGY_FILTER_DEFAULTS above), persisted through the exact same
# save_state()/bot_config.json mechanism every other Strategy Settings knob
# already uses — no new storage, no new file.
#
# get_strategy_min_score() is what BOTH the live scanning pipeline
# (run_live_filters' Score gate + scan_symbol's post-filter re-check) and
# Historical Backtest/Replay (_replay_symbol's final min-score gate) call —
# there is no other code path left that decides "is this signal's score high
# enough", so setting a strategy's score here genuinely changes what that
# strategy is allowed to signal, live and in backtests alike.
def get_strategy_min_score(state: dict, strategy: str, cfg: dict = None) -> int:
    """Effective minimum score for ONE strategy: that strategy's own
    min_signal_score if it has ever been set via /setscore, otherwise the
    legacy global get_min_signal_score() value (so upgrading users keep
    their current behavior for every strategy they haven't touched yet).

    REPRODUCIBILITY FIX (root cause D): optional `cfg` override forwarded to
    the get_min_signal_score() fallback — see that function's docstring."""
    strat_cfg = get_strategy_settings(state, strategy)
    raw = strat_cfg.get("min_signal_score")
    if raw is None:
        return get_min_signal_score(cfg)
    try:
        return max(1, min(100, int(raw)))
    except Exception:
        return get_min_signal_score(cfg)


def set_strategy_min_score(state: dict, strategy: str, value: int) -> None:
    """Permanently sets a strategy's OWN minimum score. Stored via
    set_strategy_setting() -> state["strategy_settings"][strategy], which
    save_state() persists into bot_config.json (not bot_runtime.json), so
    it survives restarts exactly like every other Strategy Settings field."""
    set_strategy_setting(state, strategy, "min_signal_score", int(value))

# ─── SIGNAL VALIDATION (Patch #5) ─────────────────────────────────────────────
MIN_RISK_THRESHOLD = 0.0001  # حداقل فاصله نسبی Entry-Stop (0.01%)

def validate_signal_rr(result: dict) -> tuple[bool, str]:
    """
    Patch #5: Validate signal math before sending.
    Rules:
      - Entry ≠ Stop
      - Risk > minimum threshold
      - Reward (to TP1) > Risk
      - RR must be mathematically valid (no division by zero, no negative reward)
    Returns (is_valid, reason_if_invalid).
    """
    entry = result.get("entry", 0)
    stop  = result.get("stop", 0)
    tp1   = result.get("tp1", 0)
    tp2   = result.get("tp2", 0)
    direction = result.get("direction", "BUY")

    if entry == 0 or stop == 0 or tp1 == 0:
        return False, "zero_price_values"

    if abs(entry - stop) < 1e-12:
        return False, "entry_equals_stop"

    risk = abs(entry - stop)
    risk_pct = risk / entry if entry > 0 else 0

    if risk_pct < MIN_RISK_THRESHOLD:
        return False, f"risk_too_small:{risk_pct:.6f}"

    # Check reward direction and magnitude
    if direction == "BUY":
        if tp1 <= entry:
            return False, "tp1_below_entry_for_buy"
        reward = tp1 - entry
    else:
        if tp1 >= entry:
            return False, "tp1_above_entry_for_sell"
        reward = entry - tp1

    if reward <= 0:
        return False, "zero_reward"

    rr = reward / risk
    if rr < 0.5:
        return False, f"rr_too_low:{rr:.2f}"

    return True, ""

def calc_atr(candles: list[dict], period: int = 14) -> float:
    """میانگین برد واقعی (ATR) — برای تشخیص نوسان غیرعادی بازار."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, prev_close = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
    return sum(trs[-period:]) / period

# ─── Patch #3b: Volume SMA20 check ───────────────────────────────────────────
def has_sufficient_volume_sma20(candles: list[dict], period: int = 20) -> bool:
    """
    Patch #3b: حجم کندل آخر باید بالاتر از SMA20 حجم باشد.
    بازارهای بی‌حجم که نویز هستند را فیلتر می‌کند.
    """
    if len(candles) < period + 1:
        return True  # داده کافی نیست، بلاک نمی‌کنیم
    vol_sma20 = sum(c["volume"] for c in candles[-(period+1):-1]) / period
    if vol_sma20 == 0:
        return True
    return candles[-1]["volume"] >= vol_sma20

# ─── Task 2 filter: Volume Filter "EMA20" option ──────────────────────────────
def has_volume_above_ema20(candles: list[dict], period: int = 20) -> bool:
    """Mirror of has_sufficient_volume_sma20() above, but compares the entry
    candle's volume against an EMA of volume instead of a simple average —
    the "Volume > EMA20" option of the new configurable Volume Filter."""
    if len(candles) < period + 1:
        return True  # داده کافی نیست، بلاک نمی‌کنیم (fail-open، مثل نسخه SMA)
    vols = [c["volume"] for c in candles[-(period + 1):]]
    ema_series = calc_ema(vols, period)
    if not ema_series or ema_series[-1] is None or ema_series[-1] == 0:
        return True
    return candles[-1]["volume"] >= ema_series[-1]

# ─── Patch #3c/3d: ATR dead market / crazy market filter ─────────────────────
def is_atr_too_low(candles: list[dict], period: int = 14, low_ratio: float = 0.4) -> bool:
    """
    Patch #3c: ATR فعلی خیلی پایین‌تر از میانگین تاریخی‌اش → بازار مرده → No Trade.
    low_ratio: اگر ATR < low_ratio * avg_historical_ATR باشد، True برمیگرداند.
    NOTE (doc correction): "historical_atr" below is NOT a long-run average —
    calc_atr() only ever averages the last `period` true ranges of whatever
    slice it's given, so historical_atr is really just the ~period-candle
    window immediately preceding the current window, not the full history.
    """
    if len(candles) < period * 3:
        return False
    current_atr = calc_atr(candles[-period*2:], period)
    historical_atr = calc_atr(candles[:-period], period)
    if historical_atr == 0:
        return False
    return current_atr < historical_atr * low_ratio

def is_atr_too_high(candles: list[dict], period: int = 14, high_ratio: float = 3.0) -> bool:
    """
    Patch #3d: ATR فعلی خیلی بالاتر از میانگین تاریخی → بازار بسیار پرنوسان → No Trade.
    high_ratio: اگر ATR > high_ratio * avg_historical_ATR باشد، True برمیگرداند.
    NOTE (doc correction): see is_atr_too_low() above — "historical_atr" is
    the ~period-candle window immediately preceding "current", not a true
    long-run historical average.
    """
    if len(candles) < period * 3:
        return False
    current_atr = calc_atr(candles[-period*2:], period)
    historical_atr = calc_atr(candles[:-period], period)
    if historical_atr == 0:
        return False
    return current_atr > historical_atr * high_ratio

def is_volatility_abnormal(candles: list[dict], period: int = 14, threshold: float = 2.2) -> bool:
    """
    اگر ATR لحظه‌ای (۳ کندل آخر) نسبت به ATR پایه (۱۴ کندل) بیش از حد بالا باشد،
    یعنی بازار غیرعادی پرنوسان شده — در این حالت SL راحت می‌خورد و بهتر است سیگنال رد شود.
    """
    if len(candles) < period + 5:
        return False
    baseline_atr = calc_atr(candles[:-3], period)
    if baseline_atr == 0:
        return False
    recent_ranges = [c["high"] - c["low"] for c in candles[-3:]]
    recent_avg_range = sum(recent_ranges) / len(recent_ranges)
    return recent_avg_range >= baseline_atr * threshold

# ─── Patch #4: Three-timeframe trend alignment (4H + 1H + 15m) ───────────────
def _get_trend_from_candles(candles: list[dict], sma_period: int = 20) -> str:
    """جهت روند از روی کندل‌ها: UP / DOWN / FLAT."""
    if len(candles) < sma_period + 1:
        return "FLAT"
    closes = [c["close"] for c in candles]
    sma = sum(closes[-sma_period:]) / sma_period
    price = closes[-1]
    if price > sma * 1.002:
        return "UP"
    elif price < sma * 0.998:
        return "DOWN"
    return "FLAT"

async def is_three_tf_aligned(session, symbol: str, direction: str) -> bool:
    """
    Patch #4: همسویی سه تایم‌فریم 4H / 1H / 15m.
    اگر جهت 4H و 1H هر دو مخالف Entry باشند → False (بلاک).
    اگر حداقل یکی از آن‌ها موافق باشد → True (عبور).
    برای استراتژی‌های Reversal این فیلتر را نرم‌تر اعمال می‌کنیم.
    """
    try:
        c4h = await get_candles(session, symbol, "4h", 25)
        c1h = await get_candles(session, symbol, "1h", 25)
        c15m = await get_candles(session, symbol, "15m", 25)
        trend_4h  = _get_trend_from_candles(c4h)
        trend_1h  = _get_trend_from_candles(c1h)
        trend_15m = _get_trend_from_candles(c15m)

        expected = "UP" if direction == "BUY" else "DOWN"
        opposite = "DOWN" if direction == "BUY" else "UP"

        # اگر هر دو 4H و 1H مخالف باشند → بلاک قطعی
        if trend_4h == opposite and trend_1h == opposite:
            return False
        return True
    except Exception:
        return True  # در صورت خطا، محافظه‌کارانه عبور می‌دهیم

async def is_aligned_with_higher_trend(session, symbol: str, direction: str) -> bool:
    """
    بررسی همسویی جهت سیگنال با روند تایم ۱ ساعته (میانگین ساده ۲۰ کندل آخر در مقابل قیمت فعلی).
    اگر سیگنال BUY باشد ولی روند ۱ساعته نزولی باشد (یا برعکس)، سیگنال ضدروند تشخیص داده و رد می‌شود.
    """
    c1h = await get_candles(session, symbol, "1h", 25)
    if len(c1h) < 20:
        return True  # داده کافی نیست، فیلتر را اعمال نمی‌کنیم (محافظه‌کارانه عبور می‌دهیم)
    closes = [c["close"] for c in c1h]
    sma20 = sum(closes[-20:]) / 20
    price = closes[-1]
    trend_up = price > sma20
    if direction == "BUY":
        return trend_up
    else:
        return not trend_up

# ─── MARKET REGIME ENGINE (موتور رژیم بازار) ──────────────────────────────────
# طبقه‌بندی استراتژی‌ها بر اساس نوع منطق‌شان — برای انتخاب استراتژی متناسب با رژیم فعلی بازار.
STRATEGY_REGIME_TYPE = {
    "Stop Hunter":         "HYBRID",    # هم در روند قوی هم در بازار رنج کاربرد دارد (شکست فیک)
    "Hammer Fib (4H)":     "REVERSAL",
    "Hammer Fib (1H)":     "REVERSAL",
    "HB":                  "REVERSAL",
    # STRATEGY CORRECTION: was "TREND", but the strategy's actual behaviour is
    # a reversal executed only after HTF trend confirmation (fade at a zone,
    # confirmed by a 3-candle reversal trigger + pullback), not a
    # trend-continuation entry. Reclassified to REVERSAL so the Market Regime
    # Engine gates it correctly (this affects which regimes allow it to run).
    "Trigger Fibonacci":   "REVERSAL",
    "Exhaustion":          "REVERSAL",
    "RSI Divergence":      "REVERSAL",
}

def calc_ema(closes: list[float], period: int) -> list[float]:
    """میانگین متحرک نمایی (EMA) — برمی‌گرداند لیست هم‌طول closes (مقادیر اول None)."""
    if len(closes) < period:
        return [None] * len(closes)
    emas: list[Optional[float]] = [None] * len(closes)
    multiplier = 2 / (period + 1)
    sma_seed = sum(closes[:period]) / period
    emas[period - 1] = sma_seed
    for i in range(period, len(closes)):
        emas[i] = (closes[i] - emas[i-1]) * multiplier + emas[i-1]
    return emas

# ─── Task 2 filter: EMA Filter (per-strategy, opt-in) ────────────────────────
def is_ema_filter_ok(closes: list[float], price: float, period: int, direction: str) -> bool:
    """
    EMA Filter: passes only if `price` is on the requested side of the EMA.
    direction "above" → price must be above the EMA; "below" → price must be
    below the EMA. Fails open (returns True) if there isn't enough data to
    compute the EMA yet, matching every other filter in run_live_filters().
    """
    ema_series = calc_ema(closes, period)
    if not ema_series or ema_series[-1] is None:
        return True
    ema_val = ema_series[-1]
    if direction == "below":
        return price < ema_val
    return price > ema_val

# ─── Auto EMA Trend Filter (dynamic mode, opt-in via ema_filter_mode="auto") ──
def is_ema_trend_filter_auto_ok(candles: list[dict], closes: list[float], price: float, period: int,
                                 signal_direction: str, atr_enabled: bool = True,
                                 atr_multiplier: float = 1.0) -> bool:
    """
    Auto EMA Trend Filter:
    - price above EMA  → only BUY signals pass
    - price below EMA  → only SELL signals pass
    - price inside the neutral zone EMA ± (ATR × atr_multiplier) → block everything
      (sideways/choppy market). If atr_enabled is False, the neutral zone is skipped
      entirely and only the above/below directional check applies.
    Fails open (returns True) if there isn't enough data to compute EMA yet, matching
    the convention of is_ema_filter_ok(). This is a separate, isolated function — it
    does not use or modify is_ema_filter_ok(), and the ATR multiplier here is a
    dedicated per-strategy value (ema_filter_auto_atr_multiplier), not the global
    atr_multiplier used by the abnormal-volatility filter elsewhere.
    """
    ema_series = calc_ema(closes, period)
    if not ema_series or ema_series[-1] is None:
        return True
    ema_val = ema_series[-1]

    if atr_enabled:
        atr_val = calc_atr(candles, 14)
        if atr_val > 0:
            neutral_band = atr_val * atr_multiplier
            if (ema_val - neutral_band) <= price <= (ema_val + neutral_band):
                return False  # neutral zone — sideways/choppy market

    if price > ema_val:
        return signal_direction == "BUY"
    return signal_direction == "SELL"

def calc_adx(candles: list[dict], period: int = 14) -> float:
    """
    محاسبه استاندارد ADX (Average Directional Index) — معیار قدرت روند، فارغ از جهت آن.
    ADX > 25 یعنی روند قوی (مناسب استراتژی‌های Trend)؛ ADX < 20 یعنی بازار رنج/بدون روند
    (مناسب استراتژی‌های Reversal). بین ۲۰ تا ۲۵ ناحیه خاکستری/گذار است.
    """
    if len(candles) < period * 2:
        return 0.0

    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(candles)):
        up_move = candles[i]["high"] - candles[i-1]["high"]
        down_move = candles[i-1]["low"] - candles[i]["low"]
        plus_dm.append(up_move if (up_move > down_move and up_move > 0) else 0)
        minus_dm.append(down_move if (down_move > up_move and down_move > 0) else 0)
        tr = max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - candles[i-1]["close"]),
            abs(candles[i]["low"] - candles[i-1]["close"])
        )
        trs.append(tr)

    def smooth(values, period):
        if len(values) < period:
            return []
        smoothed = [sum(values[:period])]
        for v in values[period:]:
            smoothed.append(smoothed[-1] - (smoothed[-1] / period) + v)
        return smoothed

    smoothed_tr = smooth(trs, period)
    smoothed_plus_dm = smooth(plus_dm, period)
    smoothed_minus_dm = smooth(minus_dm, period)

    if not smoothed_tr or len(smoothed_tr) != len(smoothed_plus_dm):
        return 0.0

    dx_values = []
    for i in range(len(smoothed_tr)):
        if smoothed_tr[i] == 0:
            continue
        plus_di = 100 * (smoothed_plus_dm[i] / smoothed_tr[i])
        minus_di = 100 * (smoothed_minus_dm[i] / smoothed_tr[i])
        di_sum = plus_di + minus_di
        if di_sum == 0:
            dx_values.append(0)
            continue
        dx = 100 * abs(plus_di - minus_di) / di_sum
        dx_values.append(dx)

    if len(dx_values) < period:
        return sum(dx_values) / len(dx_values) if dx_values else 0.0
    return sum(dx_values[-period:]) / period

def calc_ema200_slope(closes: list[float], lookback: int = 5) -> str:
    """
    شیب EMA200 طی چند کندل اخیر — جهت کلی روند بلندمدت را نشان می‌دهد.
    خروجی: "UP" (صعودی)، "DOWN" (نزولی)، یا "FLAT" (بدون شیب واضح).
    """
    emas = calc_ema(closes, 200)
    valid_emas = [e for e in emas[-lookback-1:] if e is not None]
    if len(valid_emas) < 2:
        return "FLAT"
    slope_pct = (valid_emas[-1] - valid_emas[0]) / valid_emas[0] * 100
    if slope_pct > 0.15:
        return "UP"
    elif slope_pct < -0.15:
        return "DOWN"
    return "FLAT"

def calc_atr_regime(candles: list[dict], period: int = 14, lookback_avg: int = 50) -> str:
    """
    رژیم ATR: ATR فعلی را با میانگین بلندمدت‌تر آن (مثلاً ۵۰ کندل) مقایسه می‌کند.
    خروجی: "HIGH" (نوسان بالاتر از معمول)، "LOW" (نوسان پایین‌تر از معمول)، یا "NORMAL".
    """
    if len(candles) < lookback_avg + period:
        return "NORMAL"
    current_atr = calc_atr(candles, period)
    historical_atrs = []
    step = max(1, (len(candles) - period) // lookback_avg)
    for i in range(period, len(candles) - period, step):
        historical_atrs.append(calc_atr(candles[:i+period], period))
    if not historical_atrs:
        return "NORMAL"
    avg_historical_atr = sum(historical_atrs) / len(historical_atrs)
    if avg_historical_atr == 0:
        return "NORMAL"
    ratio = current_atr / avg_historical_atr
    if ratio >= 1.4:
        return "HIGH"
    elif ratio <= 0.7:
        return "LOW"
    return "NORMAL"

def calc_volatility_regime(candles: list[dict], period: int = 20) -> str:
    """
    رژیم نوسان قیمتی بر اساس انحراف معیار بازده‌های لگاریتمی — مستقل از ATR (که بر مبنای High/Low است).
    خروجی: "HIGH", "LOW", یا "NORMAL".
    """
    if len(candles) < period + 1:
        return "NORMAL"
    closes = [c["close"] for c in candles[-(period+1):]]
    returns = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0:
            returns.append(math.log(closes[i] / closes[i-1]))
    if len(returns) < 2:
        return "NORMAL"
    mean_r = sum(returns) / len(returns)
    variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
    std = math.sqrt(variance)
    annualized_like = std * 100  # درصد ساده برای مقایسه نسبی، نه annualized واقعی

    if annualized_like >= 3.0:
        return "HIGH"
    elif annualized_like <= 0.8:
        return "LOW"
    return "NORMAL"

def calc_volume_regime(candles: list[dict], period: int = 20) -> str:
    """
    رژیم حجم معاملات: حجم میانگین چند کندل اخیر را نسبت به میانگین بلندمدت‌تر می‌سنجد.
    خروجی: "HIGH" (ورود نقدینگی بیشتر از معمول)، "LOW" (بازار بی‌رمق)، یا "NORMAL".
    """
    if len(candles) < period + 5:
        return "NORMAL"
    recent_avg = avg_volume(candles, 5)
    baseline_avg = avg_volume(candles[:-5], period)
    if baseline_avg == 0:
        return "NORMAL"
    ratio = recent_avg / baseline_avg
    if ratio >= 1.5:
        return "HIGH"
    elif ratio <= 0.6:
        return "LOW"
    return "NORMAL"

async def get_market_regime(session, symbol: str) -> dict:
    """
    تحلیل کامل رژیم بازار یک نماد، بر مبنای تایم ۱ ساعته (برای دیدگاه میان‌مدت، نه نویز کوتاه‌مدت).
    خروجی شامل تمام معیارها به‌همراه تصمیم نهایی «کدام دسته از استراتژی‌ها مجاز است».
    """
    candles = await get_candles(session, symbol, "1h", 250)
    if len(candles) < 60:
        # داده کافی نیست — محافظه‌کارانه همه نوع استراتژی را مجاز می‌کنیم
        return {
            "adx": 0, "ema200_slope": "FLAT", "atr_regime": "NORMAL",
            "volatility_regime": "NORMAL", "volume_regime": "NORMAL",
            "allowed_types": {"TREND", "REVERSAL", "HYBRID"},
            "regime_label": "UNKNOWN (insufficient data)",
        }

    closes = [c["close"] for c in candles]
    adx = calc_adx(candles, 14)
    ema_slope = calc_ema200_slope(closes)
    atr_regime = calc_atr_regime(candles)
    vol_regime = calc_volatility_regime(candles)
    volume_regime = calc_volume_regime(candles)

    # ── منطق اصلی: تعیین دسته‌های مجاز استراتژی بر اساس ADX ──
    # ADX > 25  → فقط استراتژی‌های Trend (+ Hybrid)
    # ADX < 18  → فقط استراتژی‌های Reversal (+ Hybrid)
    # 18≤ADX≤23 → ناحیه Whipsaw — Patch #5: No Trade
    # 23<ADX≤25 → ناحیه گذار؛ همه دسته‌ها مجاز
    if adx > 25:
        allowed_types = {"TREND", "HYBRID"}
        regime_label = f"TRENDING (ADX={adx:.1f})"
    elif adx >= 23:
        allowed_types = {"TREND", "REVERSAL", "HYBRID"}
        regime_label = f"TRANSITIONAL (ADX={adx:.1f})"
    elif adx >= 18:
        # Patch #5: ناحیه Whipsaw — هیچ استراتژی‌ای مجاز نیست
        allowed_types = set()
        regime_label = f"WHIPSAW (ADX={adx:.1f}) — No Trade Zone"
    else:
        allowed_types = {"REVERSAL", "HYBRID"}
        regime_label = f"RANGING (ADX={adx:.1f})"

    return {
        "adx": round(adx, 1), "ema200_slope": ema_slope, "atr_regime": atr_regime,
        "volatility_regime": vol_regime, "volume_regime": volume_regime,
        "allowed_types": allowed_types, "regime_label": regime_label,
    }

def calc_targets(entry: float, stop: float, direction: str):
    risk = abs(entry - stop)
    if direction == "BUY":
        tp1 = entry + risk * 1.5
        tp2 = entry + risk * 3.0
    else:
        tp1 = entry - risk * 1.5
        tp2 = entry - risk * 3.0
    return tp1, tp2

# ─── Patch #13: ATR-buffered SL with swing reference ─────────────────────────
def calc_safe_stop(candles: list[dict], direction: str, entry: float,
                   atr_period: int = 14, atr_buffer: float = 1.5) -> float:
    """
    Patch #13: محاسبه Stop Loss امن‌تر با استفاده از:
    1) آخرین Swing معتبر (کف/سقف اخیر)
    2) ATR buffer برای جلوگیری از استاپ‌های ناشی از Shadow/نویز

    اگر Stop محاسبه‌شده خیلی نزدیک به Entry باشد (< 0.3% فاصله)،
    فاصله را به 0.3% entry افزایش می‌دهیم.
    """
    if len(candles) < atr_period + 5:
        # fallback: همان روش قدیمی
        risk = entry * 0.015
        return (entry - risk) if direction == "BUY" else (entry + risk)

    atr = calc_atr(candles, atr_period)
    buffer = atr * atr_buffer

    if direction == "BUY":
        # Swing Low: کمترین Low در ۱۰ کندل آخر (به جز کندل فعلی)
        swing_low = min(c["low"] for c in candles[-11:-1])
        stop_candidate = swing_low - buffer
        # اطمینان از فاصله کافی از Entry
        min_dist = entry * 0.003
        if entry - stop_candidate < min_dist:
            stop_candidate = entry - min_dist
        return round(stop_candidate, 8)
    else:
        swing_high = max(c["high"] for c in candles[-11:-1])
        stop_candidate = swing_high + buffer
        min_dist = entry * 0.003
        if stop_candidate - entry < min_dist:
            stop_candidate = entry + min_dist
        return round(stop_candidate, 8)

# FIX (strategy audit): calc_safe_stop() always anchors SL to the swing
# high/low of the last 10 candles (excluding the current candle) plus an
# ATR buffer. That is a different reference point than what the four
# strategy specs actually call for — "behind the wick of the breakout
# candle" (Stop Hunter), "behind the high/low" of the fib swing (Hammer
# Fibonacci), "behind the candle high/low" of the pattern candle (HB), and
# "behind the first opposite candle" (Trigger Fibonacci). Each of those is a
# specific, named price level, not a generic 10-candle swing. This helper
# places the stop just beyond that specific reference level, using the same
# ATR-buffer style as calc_safe_stop for noise protection.
def calc_stop_from_reference(candles: list[dict], direction: str, reference: float,
                              atr_period: int = 14, atr_buffer: float = 1.5,
                              atr_enabled: bool = True) -> float:
    """
    SL placed 'behind' a specific spec-mandated reference price (e.g. a
    breakout-candle wick, a Fibonacci swing high/low, or a pattern candle's
    high/low) rather than the generic last-10-candle swing used by
    calc_safe_stop().

    ATR Stop Buffer (configurable per strategy, Admin Panel):
      atr_enabled=True  (default, preserves prior behavior): adds an ATR
        buffer (or a 0.3% fallback if ATR/history is unavailable) beyond
        the reference, using `atr_buffer` as the multiplier — so wick noise
        doesn't clip the stop.
      atr_enabled=False: SL is placed at EXACTLY the reference price, per
        the strategy's literal spec ("behind the wick", "behind the low",
        etc.), with no buffer added.
    """
    if not atr_enabled:
        return round(reference, 8)
    if len(candles) >= atr_period + 1:
        atr = calc_atr(candles, atr_period)
        buffer = atr * atr_buffer if atr > 0 else reference * 0.003
    else:
        buffer = reference * 0.003
    if direction == "BUY":
        return round(reference - buffer, 8)
    else:
        return round(reference + buffer, 8)

def _atr_buffer_settings(state: Optional[dict], strategy_name: str) -> tuple[bool, float]:
    """Reads the per-strategy ATR Stop Buffer (enabled, multiplier) config.
    Falls back to (True, 1.5) — the prior hardcoded behavior — if no state
    dict is available (e.g. called from a context that doesn't have it)."""
    if state is None:
        return True, 1.5
    cfg = get_strategy_settings(state, strategy_name)
    return cfg.get("atr_stop_buffer_enabled", True), cfg.get("atr_stop_multiplier", 1.5)

# ─── Patch #3a: Close Confirmation — ورود فقط با بسته‌شدن کندل تأیید ─────────
def has_close_confirmation(candles: list[dict], level: float, direction: str,
                           tolerance: float = 0.003) -> bool:
    """
    Patch #3a: ورود فقط زمانی معتبر است که کندل در جهت ورود بسته شده باشد.
    BUY:  کندل آخر باید بالاتر از level بسته شده باشد (نه فقط لمس)
    SELL: کندل آخر باید پایین‌تر از level بسته شده باشد
    """
    if not candles:
        return False
    last = candles[-1]
    if direction == "BUY":
        return last["close"] > level * (1 - tolerance) and last["close"] > last["open"]
    else:
        return last["close"] < level * (1 + tolerance) and last["close"] < last["open"]


# ─── STRATEGY AUDIT: Global Breakout Validation / Confirmation Candle ────────
# NOTE: has_close_confirmation() above is intentionally left unchanged — it is
# also used by strategy_exhaustion() and strategy_rsi_divergence(), both out
# of scope for this audit. The spec's mandatory global rule is stricter than
# that helper in two ways: (1) it requires the close to be strictly outside
# the zone, not merely within `tolerance` of it, and (2) it additionally
# requires at least 60% of the candle's BODY to close outside the zone. This
# new helper implements that rule and is used only by the four audited
# strategies (Stop Hunter, Hammer Fibonacci, HB, Trigger Fibonacci) at their
# respective breakout/confirmation points.
def is_valid_zone_breakout(candle: dict, zone_level: float, direction: str,
                            min_body_ratio: float = 0.6) -> bool:
    """
    Global Breakout Validation (spec-mandated):
      - Candle CLOSES outside the zone (not just a touch/wick).
      - At least `min_body_ratio` (60%) of the candle body closes outside
        the zone.
    Doubles as the Confirmation Candle check: candle color must match the
    signal direction.
    """
    body = abs(candle["close"] - candle["open"])
    if body == 0:
        return False
    if direction == "BUY":
        if not (candle["close"] > zone_level and candle["close"] > candle["open"]):
            return False
        body_low = min(candle["open"], candle["close"])
        outside_body = candle["close"] - max(body_low, zone_level)
        return (outside_body / body) >= min_body_ratio
    else:
        if not (candle["close"] < zone_level and candle["close"] < candle["open"]):
            return False
        body_high = max(candle["open"], candle["close"])
        outside_body = min(body_high, zone_level) - candle["close"]
        return (outside_body / body) >= min_body_ratio


def correction_size(candles: list[dict], n: int = 10) -> float:
    """Check if last move is a correction (20% of prior wave or ~38.2% fib)."""
    if len(candles) < n+5: return 0
    wave_high = max(c["high"] for c in candles[-(n+5):-5])
    wave_low  = min(c["low"]  for c in candles[-(n+5):-5])
    wave = wave_high - wave_low
    recent_high = max(c["high"] for c in candles[-5:])
    recent_low  = min(c["low"]  for c in candles[-5:])
    recent_move = abs(recent_high - recent_low)
    if wave == 0: return 0
    return recent_move / wave

# ─── CORRECTION SCORING (Patch #3) ────────────────────────────────────────────
CORRECTION_MIN   = 0.20    # minimum valid correction ratio (20% of wave)
CORRECTION_OPT   = 0.382   # optimal correction (38.2% Fibonacci)

def correction_score(ratio: float) -> tuple[bool, float]:
    """
    Returns (is_valid, score_multiplier) for a given pullback ratio.
    - below 20%  → invalid (False, 0)
    - 20%-38.2%  → valid but reduced score (True, 0.7)
    - 38.2%+     → full score (True, 1.0)
    """
    if ratio < CORRECTION_MIN:
        return False, 0.0
    elif ratio < CORRECTION_OPT:
        return True, 0.7
    else:
        return True, 1.0

def measure_pullback(candles: list[dict], direction: str, wave_lookback: int = 20) -> float:
    """
    After a mini-trend completes, measure how much price has pulled back.
    direction='SELL' → trend was up; pullback = how far price dropped from peak.
    direction='BUY'  → trend was down; pullback = how far price rose from trough.
    Returns ratio of pullback vs wave size (0.0 if insufficient data).
    """
    if len(candles) < wave_lookback + 3:
        return 0.0
    wave_candles = candles[-(wave_lookback + 3):-3]
    recent_candles = candles[-3:]
    if direction == "SELL":
        wave_high = max(c["high"] for c in wave_candles)
        wave_low  = min(c["low"]  for c in wave_candles)
        wave      = wave_high - wave_low
        if wave == 0: return 0.0
        recent_low  = min(c["low"] for c in recent_candles)
        pullback    = wave_high - recent_low
        return pullback / wave
    else:
        wave_high = max(c["high"] for c in wave_candles)
        wave_low  = min(c["low"]  for c in wave_candles)
        wave      = wave_high - wave_low
        if wave == 0: return 0.0
        recent_high = max(c["high"] for c in recent_candles)
        pullback    = recent_high - wave_low
        return pullback / wave

# ─── STRATEGY 1: STOP HUNTER ──────────────────────────────────────────────────
# REWRITE (strategy correction pass): the previous implementation treated any
# wick-sweep of a level followed by a lower-high/higher-low + BOS as the
# signal. That is NOT the intended Stop Hunter logic. The strategy is looking
# for a FALSE BREAKOUT followed by an immediate structural reversal:
#   1. A significant HTF zone (S/R / Order Block / Supply-Demand) is located.
#   2. Price reaches that zone.
#   3. On a lower timeframe, price BREAKS the level (a genuine close beyond
#      it, not just a wick). At this moment the breakout is neither valid nor
#      fake — it is simply a breakout.
#   4. Immediately after that breakout, the strategy waits for a CHOCH
#      (Change of Character): price must close back through the minor swing
#      point formed *during* the breakout leg, in the opposite direction.
#      That CHOCH is what confirms the breakout failed and price is
#      reversing — only then does the strategy enter.
#   5. If no CHOCH follows the breakout, no signal is generated.
# This logic is identical (mirrored) for BUY and SELL — see
# _detect_false_breakout_choch() below, which implements both directions
# through a single shared code path so they cannot drift out of sync.

def _find_structural_pivot_high(seq: list[dict]) -> Optional[float]:
    """
    Returns the price of the FIRST confirmed pivot high in `seq` — a candle
    whose high is strictly greater than the highs of the candle immediately
    before it AND the candle immediately after it, both also within `seq`.
    This is a genuine (if minimal, 1-candle-each-side) structural pivot
    confirmation, not a bare "highest wick in the window" reading — the
    pivot must be confirmed by price turning away from it on both sides.
    Returns None if no such confirmed pivot exists (e.g. `seq` has fewer
    than 3 candles, or the highs never actually turn down on both sides).
    """
    for p in range(1, len(seq) - 1):
        if seq[p]["high"] > seq[p - 1]["high"] and seq[p]["high"] > seq[p + 1]["high"]:
            return seq[p]["high"]
    return None


def _find_structural_pivot_low(seq: list[dict]) -> Optional[float]:
    """Mirror of _find_structural_pivot_high() for pivot lows (Higher Low)."""
    for p in range(1, len(seq) - 1):
        if seq[p]["low"] < seq[p - 1]["low"] and seq[p]["low"] < seq[p + 1]["low"]:
            return seq[p]["low"]
    return None


def _detect_false_breakout_choch(candles: list[dict], zone_level: float,
                                  breakout_direction: str,
                                  min_body_ratio: float = 0.6) -> Optional[dict]:
    """
    Scans `candles` for a false-breakout + CHOCH reversal around `zone_level`.

    breakout_direction="BUY"  → look for price breaking UP through the zone
                                 (e.g. resistance). If a CHOCH follows, the
                                 resulting trade direction is SELL.
    breakout_direction="SELL" → look for price breaking DOWN through the zone
                                 (e.g. support). If a CHOCH follows, the
                                 resulting trade direction is BUY.

    Returns {"direction", "entry", "stop_reference"} on a confirmed CHOCH,
    else None. Symmetric for both directions — do not special-case one side.
    """
    reversal_direction = "SELL" if breakout_direction == "BUY" else "BUY"

    # Step 3: find the most recent GENUINE breakout candle — a real close
    # beyond the zone, >=60% of its body outside it, colored in the breakout
    # direction. Searched most-recent-first within a 10-candle window so the
    # CHOCH check below reacts to the breakout that "just happened", not a
    # stale one.
    #
    # FIX (strategy audit, round 5): is_valid_zone_breakout() only checks a
    # candle's OWN close/body against zone_level — it has no notion of
    # "before vs after". That's correct for its other caller (Hammer Fib's
    # breakout confirmation) but wrong here: scanning most-recent-first with
    # only that check let this loop lock onto ANY later candle that simply
    # still closes beyond the zone with a strong body — e.g. a continuation
    # candle several bars into an already-running impulse — and treat it as
    # "the breakout". That is not a genuine crossing, it's just another
    # candle on the far side of the level, and anchoring the retracement /
    # swing / Lower-High-Higher-Low / CHOCH structure to it is anchoring to
    # the wrong reference point (violates "breakout must be a genuine
    # crossing, not simply a candle closing beyond a level").
    #
    # A genuine crossing candle is one where the PRECEDING candle was NOT
    # already closed beyond the zone in the same direction — i.e. this is
    # the candle where price actually transitioned from one side of the
    # zone to the other. Added as a local guard (kept local to Stop Hunter
    # only; is_valid_zone_breakout() itself is left untouched since Hammer
    # Fib also depends on its current, narrower semantics).
    def _is_genuine_crossing(idx: int) -> bool:
        if not is_valid_zone_breakout(candles[idx], zone_level, breakout_direction, min_body_ratio):
            return False
        if idx == 0:
            return True  # no prior candle in view — treat as the crossing
        prev_close = candles[idx - 1]["close"]
        if breakout_direction == "BUY":
            return prev_close <= zone_level  # price was on/below the zone just before
        else:
            return prev_close >= zone_level  # price was on/above the zone just before

    breakout_idx = None
    lookback_start = max(0, len(candles) - 12)
    for i in range(len(candles) - 3, lookback_start - 1, -1):
        if i < 0:
            break
        if _is_genuine_crossing(i):
            breakout_idx = i
            break
    if breakout_idx is None:
        return None

    # Step 4: everything after the breakout candle, up to (but excluding) the
    # current candle, defines the retracement leg. The current (last) candle
    # is the CHOCH confirmation candle.
    #
    # FIX (strategy audit, round 2): two remaining mismatches corrected here:
    #   (1) The Lower High / Higher Low confirmation candle must be a
    #       SEPARATE, already-closed candle that exists strictly BEFORE the
    #       CHOCH candle. The previous version fell back to using the CHOCH
    #       candle's own high/low as the Lower High/Higher Low when no
    #       distinct bounce candle existed yet — collapsing two supposedly
    #       sequential steps (structure confirmation, then break) into one
    #       candle. That fallback is removed entirely; if no separate
    #       confirmed pivot exists before the CHOCH candle, the setup is
    #       rejected outright.
    #   (2) The Lower High / Higher Low is no longer identified by comparing
    #       a bounce candle's wick against the breakout candle's wick (which
    #       is a magnitude comparison, not a structure check, and could also
    #       misfire if the impulse extended past the breakout candle before
    #       turning). It is now identified as a genuine structural pivot:
    #       a candle whose high/low is confirmed by price turning away from
    #       it on the candle immediately before AND immediately after it
    #       (see _find_structural_pivot_high/_low above) — a real market-
    #       structure turning point, not just a temporary wick or pullback.
    #
    #   (3) FIX (round 3): round 2 dropped the magnitude comparison entirely
    #       — any confirmed structural pivot was accepted as the Lower High/
    #       Higher Low, with nothing checked against it. That is also wrong:
    #       a "Lower High" is only meaningful relative to the high it is
    #       lower THAN. That reference must be the true extreme of the whole
    #       breakout impulse (the highest high / lowest low reached anywhere
    #       from the breakout candle through to the swing point where the
    #       retracement begins) — not just the single breakout candle, since
    #       the impulse commonly keeps extending for several candles past
    #       the breakout candle before it actually turns. The comparison is
    #       restored below (lower_high vs leg_extreme_high / higher_low vs
    #       leg_extreme_low), now anchored to that correct reference.
    after = candles[breakout_idx + 1:-1]
    choch_candle = candles[-1]
    if not after:
        return None  # no retracement candles yet — breakout just happened

    if breakout_direction == "BUY":
        # Breakout was upward. Retracement must first pull back down to a
        # minor swing low, then form a CONFIRMED structural pivot high (a
        # genuine Lower High, not just a wick) strictly before the CHOCH
        # candle, and only THEN may a close back below the swing low count
        # as CHOCH.
        # FIX (strategy audit, round 4): swing_low_idx must be the FIRST
        # valid retracement swing low after the breakout, not the global
        # minimum low across the whole post-breakout window. Using the
        # global min could skip past the first genuine retracement and
        # lock onto a later, deeper swing instead — which is not what the
        # required sequence (breakout -> first retracement -> first swing
        # low -> impulse extreme -> Lower High -> CHOCH) calls for.
        # The breakout candle is included as the left-side neighbor so a
        # pivot forming on the very first retracement candle can still be
        # detected.
        extended = [candles[breakout_idx]] + after
        swing_low_idx = None
        for p in range(1, len(extended) - 1):
            if extended[p]["low"] < extended[p - 1]["low"] and extended[p]["low"] < extended[p + 1]["low"]:
                swing_low_idx = p - 1  # convert back to an index into `after`
                break
        if swing_low_idx is None:
            return None  # no confirmed first retracement swing low yet
        swing_ref = after[swing_low_idx]["low"]

        # FIX (strategy audit, round 3): the Lower High must be compared
        # against the TRUE EXTREME of the whole breakout impulse — not just
        # the breakout candle alone. The impulse can keep printing new highs
        # for several candles after the breakout candle itself before the
        # retracement actually begins; that retracement begins where price
        # starts declining toward the swing low found above. So the impulse
        # leg runs from the breakout candle through to (and including) the
        # swing-low candle, and its true extreme is the highest high reached
        # anywhere in that span.
        absolute_swing_low_idx = breakout_idx + 1 + swing_low_idx
        leg_extreme_high = max(
            c["high"] for c in candles[breakout_idx: absolute_swing_low_idx + 1]
        )

        # Pivot search starts at the swing-low candle itself (it serves as
        # the left-side neighbor for the first candidate pivot) and runs
        # through the rest of the retracement leg — never into choch_candle.
        pivot_seq = after[swing_low_idx:]
        lower_high = _find_structural_pivot_high(pivot_seq)
        if lower_high is None:
            return None  # no confirmed Lower High pivot before the CHOCH candle
        if lower_high >= leg_extreme_high:
            return None  # not a real Lower High — must sit below the impulse's true high

        if choch_candle["close"] >= swing_ref:
            return None
        if not is_valid_zone_breakout(choch_candle, swing_ref, "SELL", min_body_ratio):
            return None
    else:
        # Breakout was downward. Mirror image: retracement must bounce to a
        # minor swing high, then form a CONFIRMED structural pivot low (a
        # genuine Higher Low) strictly before the CHOCH candle, before a
        # close back above the swing high counts as CHOCH.
        # FIX (strategy audit, round 4): mirror of the BUY-side fix above.
        # swing_high_idx must be the FIRST valid retracement swing high
        # after the breakout, not the global maximum high across the whole
        # post-breakout window, so the required sequence (breakout -> first
        # retracement -> first swing high -> impulse extreme -> Higher Low
        # -> CHOCH) is respected instead of possibly locking onto a later,
        # higher swing.
        extended = [candles[breakout_idx]] + after
        swing_high_idx = None
        for p in range(1, len(extended) - 1):
            if extended[p]["high"] > extended[p - 1]["high"] and extended[p]["high"] > extended[p + 1]["high"]:
                swing_high_idx = p - 1  # convert back to an index into `after`
                break
        if swing_high_idx is None:
            return None  # no confirmed first retracement swing high yet
        swing_ref = after[swing_high_idx]["high"]

        # Mirror of the BUY-side fix above: the Higher Low must be compared
        # against the true extreme (lowest low) of the whole breakout
        # impulse, from the breakout candle through the swing-high candle.
        absolute_swing_high_idx = breakout_idx + 1 + swing_high_idx
        leg_extreme_low = min(
            c["low"] for c in candles[breakout_idx: absolute_swing_high_idx + 1]
        )

        pivot_seq = after[swing_high_idx:]
        higher_low = _find_structural_pivot_low(pivot_seq)
        if higher_low is None:
            return None  # no confirmed Higher Low pivot before the CHOCH candle
        if higher_low <= leg_extreme_low:
            return None  # not a real Higher Low — must sit above the impulse's true low

        if choch_candle["close"] <= swing_ref:
            return None
        if not is_valid_zone_breakout(choch_candle, swing_ref, "BUY", min_body_ratio):
            return None

    # STOP LOSS reference (spec): "behind the wick of the impulse candle that
    # originally broke the HTF Support/Resistance zone" — i.e. the breakout
    # candle's own wick, NOT the extended range of the whole failed-breakout
    # leg (which the previous implementation used, placing stops far wider
    # than the spec intends).
    extreme = candles[breakout_idx]["high"] if breakout_direction == "BUY" \
        else candles[breakout_idx]["low"]

    return {
        "direction": reversal_direction,
        "entry": choch_candle["close"],
        "extreme": extreme,   # breakout candle's wick — SL reference
        # ── ADDED (chart visualization feature, purely additive) ──────────
        # Nothing above this line changed. These two fields just expose
        # coordinates already computed in this function so the chart
        # renderer can anchor its BOS/CHOCH markers without recalculating
        # anything itself.
        "breakout_idx": breakout_idx,
        "swing_ref": swing_ref,
    }


async def strategy_stop_hunter(
    session: aiohttp.ClientSession,
    symbol: str,
    state: Optional[dict] = None,
) -> Optional[dict]:
    """
    1. Find a significant S&R / Order Block / Supply-Demand zone on 1H and 4H.
    2. Wait for price to reach that zone.
    3. On 1m/3m/5m: wait for price to BREAK the zone (a neutral breakout,
       NOT a signal by itself — pure liquidity collection).
    4. After the breakout, price must retrace, form a Lower High (SELL) /
       Higher Low (BUY), and then break the resulting minor swing — that
       break itself IS the CHOCH and the only confirmation required (no
       extra BOS is needed after it). No retracement + Lower High/Higher
       Low + swing break → no signal.
    Mirrored for BUY (false breakdown through support) and SELL (false
    breakout through resistance).
    """
    # Get higher TF S&R
    c1h = await get_candles(session, symbol, "1h", 100)
    c4h = await get_candles(session, symbol, "4h", 100)
    if not c1h or not c4h: return None

    # FIX v3.2: use scored S&R so sr_score actually reaches calc_score
    scored_1h = find_sr_levels_scored(c1h)
    scored_4h = find_sr_levels_scored(c4h)
    # Item 5 (multi-timeframe confluence, additive score only — never a hard
    # gate): a level independently found on BOTH 1h and 4h is structurally
    # stronger than one found on either alone. Uses candle data already
    # fetched above — zero extra API calls.
    scored_1h = apply_zone_confluence(scored_1h, scored_4h)
    scored_4h = apply_zone_confluence(scored_4h, scored_1h)
    levels_1h = [sl["price"] for sl in scored_1h]
    levels_4h = [sl["price"] for sl in scored_4h]
    all_scored = scored_1h + scored_4h
    all_levels = sorted(set(levels_1h + levels_4h))

    price = c1h[-1]["close"]

    # BUG FIX (strategy audit): tolerance was 0.015 (1.5%), far looser than
    # every other strategy's S&R tolerance (0.008-0.012). On symbols with a
    # handful of levels this made "near a level" almost always true, so the
    # false-breakout thesis below was frequently anchored on a level that
    # wasn't actually meaningfully close to price. Tightened to 0.008 to
    # match Stop Hunter's sibling strategies.
    near = is_near_level(price, all_levels, tol=0.008)
    if near is None:
        # Check if price just broke a level (within last 3 candles on 1h).
        # BUG FIX: previously took the FIRST level in `all_levels` (sorted by
        # price, not by relevance) that was touched at all in the last 3
        # candles, regardless of how far away that level was. That let the
        # strategy anchor on an arbitrary, possibly irrelevant level. Now we
        # scan every touched level and keep the one closest to current price.
        recent_highs = [c["high"] for c in c1h[-3:]]
        recent_lows  = [c["low"]  for c in c1h[-3:]]
        candidates = [
            lv for lv in all_levels
            if any(h > lv * 1.001 for h in recent_highs) or any(l < lv * 0.999 for l in recent_lows)
        ]
        if candidates:
            near = min(candidates, key=lambda lv: abs(lv - price))

    if near is None:
        diag_count("Stop Hunter", "rejected_no_zone")
        return None
    diag_count("Stop Hunter", "passed_zone")

    nearest_sr = get_nearest_sr_scored(near, all_scored) if near else None
    sr_sc = nearest_sr["sr_score"] if nearest_sr else None

    # Check each lower TF for the false-breakout + CHOCH pattern
    for tf in ["1m", "3m", "5m"]:
        c = await get_candles(session, symbol, tf, 60)
        if len(c) < 20: continue

        # Two symmetric scenarios per timeframe:
        #   breakout_direction="BUY"  → false breakout THROUGH RESISTANCE → SELL trade
        #   breakout_direction="SELL" → false breakout THROUGH SUPPORT    → BUY trade
        for breakout_direction in ("BUY", "SELL"):
            diag_count("Stop Hunter", "raw_breakout_candidate")
            _mbr = get_strategy_settings(state, "Stop Hunter").get("breakout_min_body_ratio", 0.6) if state else 0.6
            res = _detect_false_breakout_choch(c, near, breakout_direction, _mbr)
            if res is None:
                diag_count("Stop Hunter", "rejected_choch_pattern")
                continue
            diag_count("Stop Hunter", "passed_choch_pattern")

            direction = res["direction"]
            entry     = res["entry"]
            extreme   = res["extreme"]
            _atr_on, _atr_mult = _atr_buffer_settings(state, "Stop Hunter")
            stop      = calc_stop_from_reference(c, direction, extreme,
                                                  atr_buffer=_atr_mult, atr_enabled=_atr_on)
            tp1, tp2  = calc_targets(entry, stop, direction)

            # How far beyond the zone the failed breakout actually traveled
            # (a deeper failed breakout is a stronger stop-hunt signature).
            if direction == "SELL":
                sweep_depth_deep = extreme > near * 1.004
            else:
                sweep_depth_deep = extreme < near * 0.996
            volume_confirm = entry_volume_ratio(c) >= 1.3
            score = calc_score([sweep_depth_deep, near in levels_4h, volume_confirm], sr_score=sr_sc)
            diag_count("Stop Hunter", "signal_emitted")
            return {
                "symbol": symbol, "direction": direction,
                "strategy": "Stop Hunter", "price": price,
                "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                "timeframe": f"S/R on 1H/4H | Entry {tf}", "score": score,
                # ── ADDED (chart visualization feature, purely additive) ──
                # Reuses values already computed above (near, res, entry-tf
                # candle list `c`) so the chart renderer can draw the exact
                # liquidity zone / BOS / CHOCH it used, with zero new
                # analysis and no effect on the signal itself.
                "visual": {
                    "type": "zone_choch",
                    "entry_tf": tf,
                    "candles": c,
                    "trigger_idx": len(c) - 1,
                    "zone_level": near,
                    "breakout_idx": res.get("breakout_idx"),
                    "swing_ref": res.get("swing_ref"),
                },
            }
    return None

# ─── STRATEGY 2: HAMMER FIBONACCI (1H + 4H) ───────────────────────────────────
async def _hammer_fib_on_tf(
    session: aiohttp.ClientSession,
    symbol: str,
    pattern_tf: str,          # "4h" or "1h"
    state: Optional[dict] = None,
) -> Optional[dict]:
    """
    وقتی کندل pattern_tf بسته شد چک میکنه آیا چکش یا شوتینگ‌استار هست.
    اگر بود، در تایم‌فریم‌های پایین‌تر فیبو 0.618 رو پیدا میکنه.
    """
    c = await get_candles(session, symbol, pattern_tf, 30)
    if len(c) < 5: return None

    _strat_name_hint = "Hammer Fib (4H)" if pattern_tf == "4h" else "Hammer Fib (1H)"

    last  = c[-1]
    price = last["close"]
    hammer = is_hammer(last)
    star   = is_shooting_star(last)
    if not hammer and not star: return None

    direction   = "BUY" if hammer else "SELL"
    pattern_name = "Hammer" if hammer else "Shooting Star"
    diag_count(_strat_name_hint, "raw_pattern_candidate")

    # تایم‌فریم‌های ورود بر اساس pattern_tf
    entry_tfs = ["1m", "3m", "5m"] if pattern_tf == "4h" else ["1m", "3m", "5m"]

    for tf in entry_tfs:
        lc = await get_candles(session, symbol, tf, 80)
        if len(lc) < 20: continue

        highs  = [x["high"]  for x in lc]
        lows   = [x["low"]   for x in lc]

        if direction == "SELL":
            # FIX (strategy audit): spec requires a bearish impulse that
            # BREAKS THE PREVIOUS LOW after the Shooting Star, not a break
            # above a previous high. This was checking the wrong direction
            # entirely (an upside break), which is backwards for a bearish
            # continuation setup.
            prev_low = min(lows[-40:-20])
            broke = any(l < prev_low for l in lows[-20:])
            if not broke:
                diag_count(_strat_name_hint, "rejected_no_breakout")
                continue
            # FIX (strategy audit): apply the spec's global Breakout
            # Validation rule (close outside the zone AND >=60% of body
            # outside it), not just a proximity-tolerant close check.
            _mbr = get_strategy_settings(state, _strat_name_hint).get("breakout_min_body_ratio", 0.6) if state else 0.6
            if not is_valid_zone_breakout(lc[-1], prev_low, "SELL", _mbr):
                diag_count(_strat_name_hint, "rejected_breakout_validation")
                continue
            diag_count(_strat_name_hint, "passed_breakout")
            # FIX (strategy audit): Fibonacci High -> Low must run from the
            # pre-impulse swing high (older window, near the Shooting Star)
            # down to the new low made by the bearish impulse (recent
            # window) — these two were swapped with the BUY branch's logic.
            wave_high = max(highs[-30:-20])
            wave_low  = min(lows[-20:])
            # FIX (Fibonacci orientation bug): anchors (wave_high/wave_low)
            # are unchanged — only fibonacci_level()'s internal formula was
            # corrected (see its definition) so 0.618 now sits close to
            # wave_high (matching TradingView), not close to wave_low.
            fib_618   = fibonacci_level(wave_high, wave_low, 0.618)
            # FIX (strategy audit): SL must be "behind the high" (the fib
            # swing high), not the generic 10-candle swing stop.
            _atr_on, _atr_mult = _atr_buffer_settings(state, _strat_name_hint)
            stop      = calc_stop_from_reference(lc, "SELL", wave_high,
                                                  atr_buffer=_atr_mult, atr_enabled=_atr_on)
            entry     = fib_618
            tp1, tp2  = calc_targets(entry, stop, "SELL")
            # BUG FIX (strategy audit): `star`, `broke`, and the literal `True`
            # were ALL already guaranteed true to reach this line (gated by
            # `if not hammer and not star: return None` and `if not broke:
            # continue` above), so the score never actually varied — it was a
            # constant. Replaced with real, independently-varying signals:
            # how much stronger than the minimum pattern threshold the
            # shooting star's wick is, how decisively price broke the prior
            # high (vs. a marginal tick-over), and whether entry volume
            # confirms the move.
            pat_body = abs(last["close"] - last["open"])
            upper_shadow = last["high"] - max(last["open"], last["close"])
            pattern_quality = pat_body > 0 and upper_shadow >= 3 * pat_body  # min required by is_shooting_star is 2x
            break_strength  = any(l < prev_low * 0.998 for l in lows[-20:])  # decisive break, not a marginal tick-under
            volume_confirm  = entry_volume_ratio(lc) >= 1.3
            score = calc_score([pattern_quality, break_strength, volume_confirm])
            diag_count(_strat_name_hint, "signal_emitted")
            return {
                "symbol": symbol, "direction": "SELL",
                "strategy": f"Hammer Fib ({pattern_tf.upper()})",
                "price": price, "entry": entry, "stop": stop,
                "tp1": tp1, "tp2": tp2,
                "timeframe": f"{pattern_tf.upper()} {pattern_name} | Fib {tf}",
                "score": score,
                # ── ADDED (chart visualization feature, purely additive) ──
                # wave_high/wave_low are the exact fib anchors already
                # computed above; indices just locate them on `lc` (the
                # entry-tf candle list) for the renderer.
                "visual": {
                    "type": "fib",
                    "entry_tf": tf,
                    "candles": lc,
                    "trigger_idx": len(lc) - 1,
                    "fib_high": wave_high,
                    "fib_low": wave_low,
                    "fib_high_idx": max(0, len(lc) - 30) + highs[max(0, len(lc) - 30):max(0, len(lc) - 20)].index(wave_high),
                    "fib_low_idx": max(0, len(lc) - 20) + lows[max(0, len(lc) - 20):].index(wave_low),
                },
            }
        else:
            # FIX (strategy audit): spec requires a bullish impulse that
            # BREAKS THE PREVIOUS HIGH after the Hammer, not a break below a
            # previous low. This was checking the wrong direction entirely
            # (a downside break), which is backwards for a bullish
            # continuation setup.
            prev_high = max(highs[-40:-20])
            broke = any(h > prev_high for h in highs[-20:])
            if not broke:
                diag_count(_strat_name_hint, "rejected_no_breakout")
                continue
            # FIX (strategy audit): apply the spec's global Breakout
            # Validation rule (close outside the zone AND >=60% of body
            # outside it), not just a proximity-tolerant close check.
            _mbr = get_strategy_settings(state, _strat_name_hint).get("breakout_min_body_ratio", 0.6) if state else 0.6
            if not is_valid_zone_breakout(lc[-1], prev_high, "BUY", _mbr):
                diag_count(_strat_name_hint, "rejected_breakout_validation")
                continue
            diag_count(_strat_name_hint, "passed_breakout")
            # FIX (strategy audit): Fibonacci Low -> High must run from the
            # pre-impulse swing low (older window, near the Hammer) up to
            # the new high made by the bullish impulse (recent window) —
            # these two were swapped with the SELL branch's logic.
            wave_low  = min(lows[-30:-20])
            wave_high = max(highs[-20:])
            # FIX (BUY Fibonacci direction, final v2): resolved with the
            # user. For a BUY setup, the Fibonacci is drawn REVERSED from
            # SELL — swing LOW = 1.0, swing HIGH = 0.0 (matching manual
            # TradingView usage for a bullish retracement) — not the same
            # fixed low=0/high=100 ladder SELL uses. Under that reversed
            # ladder, the price sitting on the "0.618" line is:
            #   price(ratio=0.618) = wave_high - 0.618*(wave_high-wave_low)
            #                       = wave_low + 0.382*(wave_high-wave_low)
            # i.e. close to wave_low — this is BUY-specific arithmetic
            # again (not the shared fibonacci_level() helper, which stays
            # SELL's fixed-ladder convention only). This is the SAME price
            # this strategy used before the brief "shared helper" attempt —
            # only the chart's ladder rendering changes now (see
            # chart_renderer.py/drawing_tools.py), not this price.
            fib_618   = wave_high - (wave_high - wave_low) * 0.618
            # FIX (strategy audit): SL must be "behind the low" (the fib
            # swing low), not the generic 10-candle swing stop.
            _atr_on, _atr_mult = _atr_buffer_settings(state, _strat_name_hint)
            stop      = calc_stop_from_reference(lc, "BUY", wave_low,
                                                  atr_buffer=_atr_mult, atr_enabled=_atr_on)
            entry     = fib_618
            tp1, tp2  = calc_targets(entry, stop, "BUY")
            # BUG FIX: see SELL branch above for rationale.
            pat_body = abs(last["close"] - last["open"])
            lower_shadow = min(last["open"], last["close"]) - last["low"]
            pattern_quality = pat_body > 0 and lower_shadow >= 3 * pat_body  # min required by is_hammer is 2x
            break_strength  = any(h > prev_high * 1.002 for h in highs[-20:])  # decisive break, not a marginal tick-over
            volume_confirm  = entry_volume_ratio(lc) >= 1.3
            score = calc_score([pattern_quality, break_strength, volume_confirm])
            diag_count(_strat_name_hint, "signal_emitted")
            return {
                "symbol": symbol, "direction": "BUY",
                "strategy": f"Hammer Fib ({pattern_tf.upper()})",
                "price": price, "entry": entry, "stop": stop,
                "tp1": tp1, "tp2": tp2,
                "timeframe": f"{pattern_tf.upper()} {pattern_name} | Fib {tf}",
                "score": score,
                # ── ADDED (chart visualization feature, purely additive) ──
                "visual": {
                    "type": "fib",
                    "entry_tf": tf,
                    "candles": lc,
                    "trigger_idx": len(lc) - 1,
                    "fib_high": wave_high,
                    "fib_low": wave_low,
                    "fib_low_idx": max(0, len(lc) - 30) + lows[max(0, len(lc) - 30):max(0, len(lc) - 20)].index(wave_low),
                    "fib_high_idx": max(0, len(lc) - 20) + highs[max(0, len(lc) - 20):].index(wave_high),
                },
            }
    return None

async def strategy_hammer_fib(session, symbol, state=None):
    return await _hammer_fib_on_tf(session, symbol, "4h", state)

async def strategy_hammer_fib_1h(session, symbol, state=None):
    return await _hammer_fib_on_tf(session, symbol, "1h", state)

# ─── STRATEGY 3: HB ───────────────────────────────────────────────────────────
async def strategy_hb(
    session: aiohttp.ClientSession,
    symbol: str,
    state: Optional[dict] = None,
) -> Optional[dict]:
    """
    On important S&R level: strong spike candle opposite to trend →
    place order at 0.5 fib of that spike candle. Stop behind candle. TP 3R.
    """
    c1h = await get_candles(session, symbol, "1h", 100)
    c4h = await get_candles(session, symbol, "4h", 60)
    if not c1h or not c4h: return None

    # AUDIT FIX: HB's zone-tolerance / spike-dominance / volume-confirm /
    # tight-touch thresholds used to be hardcoded literals below, which meant
    # the Admin Panel's per-strategy Strategy Settings had zero effect on
    # HB's actual signal detection. They now come from get_strategy_settings()
    # like every other strategy's tunables (both live scan and backtest
    # replay call this strategy function with the same `state`, so both paths
    # pick up whatever is configured). Defaults are unchanged from before.
    cfg = get_strategy_settings(state, "HB") if state is not None else STRATEGY_FILTER_DEFAULTS
    hb_zone_tol = cfg.get("hb_zone_tolerance", 0.008)
    hb_spike_mult = cfg.get("hb_spike_dominance_multiplier", 3.0)
    hb_tight_ratio = cfg.get("hb_tight_level_touch_ratio", 0.5)
    hb_vol_mult = cfg.get("entry_volume_multiplier", 1.5)

    # FIX v3.2: use scored S&R so sr_score propagates to calc_score
    scored_1h = find_sr_levels_scored(c1h)
    scored_4h = find_sr_levels_scored(c4h)
    # Item 5 (multi-timeframe confluence, additive only) — same as Stop
    # Hunter, reuses candle data already fetched above.
    scored_1h = apply_zone_confluence(scored_1h, scored_4h)
    scored_4h = apply_zone_confluence(scored_4h, scored_1h)
    all_scored = scored_1h + scored_4h
    levels = [sl["price"] for sl in all_scored]
    price  = c1h[-1]["close"]

    # CLARIFIED SPEC (HB update): 15m only (5m removed), and the trigger is
    # specifically a body-only Engulfing candle (Bullish/Bearish Engulfing),
    # not the generic strong-candle composite — see is_body_engulfing().
    for tf in ["15m"]:
        c = await get_candles(session, symbol, tf, 80)
        if len(c) < 25: continue

        last = c[-1]
        prev = c[:-1]
        diag_count("HB", "raw_candidate")

        # Check near S&R
        near = is_near_level(price, levels, tol=hb_zone_tol)
        if near is None:
            diag_count("HB", "rejected_zone")
            continue
        diag_count("HB", "passed_zone")

        strong = is_body_engulfing(last, prev[-1])
        if not strong:
            diag_count("HB", "rejected_engulfing")
            continue
        diag_count("HB", "passed_engulfing")

        # Determine trend direction from last 20 candles
        # BUG FIX (strategy audit): was comparing against c[-2], one candle
        # older than what every other strategy uses (c[-1], now guaranteed
        # closed by the get_candles fix). This put HB's trend read a full
        # candle behind Stop Hunter/Trigger Fibonacci/Exhaustion, so HB could
        # act on a trend read that had already flipped elsewhere.
        trend_up = c[-20]["close"] < c[-1]["close"]

        # FIX v3.2: get sr_score for the nearest level
        nearest_sr = get_nearest_sr_scored(near, all_scored) if near else None
        sr_sc = nearest_sr["sr_score"] if nearest_sr else None

        if trend_up:
            # At resistance, strong bearish spike → SELL at 0.5 of candle
            if last["close"] < last["open"]:  # bearish candle
                diag_count("HB", "passed_trend_color")
                # STRATEGY CORRECTION: HB does NOT require a CHOCH / market
                # structure break before entry. The intended sequence is:
                # Trend -> price reaches major S/R -> strong opposite candle
                # appears -> Fibonacci is drawn ONLY on that candle -> enter
                # at 0.5 retracement. A prior revision had added a mandatory
                # CHOCH (break of the recent local swing low) plus a global
                # Breakout Validation check against that swing — that is not
                # part of this strategy's spec and has been removed. The
                # only requirements are: near a level, a strong candle
                # (checked above), and that candle being opposite the trend
                # (checked here via its color).
                mid   = (last["high"] + last["low"]) / 2
                # FIX (strategy audit): SL must be "behind the candle high"
                # per spec — the spike candle's own high. calc_safe_stop()
                # was anchoring to the swing high of the *preceding* 10
                # candles (candles[-11:-1]), which excludes the spike candle
                # itself, so the stop could sit below the very candle it was
                # supposed to protect against.
                _atr_on, _atr_mult = _atr_buffer_settings(state, "HB")
                stop  = calc_stop_from_reference(c, "SELL", last["high"],
                                                  atr_buffer=_atr_mult, atr_enabled=_atr_on)
                entry = mid
                tp1, tp2 = calc_targets(entry, stop, "SELL")
                # BUG FIX (strategy audit): `strong` and `near is not None`
                # were already guaranteed true by the gates above (`if not
                # strong: continue`, `if near is None: continue`), and `True`
                # was a hardcoded literal — none of these three ever varied,
                # so the score's non-S&R component was a constant. Replaced
                # with real, independently-varying signals: whether the spike
                # exceeds the *minimum* strong-candle bar by a wide margin,
                # whether entry volume confirms it, and how tight the level
                # touch actually is (vs. just inside the 0.8% tolerance).
                spike_body   = abs(last["close"] - last["open"])
                spike_dominance = spike_body >= hb_spike_mult * avg_body(prev[-20:])
                volume_confirm  = entry_volume_ratio(c) >= hb_vol_mult
                tight_level_touch = abs(price - near) / near < (hb_zone_tol * hb_tight_ratio)
                score = calc_score([spike_dominance, volume_confirm, tight_level_touch], sr_score=sr_sc)
                diag_count("HB", "signal_emitted")
                return {
                    "symbol": symbol, "direction": "SELL",
                    "strategy": "HB", "price": price,
                    "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                    "timeframe": tf, "score": score,
                    # ── ADDED (chart visualization feature, purely additive) ──
                    # HB's fib is drawn only on the spike candle itself, per
                    # its own spec — that candle is `last` (= c[-1]).
                    "visual": {
                        "type": "fib_candle",
                        "entry_tf": tf,
                        "candles": c,
                        "trigger_idx": len(c) - 1,
                        "fib_high": last["high"],
                        "fib_low": last["low"],
                        "fib_high_idx": len(c) - 1,
                        "fib_low_idx": len(c) - 1,
                        "zone_level": near,
                    },
                }
        else:
            # At support, strong bullish spike → BUY at 0.5 of candle
            if last["close"] > last["open"]:  # bullish candle
                diag_count("HB", "passed_trend_color")
                # STRATEGY CORRECTION: see SELL branch above — no CHOCH /
                # market structure break is required for HB. The strong
                # opposite (bullish) candle at support is itself sufficient;
                # Fibonacci is drawn only on that candle and entry is taken
                # at its 0.5 retracement.
                mid   = (last["high"] + last["low"]) / 2
                # FIX (strategy audit): SL must be "behind the candle low"
                # per spec — the spike candle's own low. See SELL branch
                # above for why calc_safe_stop() was the wrong anchor here.
                _atr_on, _atr_mult = _atr_buffer_settings(state, "HB")
                stop  = calc_stop_from_reference(c, "BUY", last["low"],
                                                  atr_buffer=_atr_mult, atr_enabled=_atr_on)
                entry = mid
                tp1, tp2 = calc_targets(entry, stop, "BUY")
                # BUG FIX: see SELL branch above for rationale.
                spike_body   = abs(last["close"] - last["open"])
                spike_dominance = spike_body >= hb_spike_mult * avg_body(prev[-20:])
                volume_confirm  = entry_volume_ratio(c) >= hb_vol_mult
                tight_level_touch = abs(price - near) / near < (hb_zone_tol * hb_tight_ratio)
                score = calc_score([spike_dominance, volume_confirm, tight_level_touch], sr_score=sr_sc)
                diag_count("HB", "signal_emitted")
                return {
                    "symbol": symbol, "direction": "BUY",
                    "strategy": "HB", "price": price,
                    "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                    "timeframe": tf, "score": score,
                    # ── ADDED (chart visualization feature, purely additive) ──
                    "visual": {
                        "type": "fib_candle",
                        "entry_tf": tf,
                        "candles": c,
                        "trigger_idx": len(c) - 1,
                        "fib_high": last["high"],
                        "fib_low": last["low"],
                        "fib_high_idx": len(c) - 1,
                        "fib_low_idx": len(c) - 1,
                        "zone_level": near,
                    },
                }
    return None

def _find_trigger_fib_setup(c: list[dict], trend_up: bool, zone_level: float,
                             zone_tol: float = 0.008, min_body_ratio: float = 0.6) -> Optional[dict]:
    """
    Locates the Trigger Fibonacci pattern (spec Steps 3-7) on candle list `c`.

    Step 3: first trigger candle (t1) — strong, opposite to the HTF trend.
    Step 4: second candle (t2) — same color as t1, closes beyond t1's wick.
    Step 5: t2 also closes beyond the last strong swing wick of the
            ORIGINAL trend (the structural break).
    Step 6: do NOT enter immediately — the counter-trend impulse that starts
            at t2 must run its course, and only once its FIRST retracement
            candle (opposite color) appears is the impulse "complete".
    Step 7: the Fibonacci leg spans the ENTIRE impulse — from t1 to the
            extreme reached by the last impulse candle (not just t2 or t3).

    FIX (strategy audit): the previous implementation only ever looked at a
    fixed 3-candle window (t1=c[-3], t2=c[-2], "t3"=c[-1]) and treated t3 as
    if it were automatically the end of the impulse and the fib leg's other
    end — regardless of whether a retracement had actually begun. That let
    the strategy enter one candle after the trigger sequence with no
    impulse-completion or pullback-start check at all, directly violating
    the spec's explicit "Do NOT enter immediately after the trigger candles"
    / "the pullback after the impulse is mandatory" requirements. This
    helper instead scans for a valid t1/t2 pair, lets the impulse extend for
    as many candles as it actually runs, and only returns a setup once a
    genuine first retracement candle has appeared after it.

    FIX (strategy audit, round 2 — zone/trigger timing): a qualifying t1/t2
    pair used to be accepted no matter where in the lookback window it
    occurred, so a trigger sequence from an old, unrelated swing (unconnected
    to the zone the strategy just detected) could pass. `zone_level` is now
    required, and a candidate is only accepted if price actually traded
    through the zone (within `zone_tol`) in the few candles leading into t1 —
    i.e. the trigger sequence must be part of the same market move that
    interacted with the zone, not a stale pattern from further back.

    FIX (strategy audit, round 2 — Condition 3 candle quality): the
    "original trend" reference candles used to be selected by color alone
    (any candle closing in the trend direction). They must actually be
    strong trend candles, so the same `is_strong_candle()` check used for t1
    is now applied to each candidate trend candle as well.

    Returns {"t1", "t2", "impulse_extreme", "trend_wick", "correction_candles"}
    or None. Symmetric for both directions — do not special-case one side.
    """
    n = len(c)
    impulse_is_bearish = trend_up  # trend UP → counter-trend impulse is bearish

    # Search recent history for the most recent qualifying t1/t2 pair that
    # also has a completed impulse + retracement start after it.
    earliest_t1 = max(20, n - 30)
    for i in range(n - 3, earliest_t1 - 1, -1):
        t1 = c[i]
        t2 = c[i + 1]
        diag_count("Trigger Fibonacci", "raw_t1_candidate")

        if impulse_is_bearish:
            cond1 = t1["close"] < t1["open"] and is_strong_candle(t1, c[i - 20:i])
            cond2 = t2["close"] < t2["open"] and t2["close"] < t1["low"]
        else:
            cond1 = t1["close"] > t1["open"] and is_strong_candle(t1, c[i - 20:i])
            cond2 = t2["close"] > t2["open"] and t2["close"] > t1["high"]
        if not (cond1 and cond2):
            diag_count("Trigger Fibonacci", "rejected_t1_t2")
            continue
        diag_count("Trigger Fibonacci", "passed_t1_t2")

        # FIX (zone/trigger timing): the trigger sequence must be part of the
        # same market move that reached the zone — require price to have
        # actually traded through the zone (within tolerance) in the candles
        # immediately leading into t1, not just at the current 1H close.
        zone_window = c[max(0, i - 3):i + 1]
        zone_touched = any(
            x["low"] <= zone_level * (1 + zone_tol) and x["high"] >= zone_level * (1 - zone_tol)
            for x in zone_window
        )
        if not zone_touched:
            diag_count("Trigger Fibonacci", "rejected_zone_touch")
            continue
        diag_count("Trigger Fibonacci", "passed_zone_touch")

        # Step 5: t2 must also break the last strong swing wick of the
        # ORIGINAL (pre-trigger) trend. FIX (Condition 3): reference candles
        # must actually be strong trend candles (existing is_strong_candle
        # logic), not merely candles that closed in the trend's color.
        trend_start = max(0, i - 17)
        trend_window = c[trend_start:i]
        if trend_up:
            trend_candles = [
                x for k, x in enumerate(trend_window)
                if x["close"] > x["open"]
                and is_strong_candle(x, c[max(0, trend_start + k - 20):trend_start + k])
            ]
            if not trend_candles:
                continue
            trend_wick = min(x["low"] for x in trend_candles[-3:])
            cond3 = t2["close"] < trend_wick and is_valid_zone_breakout(t2, trend_wick, "SELL", min_body_ratio)
        else:
            trend_candles = [
                x for k, x in enumerate(trend_window)
                if x["close"] < x["open"]
                and is_strong_candle(x, c[max(0, trend_start + k - 20):trend_start + k])
            ]
            if not trend_candles:
                continue
            trend_wick = max(x["high"] for x in trend_candles[-3:])
            cond3 = t2["close"] > trend_wick and is_valid_zone_breakout(t2, trend_wick, "BUY", min_body_ratio)
        if not cond3:
            diag_count("Trigger Fibonacci", "rejected_trend_break")
            continue
        diag_count("Trigger Fibonacci", "passed_trend_break")

        # Step 6: extend the impulse candle-by-candle for as long as it keeps
        # printing the counter-trend color, then require the very next
        # candle to be the FIRST retracement candle (opposite color) — that
        # is what marks "the impulse finishes and starts retracing".
        j = i + 1
        while j + 1 < n:
            nxt = c[j + 1]
            same_color = (nxt["close"] < nxt["open"]) if impulse_is_bearish else (nxt["close"] > nxt["open"])
            if not same_color:
                break
            j += 1
        if j + 1 >= n:
            diag_count("Trigger Fibonacci", "rejected_impulse_not_finished")
            continue  # impulse still running / no retracement candle yet
        retrace_candle = c[j + 1]
        retrace_is_opposite = (retrace_candle["close"] > retrace_candle["open"]) if impulse_is_bearish \
            else (retrace_candle["close"] < retrace_candle["open"])
        if not retrace_is_opposite:
            diag_count("Trigger Fibonacci", "rejected_retracement")
            continue
        diag_count("Trigger Fibonacci", "passed_impulse_retracement")

        # Step 7: fib leg spans t1 through the extreme of the WHOLE impulse
        # (t2 .. last impulse candle j), not just t2/t3.
        impulse_candles = c[i + 1:j + 1]
        impulse_extreme = min(x["low"] for x in impulse_candles) if impulse_is_bearish \
            else max(x["high"] for x in impulse_candles)

        # FIX (pullback tracking): hand back the exact correction candles
        # (the retracement wave that starts right after the impulse ends,
        # through the present) so the caller can measure the pullback
        # against THIS wave instead of an unrelated fixed window.
        correction_candles = c[j + 1:]

        diag_count("Trigger Fibonacci", "setup_found")
        return {
            "t1": t1, "t2": t2, "impulse_extreme": impulse_extreme,
            "trend_wick": trend_wick, "correction_candles": correction_candles,
        }
    return None


def _structure_trend_up(candles: list[dict], swing_window: int = 5) -> Optional[bool]:
    """
    FIX (strategy audit — market structure): Trigger Fibonacci used to decide
    trend direction with a single close-vs-close-10-candles-ago comparison,
    which is not market structure at all. This compares the two most recent
    swing highs/lows (via the existing swing_high()/swing_low() helpers)
    instead: an uptrend requires a higher high AND a higher low; a downtrend
    requires a lower high AND a lower low. Kept intentionally lightweight
    (no full redesign) — mixed/inconclusive structure returns None so the
    caller can simply skip the symbol, same as before.
    """
    if len(candles) < swing_window * 4:
        return None
    recent = candles[-swing_window * 2:]
    prior  = candles[-swing_window * 4:-swing_window * 2]
    recent_high, recent_low = swing_high(recent, len(recent)), swing_low(recent, len(recent))
    prior_high, prior_low   = swing_high(prior, len(prior)),   swing_low(prior, len(prior))
    if recent_high > prior_high and recent_low > prior_low:
        return True
    if recent_high < prior_high and recent_low < prior_low:
        return False
    return None


# ─── STRATEGY 4: TRIGGER FIBONACCI ────────────────────────────────────────────
async def strategy_trigger_fib(
    session: aiohttp.ClientSession,
    symbol: str,
    state: Optional[dict] = None,
) -> Optional[dict]:
    """
    1H/15m structure → find important zone → wait for price to touch
    3m/1m: 3-condition trigger candle sequence → fib 0.618 entry after correction
    Condition 1: strong opposite candle
    Condition 2: next candle same color, close beyond shadow of prev
    Condition 3: close beyond shadow of last strong trend candle
    """
    c1h  = await get_candles(session, symbol, "1h",  60)
    c15m = await get_candles(session, symbol, "15m", 80)
    # FIX (strategy audit): spec's "Zone Detection" stage is 15m/5m, not
    # 1H/15m — 5m candles were never fetched, so the zone was being built
    # one timeframe pair too high.
    c5m  = await get_candles(session, symbol, "5m",  80)
    if not c1h or not c15m or not c5m: return None

    # FIX (strategy audit): Zone Detection uses 15m + 5m per spec (was
    # 1H + 15m).
    scored_15m_tf = find_sr_levels_scored(c15m)
    scored_5m_tf  = find_sr_levels_scored(c5m)
    all_scored_tf = scored_15m_tf + scored_5m_tf
    levels = [sl["price"] for sl in all_scored_tf]
    price  = c1h[-1]["close"]
    near   = is_near_level(price, levels, tol=0.008)
    if near is None: return None
    nearest_sr_tf = get_nearest_sr_scored(near, all_scored_tf) if near else None
    sr_sc_tf = nearest_sr_tf["sr_score"] if nearest_sr_tf else None

    # FIX (strategy audit): spec's "Market Structure Analysis" stage is
    # 1H + 15m, not 1H alone — require the two timeframes to agree on trend
    # direction before treating structure as established; otherwise there is
    # no clean Market Structure Shift to trade against.
    #
    # FIX (strategy audit, round 2 — market structure): trend direction used
    # to come from a single close-vs-close-10-candles-ago comparison, which
    # is not structure. Replaced with _structure_trend_up() — a lightweight
    # swing high/low comparison (existing swing_high()/swing_low() helpers).
    # Inconclusive/mixed structure on either timeframe now blocks the signal
    # instead of falling back to the old close-comparison.
    trend_up_1h  = _structure_trend_up(c1h)
    trend_up_15m = _structure_trend_up(c15m)
    if trend_up_1h is None or trend_up_15m is None or trend_up_1h != trend_up_15m:
        return None
    trend_up = trend_up_1h

    for tf in ["3m", "1m"]:
        c = await get_candles(session, symbol, tf, 50)
        if len(c) < 30: continue

        # FIX (strategy audit): replaced the old fixed t1=c[-3]/t2=c[-2]/
        # "t3"=c[-1] window with a proper scan that lets the counter-trend
        # impulse run for as many candles as it needs, and only proceeds
        # once its first retracement candle has actually appeared (spec
        # Step 6). See _find_trigger_fib_setup() docstring for details.
        #
        # FIX (strategy audit, round 2 — zone/trigger timing): pass the
        # detected zone level (`near`) so the setup finder can confirm the
        # trigger sequence actually happened as part of the same move that
        # touched the zone, instead of accepting an unrelated older pattern.
        setup = _find_trigger_fib_setup(
            c, trend_up, near,
            min_body_ratio=(get_strategy_settings(state, "Trigger Fibonacci").get("breakout_min_body_ratio", 0.6) if state else 0.6),
        )
        if setup is None:
            continue
        t1, t2 = setup["t1"], setup["t2"]
        impulse_extreme = setup["impulse_extreme"]
        trend_wick = setup["trend_wick"]
        correction_candles = setup["correction_candles"]

        direction = "SELL" if trend_up else "BUY"

        # FIX (strategy audit, round 2 — pullback tracking): the pullback
        # used to be measured with measure_pullback()'s own fixed 20-candle
        # lookback window, unrelated to the actual trigger impulse that was
        # just identified. Now the pullback is measured directly against
        # THIS impulse's own wave (t1's true extreme -> impulse_extreme),
        # using the exact correction candles returned by the setup finder
        # (the retracement wave starting right after the impulse ends).
        wave_anchor = t1["high"] if direction == "SELL" else t1["low"]
        wave_size = abs(wave_anchor - impulse_extreme)
        if wave_size == 0 or not correction_candles:
            continue
        if direction == "SELL":
            correction_extreme = max(x["high"] for x in correction_candles)
            pullback_ratio = (correction_extreme - impulse_extreme) / wave_size
        else:
            correction_extreme = min(x["low"] for x in correction_candles)
            pullback_ratio = (impulse_extreme - correction_extreme) / wave_size
        pb_valid, pb_score_mult = correction_score(pullback_ratio)
        if not pb_valid:
            # Less than 20% pullback → early entry risk, skip
            continue

        # Step 7: Fibonacci drawn over the ENTIRE impulse — from t1 to the
        # extreme reached by the impulse's last candle (not just t2/t3).
        #
        # FIX (strategy audit, round 2 — Fibonacci anchor): the leg used to
        # start from t1's OPEN, which understates the move whenever t1 has a
        # wick beyond its open. The leg must represent the full small wave,
        # so it now starts from t1's true wick extreme (its high for a
        # bearish t1 / SELL setup, its low for a bullish t1 / BUY setup) —
        # the same reference point already used for the stop loss below.
        if direction == "SELL":
            fib_high = t1["high"]
            fib_low  = impulse_extreme
            # Entry is the pullback retracement level: price pulling back
            # UP from the low toward the high before continuing down. That
            # sits close to fib_high, which is fibonacci_level()'s output
            # (see its definition) — confirmed against a live TradingView
            # chart. Unchanged from the SELL fix; do not modify.
            fib_618  = fibonacci_level(fib_high, fib_low, 0.618)
        else:
            fib_low  = t1["low"]
            fib_high = impulse_extreme
            # FIX (BUY Fibonacci direction, final v2): same resolution as
            # Hammer Fib BUY — the ladder is drawn REVERSED for BUY (swing
            # low=1.0, swing high=0.0), so the price on the "0.618" line is
            # high - 0.618*(high-low), close to the low. BUY-specific
            # arithmetic again (not the shared SELL-only fibonacci_level()
            # helper). Only the chart rendering direction changes now, not
            # this price — see chart_renderer.py/drawing_tools.py.
            fib_618  = fib_high - (fib_high - fib_low) * 0.618

        # SL "behind the first opposite candle" per spec — t1's full wick.
        stop_ref = t1["high"] if direction == "SELL" else t1["low"]
        _atr_on, _atr_mult = _atr_buffer_settings(state, "Trigger Fibonacci")
        stop  = calc_stop_from_reference(c, direction, stop_ref,
                                          atr_buffer=_atr_mult, atr_enabled=_atr_on)
        entry = fib_618

        # BUG FIX (TP1==TP2 display bug): this used to force tp1 = tp2 = the
        # 3R price (a "single fixed target, no partial TP1" design). But
        # build_message() unconditionally labels TP1 as "(1.5R)" for every
        # strategy, with no per-strategy override — so that design made this
        # strategy's signals display "TP1: <3R price> (1.5R)", i.e. a TP1
        # value equal to TP2 under a label that says 1.5R. That mismatch is
        # the reported bug. Restored to the same shared calc_targets()
        # (1.5R/3.0R) helper every other strategy uses, so TP1 is a real,
        # distinct 1.5R value matching its label. calc_targets() itself, and
        # every other strategy's call to it, is unchanged.
        tp1, tp2 = calc_targets(entry, stop, direction)

        # Scoring inputs: whether t1 is a dominant (not just minimally
        # qualifying) candle, whether t2's break of t1 is decisive rather
        # than marginal, and how tight the level touch is.
        t1_body = abs(t1["close"] - t1["open"])
        t1_idx = c.index(t1)
        t1_dominance = t1_body > 0 and t1_body >= 2.5 * avg_body(c[max(0, t1_idx - 20):t1_idx])
        if direction == "SELL":
            cond2_decisive = t2["close"] < t1["low"] * 0.998
        else:
            cond2_decisive = t2["close"] > t1["high"] * 1.002
        tight_level_touch = abs(price - near) / near < 0.004
        base_score = calc_score([t1_dominance, cond2_decisive, tight_level_touch], sr_score=sr_sc_tf)
        score = max(1, min(100, round(base_score * pb_score_mult)))
        return {
            "symbol": symbol, "direction": direction,
            "strategy": "Trigger Fibonacci", "price": price,
            "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
            "timeframe": f"1H/15m zone | Entry {tf}", "score": score,
            "pullback_ratio": round(pullback_ratio, 3),
            # ── ADDED (chart visualization feature, purely additive) ──
            # Reuses fib_high/fib_low/trend_wick/near/t1_idx already
            # computed above so the renderer can draw the exact impulse
            # Fibonacci + market-structure break (BOS) line this strategy
            # used, with zero new analysis.
            "visual": {
                "type": "fib_impulse",
                "entry_tf": tf,
                "candles": c,
                "trigger_idx": len(c) - 1,
                "fib_high": fib_high,
                "fib_low": fib_low,
                "fib_high_idx": t1_idx if direction == "SELL" else None,
                "fib_low_idx": t1_idx if direction == "BUY" else None,
                "zone_level": near,
                "trend_wick": trend_wick,
                "t1_idx": t1_idx,
            },
        }
    return None

# ─── TP2 POTENTIAL DETECTOR ────────────────────────────────────────────────────
def calc_tp2_potential_score(candles_1h: list, candles_4h: list, direction: str) -> int:
    """
    امتیاز پتانسیل رسیدن به TP2 (۰ تا ۱۰۰) بر اساس چند عامل کلیدی:
    ۱. عمق حرکت: آیا بازار در گذشته حرکت‌های بزرگ ≥3R داشته؟
    ۲. ساختار موج: آیا موج‌های ایمپالس قوی با pullback کم وجود دارد؟
    ۳. momentum قوی در تایم ۴ ساعته: EMA شیب‌دار + حجم بالا
    ۴. فضای باز تا ناحیه مقاومت/حمایت بعدی (فاصله کافی برای رسیدن به TP2)

    سیگنال‌هایی که امتیاز TP2 Potential بالایی دارند در پیام تلگرام با تگ 🚀 نشان داده می‌شوند.
    """
    if not candles_1h or len(candles_1h) < 50:
        return 50  # default mid

    score = 0

    # ── ۱. بررسی حرکت‌های بزرگ در گذشته (momentum history) ──
    closes_1h = [c["close"] for c in candles_1h]
    # آیا در ۵۰ کندل گذشته، حرکتی بیش از ۳٪ در ۱۰ کندل پیدا می‌کنیم؟
    big_moves = 0
    for i in range(10, len(closes_1h)):
        move_pct = abs(closes_1h[i] - closes_1h[i-10]) / closes_1h[i-10] * 100
        if move_pct >= 3.0:
            big_moves += 1
    if big_moves >= 5:
        score += 25
    elif big_moves >= 2:
        score += 15
    elif big_moves >= 1:
        score += 8

    # ── ۲. Momentum: RSI در ناحیه قوی (نه اشباع) ──
    rsis = calc_rsi(closes_1h, 14)
    rsi_now = next((r for r in reversed(rsis) if r is not None), None)
    if rsi_now is not None:
        if direction == "BUY" and 50 <= rsi_now <= 70:
            score += 20  # روند صعودی قوی بدون اشباع
        elif direction == "SELL" and 30 <= rsi_now <= 50:
            score += 20
        elif direction == "BUY" and 45 <= rsi_now < 50:
            score += 10
        elif direction == "SELL" and 50 < rsi_now <= 55:
            score += 10

    # ── ۳. شیب EMA200 همسو با سیگنال ──
    ema_slope = calc_ema200_slope(closes_1h)
    if (direction == "BUY" and ema_slope == "UP") or (direction == "SELL" and ema_slope == "DOWN"):
        score += 20
    elif ema_slope == "FLAT":
        score += 8

    # ── ۴. فضای باز: ناحیه S/R بعدی در فاصله کافی ──
    if candles_4h and len(candles_4h) >= 20:
        scored_4h = find_sr_levels_scored(candles_4h)
        price = closes_1h[-1]
        if scored_4h:
            if direction == "BUY":
                resistances = [sl["price"] for sl in scored_4h if sl["price"] > price]
                if resistances:
                    nearest_res = min(resistances)
                    space_pct = (nearest_res - price) / price * 100
                    if space_pct >= 4.0:
                        score += 25  # فضای زیاد تا مقاومت بعدی
                    elif space_pct >= 2.5:
                        score += 15
                    elif space_pct >= 1.5:
                        score += 8
                else:
                    score += 20  # هیچ مقاومتی بالاتر نیست — فضای کاملاً باز
            else:
                supports = [sl["price"] for sl in scored_4h if sl["price"] < price]
                if supports:
                    nearest_sup = max(supports)
                    space_pct = (price - nearest_sup) / price * 100
                    if space_pct >= 4.0:
                        score += 25
                    elif space_pct >= 2.5:
                        score += 15
                    elif space_pct >= 1.5:
                        score += 8
                else:
                    score += 20

    # ── ۵. ADX قوی (روند واضح) ──
    adx = calc_adx(candles_1h[-50:] if len(candles_1h) >= 50 else candles_1h, 14)
    if adx >= 35:
        score += 10
    elif adx >= 25:
        score += 5

    return min(100, score)


TP2_POTENTIAL_THRESHOLD = 65  # امتیاز بالاتر از این → سیگنال با 🚀 نشان داده می‌شود

# ─── COOLDOWN TRACKER ─────────────────────────────────────────────────────────
_last_signal: dict[str, float] = {}   # key: "symbol_strategy_direction" → timestamp
_filter_cooldown: dict[str, float] = {}  # Patch #8/#13: symbol → time when filter cooldown expires

# Patch #9: Duplicate guard uses (symbol + strategy + direction)
SIGNAL_COOLDOWN_WINDOW = SIGNAL_COOLDOWN       # 3600s for confirmed signals
FILTER_RESCAN_COOLDOWN = 10 * 60               # Patch #8/#13: 10 minutes after filter rejection

def can_signal(symbol: str, strategy: str, open_keys: set = None, direction: str = "") -> bool:
    """Patch #9: Duplicate guard on (symbol + strategy + direction) within cooldown window.
    FIX (audit #1): this function now only CHECKS the cooldown/duplicate state — it no
    longer registers `_last_signal[key]` itself. Registration happens only once a
    candidate has actually passed all filters and is being sent (see mark_signal_sent()),
    so a candidate that is later rejected by run_live_filters()/score/RR no longer
    consumes the cooldown window for that symbol+strategy+direction.
    """
    key = f"{symbol}_{strategy}_{direction}"
    now = time.time()
    if key in _last_signal and now - _last_signal[key] < SIGNAL_COOLDOWN_WINDOW:
        remaining = SIGNAL_COOLDOWN_WINDOW - (now - _last_signal[key])
        log.info(
            f"Signal rejection | ref={generate_rejection_ref()} DUPLICATE/COOLDOWN BLOCKED: "
            f"{symbol} | {strategy} | {direction} | remaining={remaining:.0f}s"
        )
        return False
    if open_keys is not None and f"{symbol}_{strategy}" in open_keys:
        log.info(
            f"Signal rejection | ref={generate_rejection_ref()} DUPLICATE/COOLDOWN BLOCKED: "
            f"{symbol} | {strategy} | {direction} | reason=open_trade_exists"
        )
        return False
    return True

def mark_signal_sent(symbol: str, strategy: str, direction: str = "") -> None:
    """FIX (audit #1): registers the cooldown timestamp for a signal that has actually
    passed all filters and is being sent. Must be called at the point of dispatch,
    not when a strategy merely produces a raw candidate."""
    key = f"{symbol}_{strategy}_{direction}"
    _last_signal[key] = time.time()

def mark_filtered_for_rescan(symbol: str):
    """Patch #8/#13: Mark a symbol for re-scan after filter rejection cooldown."""
    _filter_cooldown[symbol] = time.time() + FILTER_RESCAN_COOLDOWN

def is_in_filter_cooldown(symbol: str) -> bool:
    """Returns True if symbol is still in filter cooldown (not yet ready for rescan)."""
    expiry = _filter_cooldown.get(symbol)
    if expiry is None:
        return False
    if time.time() >= expiry:
        del _filter_cooldown[symbol]
        return False
    return True

def get_open_trade_keys() -> set:
    """مجموعه‌ای از کلیدهای 'symbol_strategy' که هنوز معامله فعال دارند.
    Patch #6: شامل PENDING، ENTERED، و TP1_HIT (در انتظار TP2 یا SL) می‌شود."""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.symbol, s.strategy FROM signals s
            JOIN results r ON r.signal_id = s.id
            WHERE r.status IN ('OPEN','PENDING','ENTERED','TP1_HIT')
        """).fetchall()
        return {f"{row['symbol']}_{row['strategy']}" for row in rows}
    finally:
        conn.close()

# ─── STRATEGY 5: EXHAUSTION (ضعیف شدن روند در ناحیه مهم) ──────────────────────
async def strategy_exhaustion(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Optional[dict]:
    """
    روند صعودی به ناحیه مقاومتی میرسه:
      - هرچه نزدیک‌تر، کندل‌های صعودی ضعیف‌تر و حجم کمتر → SELL
    روند نزولی به ناحیه حمایتی میرسه:
      - هرچه نزدیک‌تر، کندل‌های نزولی ضعیف‌تر و حجم کمتر → BUY
    """
    c1h = await get_candles(session, symbol, "1h", 120)
    c4h = await get_candles(session, symbol, "4h",  80)
    if not c1h or not c4h: return None

    # FIX v3.2: use scored S&R so sr_score propagates to calc_score
    scored_1h = find_sr_levels_scored(c1h)
    scored_4h = find_sr_levels_scored(c4h)
    all_scored_ex = scored_1h + scored_4h
    levels = [sl["price"] for sl in all_scored_ex]
    if not levels: return None

    price = c1h[-1]["close"]

    # پیدا کردن ناحیه نزدیک (tolerance بیشتر چون داریم نزدیک شدن رو چک میکنیم)
    near = is_near_level(price, levels, tol=0.012)
    if near is None:
        diag_count("Exhaustion", "rejected_no_zone")
        return None
    diag_count("Exhaustion", "passed_zone")

    # کندل‌های ۱۵ دقیقه برای بررسی ضعیف شدن
    c15 = await get_candles(session, symbol, "15m", 60)
    if len(c15) < 20: return None

    # تعیین جهت روند اصلی از ۱ ساعته
    trend_up = c1h[-15]["close"] < c1h[-1]["close"]

    # آخرین ۱۰ کندل نزدیک به ناحیه
    recent = c15[-10:]

    if trend_up and price < near * 1.001:
        diag_count("Exhaustion", "raw_candidate_sell_side")
        # بررسی ضعیف شدن کندل‌های صعودی
        bull_candles = [c for c in recent if c["close"] > c["open"]]
        if len(bull_candles) < 3:
            diag_count("Exhaustion", "rejected_insufficient_candles")
            return None

        # بدنه کندل‌های صعودی باید کوچک‌تر بشن
        bodies = [abs(c["close"] - c["open"]) for c in bull_candles]
        body_weakening = bodies[-1] < bodies[0] * 0.7  # آخری حداقل ۳۰٪ کوچکتر از اولی

        # حجم کندل‌های صعودی باید کم بشه
        vols = [c["volume"] for c in bull_candles]
        vol_weakening = vols[-1] < vols[0] * 0.75

        # کندل آخر باید صعودی ضعیف یا نزولی باشه
        last_weak = abs(recent[-1]["close"] - recent[-1]["open"]) < avg_body(c15[-20:]) * 0.8

        # BUG FIX (strategy audit): was `or` between body_weakening/vol_weakening,
        # which meant a candle only needed to weaken on ONE dimension (body OR
        # volume) to count as "exhaustion". A genuinely exhausting move should
        # be losing steam on both fronts; requiring only one let strong-volume-
        # but-shrinking-body (or vice versa) continuation candles false-trigger
        # reversal signals. Changed to `and`.
        if (body_weakening and vol_weakening) and last_weak:
            diag_count("Exhaustion", "passed_weakening_sell_side")
            # Patch #3a: Close Confirmation
            # BUG FIX (strategy audit): was passing `price` (== c1h[-1]["close"],
            # essentially the same market moment as c15[-1]) as the breakout
            # reference level. That makes `candle.close < zone_level` compare
            # the candle's close to a value equal to (or a hair off from)
            # itself — a near-tautological, unstable check, not a real
            # structural confirmation. This is the exact same class of bug
            # already found and fixed in RSI Divergence (see "QA report #1,
            # CRITICAL" there): use the actual detected S/R zone level (`near`)
            # instead, consistent with every other is_valid_zone_breakout()
            # call site in this file (Stop Hunter, Hammer Fib, Trigger
            # Fibonacci, RSI Divergence all pass a genuine structural
            # reference, never the entry price itself).
            if not is_valid_zone_breakout(c15[-1], near, "SELL"):
                diag_count("Exhaustion", "rejected_close_confirmation")
                return None
            stop  = calc_safe_stop(c15, "SELL", price)  # Patch #13
            entry = price
            tp1, tp2 = calc_targets(entry, stop, "SELL")
            # FIX v3.2: use sr_score from nearest scored S&R
            nearest_sr_ex = get_nearest_sr_scored(near, all_scored_ex) if near else None
            sr_sc_ex = nearest_sr_ex["sr_score"] if nearest_sr_ex else None
            in_4h = any(abs(near - sl["price"]) / sl["price"] < 0.012 for sl in scored_4h)
            # BUG FIX (strategy audit): after the OR→AND fix above, body_weakening
            # and vol_weakening are now BOTH required true to reach this line, so
            # passing them into calc_score no longer varies anything (same fake-
            # score problem found elsewhere). Replaced with strength tiers that
            # exceed the minimum bar: a >50%/>40% collapse rather than the
            # minimum-qualifying 30%/25% used to gate entry.
            strong_body_weakening = bodies[-1] < bodies[0] * 0.5
            strong_vol_weakening  = vols[-1] < vols[0] * 0.6
            score = calc_score([strong_body_weakening, strong_vol_weakening, in_4h], sr_score=sr_sc_ex)
            diag_count("Exhaustion", "signal_emitted")
            return {
                "symbol": symbol, "direction": "SELL",
                "strategy": "Exhaustion", "price": price,
                "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                "timeframe": "1H/4H zone | 15m exhaustion", "score": score,
                # ── ADDED (chart visualization feature, purely additive) ──
                "visual": {
                    "type": "exhaustion",
                    "entry_tf": "15m",
                    "candles": c15,
                    "trigger_idx": len(c15) - 1,
                    "zone_level": near,
                },
            }

    elif not trend_up and price > near * 0.999:
        diag_count("Exhaustion", "raw_candidate_buy_side")
        # بررسی ضعیف شدن کندل‌های نزولی
        bear_candles = [c for c in recent if c["close"] < c["open"]]
        if len(bear_candles) < 3:
            diag_count("Exhaustion", "rejected_insufficient_candles")
            return None

        bodies = [abs(c["close"] - c["open"]) for c in bear_candles]
        body_weakening = bodies[-1] < bodies[0] * 0.7

        vols = [c["volume"] for c in bear_candles]
        vol_weakening = vols[-1] < vols[0] * 0.75

        last_weak = abs(recent[-1]["close"] - recent[-1]["open"]) < avg_body(c15[-20:]) * 0.8

        # BUG FIX (strategy audit): same OR→AND fix as the SELL branch above.
        if (body_weakening and vol_weakening) and last_weak:
            diag_count("Exhaustion", "passed_weakening_buy_side")
            # Patch #3a: Close Confirmation
            # BUG FIX (strategy audit): see SELL branch above for rationale —
            # same fix, use the actual zone level (`near`) instead of `price`.
            if not is_valid_zone_breakout(c15[-1], near, "BUY"):
                diag_count("Exhaustion", "rejected_close_confirmation")
                return None
            stop  = calc_safe_stop(c15, "BUY", price)  # Patch #13
            entry = price
            tp1, tp2 = calc_targets(entry, stop, "BUY")
            # FIX v3.2: use sr_score from nearest scored S&R
            nearest_sr_ex = get_nearest_sr_scored(near, all_scored_ex) if near else None
            sr_sc_ex = nearest_sr_ex["sr_score"] if nearest_sr_ex else None
            in_4h = any(abs(near - sl["price"]) / sl["price"] < 0.012 for sl in scored_4h)
            # BUG FIX: see SELL branch above for rationale.
            strong_body_weakening = bodies[-1] < bodies[0] * 0.5
            strong_vol_weakening  = vols[-1] < vols[0] * 0.6
            score = calc_score([strong_body_weakening, strong_vol_weakening, in_4h], sr_score=sr_sc_ex)
            diag_count("Exhaustion", "signal_emitted")
            return {
                "symbol": symbol, "direction": "BUY",
                "strategy": "Exhaustion", "price": price,
                "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                "timeframe": "1H/4H zone | 15m exhaustion", "score": score,
                # ── ADDED (chart visualization feature, purely additive) ──
                "visual": {
                    "type": "exhaustion",
                    "entry_tf": "15m",
                    "candles": c15,
                    "trigger_idx": len(c15) - 1,
                    "zone_level": near,
                },
            }

    return None

# ─── STRATEGY 6: RSI DIVERGENCE / OVERBOUGHT-OVERSOLD ────────────────────────
async def strategy_rsi_divergence(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Optional[dict]:
    """
    وقتی RSI (تایم 15 دقیقه) به بالای ۷۰ میرسد و دوباره به زیر ۷۰ برمیگردد → SELL
    وقتی RSI به زیر ۳۰ میرسد و دوباره به بالای ۳۰ برمیگردد → BUY
    همچنین واکینش‌های ساده (Divergence) بین قیمت و RSI روی سقف/کف‌های اخیر بررسی میشود.
    """
    c = await get_candles(session, symbol, "15m", 100)
    if len(c) < 30: return None

    closes = [x["close"] for x in c]
    highs  = [x["high"]  for x in c]
    lows   = [x["low"]   for x in c]
    rsis   = calc_rsi(closes, 14)

    if rsis[-1] is None or rsis[-2] is None:
        return None

    price       = closes[-1]
    rsi_now     = rsis[-1]
    rsi_prev    = rsis[-2]

    # ── خروج از ناحیه اشباع خرید (Overbought) → SELL ──
    # BUG FIX (strategy audit): the original trigger `rsi_prev >= 70 and
    # rsi_now < 70` fires on a single-tick boundary cross — RSI going from
    # 70.01 to 69.99 for one candle (pure noise near the threshold, with no
    # sustained overbought condition beforehand) was enough to trigger a
    # reversal signal with no minimum drop magnitude required. Added:
    # (a) a minimum RSI drop of 3 points, and (b) a requirement that RSI was
    # actually overbought for more than one candle beforehand (sustained,
    # not a single spike), to filter out boundary noise.
    sustained_overbought = rsis[-3] is not None and rsis[-3] >= 68
    rsi_drop_meaningful = (rsi_prev - rsi_now) >= 3
    if rsi_prev >= 70 and rsi_now < 70 and sustained_overbought and rsi_drop_meaningful:
        diag_count("RSI Divergence", "passed_rsi_crossing_sell_side")
        # بررسی Bearish Divergence: قیمت سقف بالاتر زده ولی RSI سقف پایین‌تر زده
        recent_rsi_vals  = [r for r in rsis[-20:] if r is not None]
        divergence = False
        if len(recent_rsi_vals) >= 10:
            past_high_idx  = highs[-20:-5].index(max(highs[-20:-5])) if len(highs) >= 20 else 0
            recent_high    = max(highs[-5:])
            past_high      = max(highs[-20:-5]) if len(highs) >= 20 else 0
            if recent_high > past_high and rsi_now < max(recent_rsi_vals[:10] or [0]):
                divergence = True

        # Patch #3a: Close Confirmation — کندل باید نزولی بسته شده باشد
        # BUG FIX (QA report #1, CRITICAL): this used to pass `price` (which
        # equals c[-1]["close"]) as the breakout level, making the check
        # `candle.close < zone_level` compare the candle's close to itself —
        # always False, so this strategy could never fire. Use the recent
        # swing low (excluding the current candle) as the actual reference
        # level to break below, consistent with the recent_low/recent_high
        # pattern used for is_valid_zone_breakout() elsewhere in this file.
        recent_swing_low = min(lows[-6:-1])
        if not is_valid_zone_breakout(c[-1], recent_swing_low, "SELL"):
            diag_count("RSI Divergence", "rejected_close_confirmation")
            return None
        diag_count("RSI Divergence", "passed_close_confirmation")
        stop  = calc_safe_stop(c, "SELL", price)  # Patch #13
        entry = price
        tp1, tp2 = calc_targets(entry, stop, "SELL")
        # BUG FIX: two of the four original factors (rsi_prev>=70, rsi_now<70)
        # were already required true by the gate above, so they never varied.
        # Replaced with the real, independently-varying strength signal.
        score = calc_score([divergence, rsi_now < 65, (rsi_prev - rsi_now) >= 5])
        diag_count("RSI Divergence", "signal_emitted")
        return {
            "symbol": symbol, "direction": "SELL",
            "strategy": "RSI Divergence", "price": price,
            "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
            "timeframe": f"15m | RSI: {rsi_now:.1f} (reversed from above 70)" + (" | Divergence" if divergence else ""),
            "score": score,
            "rsi": round(rsi_now, 1),
            # ── ADDED (chart visualization feature, purely additive) ──
            # rsis is the exact RSI series already computed above.
            "visual": {
                "type": "rsi_divergence",
                "entry_tf": "15m",
                "candles": c,
                "trigger_idx": len(c) - 1,
                "divergence": divergence,
                "rsi_series": rsis,
            },
        }

    # ── خروج از ناحیه اشباع فروش (Oversold) → BUY ──
    # BUG FIX: same boundary-noise issue as the SELL branch above.
    sustained_oversold = rsis[-3] is not None and rsis[-3] <= 32
    rsi_rise_meaningful = (rsi_now - rsi_prev) >= 3
    if rsi_prev <= 30 and rsi_now > 30 and sustained_oversold and rsi_rise_meaningful:
        diag_count("RSI Divergence", "passed_rsi_crossing_buy_side")
        recent_rsi_vals = [r for r in rsis[-20:] if r is not None]
        divergence = False
        if len(recent_rsi_vals) >= 10:
            recent_low = min(lows[-5:])
            past_low   = min(lows[-20:-5]) if len(lows) >= 20 else 0
            if recent_low < past_low and rsi_now > min(recent_rsi_vals[:10] or [100]):
                divergence = True

        # Patch #3a: Close Confirmation — کندل باید صعودی بسته شده باشد
        # BUG FIX (QA report #1, CRITICAL): see SELL branch above — use the
        # recent swing high (excluding the current candle) as the actual
        # reference level to break above, instead of the candle's own close.
        recent_swing_high = max(highs[-6:-1])
        if not is_valid_zone_breakout(c[-1], recent_swing_high, "BUY"):
            diag_count("RSI Divergence", "rejected_close_confirmation")
            return None
        diag_count("RSI Divergence", "passed_close_confirmation")
        stop  = calc_safe_stop(c, "BUY", price)  # Patch #13
        entry = price
        tp1, tp2 = calc_targets(entry, stop, "BUY")
        # BUG FIX: see SELL branch above for rationale.
        score = calc_score([divergence, rsi_now > 35, (rsi_now - rsi_prev) >= 5])
        diag_count("RSI Divergence", "signal_emitted")
        return {
            "symbol": symbol, "direction": "BUY",
            "strategy": "RSI Divergence", "price": price,
            "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
            "timeframe": f"15m | RSI: {rsi_now:.1f} (reversed from below 30)" + (" | Divergence" if divergence else ""),
            "score": score,
            "rsi": round(rsi_now, 1),
            # ── ADDED (chart visualization feature, purely additive) ──
            "visual": {
                "type": "rsi_divergence",
                "entry_tf": "15m",
                "candles": c,
                "trigger_idx": len(c) - 1,
                "divergence": divergence,
                "rsi_series": rsis,
            },
        }

    return None

# ─── MAIN SCANNER ─────────────────────────────────────────────────────────────
STRATEGIES = [
    ("Stop Hunter",         strategy_stop_hunter),
    ("Hammer Fib (4H)",     strategy_hammer_fib),
    ("Hammer Fib (1H)",     strategy_hammer_fib_1h),
    ("HB",                  strategy_hb),
    ("Trigger Fibonacci",   strategy_trigger_fib),
    ("Exhaustion",          strategy_exhaustion),
    ("RSI Divergence",      strategy_rsi_divergence),
]

# ─── NEW STRATEGY FILTERS (Open Interest / Funding Rate / BTC Trend / News) ──
async def get_open_interest_change_pct(session, symbol: str, period: str = "5m", limit: int = 12) -> Optional[float]:
    """درصد تغییر Open Interest در بازه اخیر — برای تشخیص قدرت پول ورودی/خروجی.

    REPRODUCIBILITY FIX (root cause C): Binance's OI-history endpoint has no
    historical/"as of" mode — it only ever returns recent live data. Unlike
    get_candles(), there is no historical series to serve here during a
    Replay job. Returning None during replay is not a behavior change to
    this filter: run_live_filters() already treats a None OI reading as
    "unknown, don't block" (see the oi_filter_enabled block) — the same
    fail-open handling already used for a live API error. This just makes
    that the deterministic outcome during replay instead of occasionally
    leaking today's live OI into a historical evaluation.
    """
    if _replay_ctx.get() is not None:
        return None
    try:
        data = await binance_request(session, "https://fapi.binance.com/futures/data/openInterestHist",
                                      params={"symbol": symbol, "period": period, "limit": limit}, timeout=10)
        if not isinstance(data, list) or len(data) < 2:
            return None
        first = float(data[0]["sumOpenInterest"])
        last  = float(data[-1]["sumOpenInterest"])
        if first == 0:
            return None
        return (last - first) / first * 100.0
    except Exception:
        return None

async def get_funding_rate(session, symbol: str) -> Optional[float]:
    """آخرین نرخ Funding Rate (به درصد) — برای تشخیص معاملات شلوغ/اورکراودد.

    REPRODUCIBILITY FIX (root cause C): same reasoning as get_open_interest_
    change_pct() above — /premiumIndex only ever returns the current live
    funding rate, with no historical window available. During replay this
    now returns None (already-existing fail-open path for the
    funding_filter_enabled check) instead of reading today's live funding
    rate into a historical evaluation.
    """
    if _replay_ctx.get() is not None:
        return None
    try:
        data = await binance_request(session, f"{BINANCE_BASE}/premiumIndex", params={"symbol": symbol}, timeout=10)
        if not isinstance(data, dict):
            return None
        rate = data.get("lastFundingRate")
        return float(rate) * 100.0 if rate is not None else None
    except Exception:
        return None

async def is_btc_trend_aligned(session, direction: str, timeframe: str) -> Optional[bool]:
    """بررسی هم‌جهت بودن جهت سیگنال با روند بیت‌کوین در تایم‌فریم انتخابی."""
    try:
        candles = await get_candles(session, "BTCUSDT", timeframe, 250)
        closes = [c["close"] for c in candles]
        slope = calc_ema200_slope(closes)
        if slope == "UP":
            return direction == "BUY"
        if slope == "DOWN":
            return direction == "SELL"
        return True  # روند خنثی → مسدود نشود
    except Exception:
        return None

def is_within_news_blackout(state: dict, before_minutes: int, after_minutes: int) -> bool:
    """بررسی می‌کند آیا زمان فعلی داخل بازه بلک‌اوت یک خبر مهم (state['news_events']) است.
    news_events لیستی از timestampهای ISO است که از بیرون (مثلاً ادمین یا یک سرویس اقتصادی) پر می‌شود؛
    اگر لیست خالی باشد، این فیلتر هیچ سیگنالی را بلاک نمی‌کند.

    REPRODUCIBILITY FIX (root cause C): during a Replay job this must check
    the SIMULATED historical moment, not real wall-clock time — otherwise a
    historical candidate is judged against whether today happens to be near
    a configured news event, which is both wrong (it should be judged
    against whether the ORIGINAL simulated moment was near one) and
    non-deterministic (depends on which real day the replay is run on).
    Uses the same _replay_ctx contextvar as get_current_session()/
    get_candles() — live scanning never sets it, so live behavior (real
    wall-clock time) is unchanged.
    """
    from datetime import timedelta
    events = state.get("news_events", [])
    if not events:
        return False
    _cache = _replay_ctx.get()
    if _cache is not None and _cache.sim_time_ms:
        now = datetime.fromtimestamp(_cache.sim_time_ms / 1000, tz=timezone.utc)
    else:
        now = datetime.now(timezone.utc)
    for ev in events:
        try:
            ev_time = datetime.fromisoformat(ev)
            if ev_time.tzinfo is None:
                ev_time = ev_time.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        window_start = ev_time - timedelta(minutes=before_minutes)
        window_end = ev_time + timedelta(minutes=after_minutes)
        if window_start <= now <= window_end:
            return True
    return False

# ═══════════════════════════════════════════════════════════════════════════════
# 🟢 LAYER A — LIVE SIGNAL ENGINE  (all filters MUST pass, else signal blocked)
# ═══════════════════════════════════════════════════════════════════════════════
async def run_live_filters(session, symbol: str, result: dict, state: dict, cfg: dict = None) -> tuple[bool, list[str]]:
    """
    زنجیره کامل فیلترهای Live Engine.
    خروجی: (True, []) اگر همه فیلترها پاس شوند — (False, [reasons]) اگر هر فیلتری بلاک کند.
    هر فیلتر شکست‌خورده دلیلش را در لیست reasons می‌گذارد.

    v3.5 Patches applied here:
    Patch #3b: Volume >= SMA20 (hard filter)
    Patch #3c: ATR too low → dead market → block
    Patch #3d: ATR too high → crazy market → block (via existing abnormal_volatility)
    Patch #4:  3-TF trend alignment (4H + 1H) — hard block for TREND, soft for REVERSAL
    Patch #5:  ADX 18-23 whipsaw zone → No Trade (handled by regime allowed_types=set())
    Patch #6:  Market Regime blocks incompatible strategies (allowed_types already in scan_symbol)
    Patch #7:  high-score signals (85+) get relaxed strictness
    Patch #10: reversal strategies — HTF alignment is soft (score penalty) not hard reject
    Patch #11: volume spikes with structure context are confirmations, not rejections
    Patch #12: 3+ confluences soften weak filters
    """
    reasons = []
    direction  = result["direction"]
    score      = result.get("score", 0)
    strategy   = result.get("strategy", "")
    strat_type = STRATEGY_REGIME_TYPE.get(strategy, "HYBRID")
    diag_count(strategy, "reached_shared_filters")
    diag_score(strategy, score)

    # ── Patch #7: high-score relaxation ──
    high_score = score >= 85
    very_high_score = score >= 86

    # ── Patch #12: Count confluences for override system ──
    confluence_count = 0
    confluence_flags = result.get("confluence_flags", [])

    # ── Strategy Settings: تنظیمات فیلترها مختص همین استراتژی ──
    cfg = get_strategy_settings(state, strategy)

    # ── 1. Session filter (per-strategy) ──
    strat_session_filters = cfg.get("session_filters", SESSION_FILTERS_DEFAULT)
    if not strat_session_filters.get(get_current_session(), True):
        reasons.append(f"session_blocked:{get_current_session()}")
        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
        return False, reasons

    # ── 2. Volume (entry candle) filter (per-strategy) ──
    entry_vol_filter_on = cfg.get("entry_volume_filter_enabled", True)
    ma_period    = cfg.get("entry_volume_ma_period", 20)
    vol_multiplier = cfg.get("entry_volume_multiplier", 1.5)
    c5m_vol = []
    if entry_vol_filter_on:
        try:
            c5m_vol = await get_candles(session, symbol, "5m", ma_period + 5)
            if not has_sufficient_entry_volume(c5m_vol, ma_period, vol_multiplier):
                if not high_score:
                    reasons.append("low_entry_volume")
                    diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                    return False, reasons
        except Exception as e:
            # AUDIT FIX (fail-closed error handling): this used to be a bare
            # `except Exception: pass`, which meant that if fetching/checking
            # entry volume failed for ANY reason (network hiccup, malformed
            # candle, etc.), the filter was silently treated as "passed" —
            # an enabled, supposedly-hard filter could be bypassed by an
            # unrelated error. Every filter in this chain now fails CLOSED
            # (rejects the signal) on an unexpected exception, logs it, and
            # gives an explicit reason — a failed check can never silently
            # let an unvalidated signal through.
            log.warning(f"run_live_filters: entry_volume check failed for {symbol}/{strategy}: {e}")
            reasons.append("entry_volume_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── Patch #3b: حجم کندل ورود باید بالاتر از SMA20 باشد ──
    try:
        if c5m_vol and not has_sufficient_volume_sma20(c5m_vol, period=20):
            if not high_score:
                reasons.append("volume_below_sma20")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons
    except Exception as e:
        log.warning(f"run_live_filters: volume_sma20 check failed for {symbol}/{strategy}: {e}")
        reasons.append("volume_sma20_check_failed")
        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
        return False, reasons

    # ── 3. ADX regime filter ──
    c1h_adx = []
    adx_val = 0.0
    try:
        c1h_adx = await get_candles(session, symbol, "1h", 100)
        adx_val = calc_adx(c1h_adx, 14)
        diag_adx(strategy, adx_val)
        if cfg.get("adx_threshold_enabled", True):
            adx_min = cfg.get("adx_threshold", ADX_THRESHOLD_DEFAULT)
            effective_adx_min = max(10.0, adx_min - 8) if very_high_score else adx_min
            if adx_val < effective_adx_min:
                if not high_score:
                    reasons.append(f"adx_too_low:{adx_val:.1f}")
                    diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                    return False, reasons
        # ── Task 2 filter: ADX Maximum (per-strategy, opt-in via adx_max) ──
        adx_max = cfg.get("adx_max")
        if adx_max is not None and adx_val > adx_max:
            if not high_score:
                reasons.append(f"adx_too_high:{adx_val:.1f}")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons
    except Exception as e:
        log.warning(f"run_live_filters: ADX check failed for {symbol}/{strategy}: {e}")
        reasons.append("adx_check_failed")
        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
        return False, reasons

    # ── Patch #3c: ATR خیلی پایین → بازار مرده → بلاک ──
    try:
        c1h_for_atr = c1h_adx if c1h_adx else await get_candles(session, symbol, "1h", 60)
        if is_atr_too_low(c1h_for_atr):
            if not high_score:
                reasons.append("atr_too_low_dead_market")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons
    except Exception as e:
        log.warning(f"run_live_filters: ATR-too-low check failed for {symbol}/{strategy}: {e}")
        reasons.append("atr_low_check_failed")
        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
        return False, reasons

    # ── 4. EMA200 slope / trend filter — SOFT for Reversal strategies (Patch #10) ──
    htf_conflict = False
    try:
        c1h_ema  = await get_candles(session, symbol, "1h", 250)
        closes_1h = [c["close"] for c in c1h_ema]
        ema_slope = calc_ema200_slope(closes_1h)
        ema_conflict = (direction == "BUY" and ema_slope == "DOWN") or \
                       (direction == "SELL" and ema_slope == "UP")
        if ema_conflict:
            if strat_type == "TREND":
                reasons.append("ema200_slope_conflict_trend")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons
            elif strat_type in ("REVERSAL", "HYBRID"):
                htf_conflict = True
                result["htf_score_penalty"] = 15
    except Exception as e:
        # TREND strategies rely on this as a hard gate — if we can't confirm
        # HTF alignment, we cannot confirm the trade is trend-aligned, so we
        # do not let it through. REVERSAL/HYBRID strategies only use this as
        # a soft score penalty; if it can't be computed, they proceed without
        # the penalty (explicitly logged, not silently swallowed).
        log.warning(f"run_live_filters: ema200_slope check failed for {symbol}/{strategy}: {e}")
        if strat_type == "TREND":
            reasons.append("ema200_slope_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── Patch #4: سه تایم‌فریم (4H + 1H + 15m) باید هم‌جهت باشند ──
    try:
        three_tf_ok = await is_three_tf_aligned(session, symbol, direction)
        if not three_tf_ok:
            if strat_type == "TREND":
                reasons.append("three_tf_misaligned")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons
            elif strat_type in ("REVERSAL", "HYBRID"):
                htf_conflict = True
                result["htf_score_penalty"] = result.get("htf_score_penalty", 0) + 8
    except Exception as e:
        log.warning(f"run_live_filters: three_tf_aligned check failed for {symbol}/{strategy}: {e}")
        if strat_type == "TREND":
            reasons.append("three_tf_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── 5. ATR volatility filter (Patch #3d: بازار خیلی پرنوسان) ──
    atr_mult = cfg.get("atr_multiplier", ATR_MULTIPLIER_DEFAULT)
    try:
        c5m_atr = await get_candles(session, symbol, "5m", 30)
        if is_volatility_abnormal(c5m_atr, threshold=atr_mult) or is_atr_too_high(c5m_atr):
            if not high_score:
                reasons.append("abnormal_volatility")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons
    except Exception as e:
        log.warning(f"run_live_filters: volatility check failed for {symbol}/{strategy}: {e}")
        reasons.append("volatility_check_failed")
        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
        return False, reasons

    # ── 6. Suspicious volume filter (Patch #11: context-aware) ──
    filter_on = state.get("volume_filter_enabled", True)
    if filter_on:
        try:
            c5m = await get_candles(session, symbol, "5m", 30)
            if has_suspicious_opposing_volume(c5m, direction):
                has_structure_context = _has_volume_structure_context(c5m, direction)
                if has_structure_context:
                    confluence_count += 1
                elif not high_score:
                    reasons.append("suspicious_opposing_volume")
                    diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                    return False, reasons
        except Exception as e:
            log.warning(f"run_live_filters: suspicious_volume check failed for {symbol}/{strategy}: {e}")
            reasons.append("suspicious_volume_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── 7. Higher-trend alignment filter ──
    if not htf_conflict:
        try:
            if not await is_aligned_with_higher_trend(session, symbol, direction):
                if strat_type == "TREND":
                    reasons.append("against_higher_tf_trend")
                    diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                    return False, reasons
                elif strat_type in ("REVERSAL", "HYBRID"):
                    htf_conflict = True
                    result["htf_score_penalty"] = result.get("htf_score_penalty", 0) + 10
        except Exception as e:
            log.warning(f"run_live_filters: higher_trend check failed for {symbol}/{strategy}: {e}")
            if strat_type == "TREND":
                reasons.append("higher_trend_check_failed")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons

    # ── Patch #10: Apply HTF penalty for reversals ──
    if htf_conflict:
        penalty = result.get("htf_score_penalty", 15)
        result["score"] = max(1, result.get("score", 0) - penalty)
        score = result["score"]

    # ── Patch #12: Confluence override — soften remaining filters if 3+ confluences ──
    total_confluences = confluence_count + len(confluence_flags)
    high_confluence = total_confluences >= 3 or (high_score and total_confluences >= 2)

    # ── 8. Score gate — Dynamic Score (Patch #11 spec) ──
    # حداقل Score بر اساس رژیم فعلی بازار به‌صورت پویا تنظیم می‌شود.
    runtime_min = get_strategy_min_score(state, strategy, cfg)
    USER_STRICT_THRESHOLD = 95
    user_is_strict = runtime_min >= USER_STRICT_THRESHOLD

    # Patch #4: Dynamic min score — آستانه‌ها از bot_config.json خوانده می‌شوند (نه hardcoded)
    regime_label = result.get("market_regime", "")
    if not user_is_strict:
        dynamic_min = get_dynamic_score_threshold(regime_label, runtime_min, cfg)
    else:
        dynamic_min = runtime_min

    effective_min_score = dynamic_min
    if not user_is_strict:
        if high_confluence:
            effective_min_score = max(50, effective_min_score - 10)
        if very_high_score:
            effective_min_score = max(50, effective_min_score - 8)

    # FIX (lifecycle patch #7): use math.ceil(score) >= threshold instead of a raw float
    # compare, so e.g. a score of 98.2 against a /setscore 99 threshold is judged fairly
    # (ceil(98.2)=99 >= 99) rather than failing on floating-point fractions.
    if not (math.ceil(score) >= effective_min_score):
        reasons.append(f"low_score:{score} (dynamic_min:{effective_min_score})")
        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
        return False, reasons

    # ── 9. Signal RR/math validation ──
    rr_ok, rr_reason = validate_signal_rr(result)
    if not rr_ok:
        reasons.append(f"invalid_rr:{rr_reason}")
        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
        return False, reasons

    # ── Task 2 filter: EMA Filter (per-strategy, opt-in) ──
    if cfg.get("ema_filter_enabled", False):
        try:
            ema_period = cfg.get("ema_filter_period", 50)
            c1h_emaf = await get_candles(session, symbol, "1h", max(ema_period + 10, 60))
            closes_emaf = [c["close"] for c in c1h_emaf]
            ema_mode = cfg.get("ema_filter_mode", "manual")
            if closes_emaf and ema_mode == "auto":
                # ── Auto EMA Trend Filter (new, isolated logic) ──
                atr_enabled_emaf = cfg.get("ema_filter_auto_atr_enabled", True)
                atr_mult_emaf = cfg.get("ema_filter_auto_atr_multiplier", 1.0)
                if not is_ema_trend_filter_auto_ok(c1h_emaf, closes_emaf, closes_emaf[-1], ema_period,
                                                    direction, atr_enabled_emaf, atr_mult_emaf):
                    if not high_score:
                        reasons.append(f"ema_auto_trend_blocked:{ema_period}")
                        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                        return False, reasons
            elif closes_emaf:
                # ── Manual EMA Filter (unchanged original behavior) ──
                ema_direction = cfg.get("ema_filter_direction", "above")
                if not is_ema_filter_ok(closes_emaf, closes_emaf[-1], ema_period, ema_direction):
                    if not high_score:
                        reasons.append(f"ema_filter_blocked:{ema_direction}_{ema_period}")
                        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                        return False, reasons
        except Exception as e:
            log.warning(f"run_live_filters: ema_filter check failed for {symbol}/{strategy}: {e}")
            reasons.append("ema_filter_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons
    # ── Task 2 filter: ATR Range Filter (per-strategy, opt-in) ──
    # ATR expressed as a % of price so the same min/max setting is meaningful
    # across symbols of very different price scales (unlike a raw ATR value).
    if cfg.get("atr_range_filter_enabled", False):
        try:
            c1h_atr_range = c1h_adx if c1h_adx else await get_candles(session, symbol, "1h", 60)
            ref_price = result.get("price") or (c1h_atr_range[-1]["close"] if c1h_atr_range else 0)
            atr_raw = calc_atr(c1h_atr_range, 14)
            atr_pct = (atr_raw / ref_price * 100) if ref_price else 0
            atr_min_pct = cfg.get("atr_min_pct")
            atr_max_pct = cfg.get("atr_max_pct")
            if atr_min_pct is not None and atr_pct < atr_min_pct:
                if not high_score:
                    reasons.append(f"atr_pct_below_min:{atr_pct:.3f}")
                    diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                    return False, reasons
            if atr_max_pct is not None and atr_pct > atr_max_pct:
                if not high_score:
                    reasons.append(f"atr_pct_above_max:{atr_pct:.3f}")
                    diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                    return False, reasons
        except Exception as e:
            log.warning(f"run_live_filters: atr_range check failed for {symbol}/{strategy}: {e}")
            reasons.append("atr_range_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── Task 2 filter: Volume Filter — Volume > SMA20 / Volume > EMA20 (per-strategy, opt-in) ──
    if cfg.get("volume_ma_filter_enabled", False):
        try:
            vol_ma_type = cfg.get("volume_ma_type", "sma")
            c5m_volma = c5m_vol if c5m_vol else await get_candles(session, symbol, "5m", 30)
            vol_ok = has_volume_above_ema20(c5m_volma) if vol_ma_type == "ema" else has_sufficient_volume_sma20(c5m_volma, period=20)
            if not vol_ok:
                if not high_score:
                    reasons.append(f"volume_ma_filter_blocked:{vol_ma_type}")
                    diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                    return False, reasons
        except Exception as e:
            log.warning(f"run_live_filters: volume_ma_filter check failed for {symbol}/{strategy}: {e}")
            reasons.append("volume_ma_filter_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── 10. Open Interest Filter — تشخیص قدرت پول ورودی ──
    # NOTE: opt-in and off by default. If an admin explicitly enables it, an
    # OI-API failure now blocks the signal (logged) rather than silently
    # treating the filter as passed — same fail-closed policy as every other
    # filter above. If OI data proves unreliable for some symbols, that
    # should be fixed at the API layer (get_open_interest_change_pct) or the
    # filter left disabled, not papered over here.
    if cfg.get("oi_filter_enabled", False):
        try:
            oi_change = await get_open_interest_change_pct(session, symbol)
            if oi_change is not None and abs(oi_change) < cfg.get("oi_threshold", 5.0):
                if not high_score:
                    reasons.append(f"oi_change_too_low:{oi_change:.2f}")
                    diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                    return False, reasons
        except Exception as e:
            log.warning(f"run_live_filters: OI filter check failed for {symbol}/{strategy}: {e}")
            reasons.append("oi_filter_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── 11. Funding Rate Filter — حذف معاملات شلوغ (Overcrowded) ──
    if cfg.get("funding_filter_enabled", False):
        try:
            funding = await get_funding_rate(session, symbol)
            limit = cfg.get("funding_rate_limit", 0.05)
            if funding is not None and abs(funding) > limit:
                reasons.append(f"funding_rate_overcrowded:{funding:.4f}")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons
        except Exception as e:
            log.warning(f"run_live_filters: funding_rate check failed for {symbol}/{strategy}: {e}")
            reasons.append("funding_filter_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── 12. BTC Trend Filter — فقط هم‌جهت با روند بیت‌کوین ──
    if cfg.get("btc_trend_filter_enabled", False):
        try:
            aligned = await is_btc_trend_aligned(session, direction, cfg.get("btc_trend_timeframe", "4h"))
            if aligned is False:
                reasons.append("against_btc_trend")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons
        except Exception as e:
            log.warning(f"run_live_filters: BTC trend check failed for {symbol}/{strategy}: {e}")
            reasons.append("btc_trend_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── 13. News Filter — پرهیز از معامله نزدیک اخبار مهم ──
    if cfg.get("news_filter_enabled", False):
        try:
            if is_within_news_blackout(state, cfg.get("news_before_minutes", 30), cfg.get("news_after_minutes", 30)):
                reasons.append("news_blackout")
                diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
                return False, reasons
        except Exception as e:
            log.warning(f"run_live_filters: news_blackout check failed for {symbol}/{strategy}: {e}")
            reasons.append("news_filter_check_failed")
            diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
            return False, reasons

    # ── Patch #12 (spec): TP2 Probability gate ──
    # اگر احتمال رسیدن به TP2 خیلی پایین باشد → سیگنال فقط با TP1 فرستاده می‌شود
    # یا اگر خیلی پایین بود → کلاً بلاک
    tp2_prob = result.get("tp2_potential", 50)
    tp2_min_threshold = 30  # زیر این → سیگنال بلاک
    if tp2_prob < tp2_min_threshold and not high_score:
        # فقط اگر score بالا نیست بلاک می‌کنیم
        reasons.append(f"tp2_probability_too_low:{tp2_prob}")
        diag_count(strategy, "rejected:" + (reasons[-1].split(":")[0] if reasons else "unknown"))
        return False, reasons
    elif tp2_prob < 45:
        # TP2 محتمل نیست → فقط TP1 پیشنهاد بشود (flag برای build_message)
        result["tp2_unlikely"] = True

    diag_count(strategy, "passed_all_filters")
    return True, []


def _has_volume_structure_context(candles: list[dict], direction: str) -> bool:
    """
    Patch #11: Returns True if high volume candle appears with structural context
    (liquidity sweep, divergence signal, exhaustion candle, or strong reversal candle).
    """
    if len(candles) < 10:
        return False
    avg_vol = avg_volume(candles[:-3], 20) if len(candles) > 23 else avg_volume(candles, len(candles)-1)
    if avg_vol == 0:
        return False
    recent = candles[-5:]
    for c in recent:
        body = abs(c["close"] - c["open"])
        high_vol = c["volume"] >= 1.8 * avg_vol
        if not high_vol:
            continue
        # Strong reversal candle: body >= 60% of range
        total_range = c["high"] - c["low"]
        strong_candle = total_range > 0 and body / total_range >= 0.6
        # Wick sweep: long shadow toward opposite direction (liquidity sweep)
        if direction == "BUY":
            lower_shadow = min(c["open"], c["close"]) - c["low"]
            sweep = lower_shadow >= body * 1.5 and c["close"] > c["open"]
        else:
            upper_shadow = c["high"] - max(c["open"], c["close"])
            sweep = upper_shadow >= body * 1.5 and c["close"] < c["open"]
        if strong_candle or sweep:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# 📊 LAYER B — BACKTEST ENGINE  (async, batched, queued, cached)
# ═══════════════════════════════════════════════════════════════════════════════
_backtest_queue: asyncio.Queue = None          # init در main()
_backtest_cache: dict = {}                     # job_id → result dict
_backtest_running = False

def _init_backtest_queue():
    global _backtest_queue
    _backtest_queue = asyncio.Queue(maxsize=BACKTEST_MAX_QUEUE)

def _bt_job_id(params: dict) -> str:
    import hashlib
    return hashlib.md5(json.dumps(params, sort_keys=True).encode()).hexdigest()[:12]

async def _fetch_candles_for_backtest(session, symbol: str, interval: str, timerange_hours: int) -> list:
    """تعداد کندل‌های مناسب برای بازه زمانی خواسته‌شده (با احتساب تبدیل ساعت به تعداد کندل)."""
    interval_minutes = {"1h": 60, "4h": 240, "1m": 1, "5m": 5, "15m": 15}
    mins = interval_minutes.get(interval, 60)
    limit = min(1000, max(50, (timerange_hours * 60) // mins + 50))
    return await get_candles(session, symbol, interval, limit)

INTERVAL_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
               "1h": 3_600_000, "4h": 14_400_000}

async def _fetch_historical_window(session, symbol: str, interval: str,
                                    start_ms: int, end_ms: int) -> list[dict]:
    """
    Fetches CLOSED candles of any interval for [start_ms, end_ms), paginating
    past Binance's ~1000-candles-per-call cap. Generalizes the original
    1m-only _fetch_1m_window() (see _fetch_1m_window() below, kept as a thin
    wrapper for the existing ATR/outcome-consistency work) so the Historical
    Strategy Replay engine can pre-fetch 1m/3m/5m/15m/1h/4h alike — every
    timeframe any active strategy needs — through ONE shared paginated
    fetcher.

    Uses startTime/endTime directly against Binance's /klines endpoint (via
    the shared binance_request() rate-limit gate) since get_candles() only
    supports "most recent N candles ending now", not a historical window.
    """
    step_ms = INTERVAL_MS.get(interval, 60_000)
    out: list[dict] = []
    cursor = start_ms
    now_ms = int(time.time() * 1000)
    end_ms = min(end_ms, now_ms)
    safety_iters = 0
    max_iters = 60  # generous cap; 60 x 1000 candles covers any realistic replay window per interval
    while cursor < end_ms and safety_iters < max_iters:
        safety_iters += 1
        params = {"symbol": symbol, "interval": interval, "startTime": cursor,
                  "endTime": end_ms, "limit": 1000}
        try:
            raw = await binance_request(session, f"{BINANCE_BASE}/klines", params=params, timeout=15)
        except Exception as e:
            log.debug(f"Replay window fetch error {symbol} {interval}: {e}")
            break
        if not raw:
            break
        batch = []
        for k in raw:
            close_time_ms = k[6] if len(k) > 6 else None
            batch.append({
                "open_time": k[0], "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
                "close_time": close_time_ms,
            })
        # Drop the currently-forming candle if this page reached "now".
        if batch and (batch[-1]["close_time"] is None or batch[-1]["close_time"] > now_ms):
            batch = batch[:-1]
        if not batch:
            break
        out.extend(batch)
        last_open = batch[-1]["open_time"]
        if len(raw) < 1000 or last_open + step_ms >= end_ms:
            break
        cursor = last_open + step_ms
    return out

async def _fetch_1m_window(session, symbol: str, start_ms: int, end_ms: int) -> list[dict]:
    """Thin wrapper over _fetch_historical_window for 1m — kept for the
    existing ATR/backtest-consistency call sites that only need 1m."""
    return await _fetch_historical_window(session, symbol, "1m", start_ms, end_ms)


# ═══════════════════════════════════════════════════════════════════════════
# HISTORICAL STRATEGY REPLAY — deterministic, "as of" universe selection
# ═══════════════════════════════════════════════════════════════════════════
# REPRODUCIBILITY FIX (root cause A): get_top_symbols()/get_symbol_spread_
# quality()/calc_symbol_quality_score() are all correct for LIVE scanning,
# but they read live-only Binance endpoints (/ticker/24hr current 24h
# volume, /ticker/bookTicker current bid/ask, and get_candles()'s live path
# for the most recent 100 candles) — none of which are anchored to a
# historical timestamp. Calling them from the Replay entrypoint meant the
# replay universe reflected "right now" regardless of which historical
# period was being simulated, and reflected a DIFFERENT "right now" every
# time the request was repeated. The functions below are historical
# equivalents: every ranking input is fetched via _fetch_historical_window()
# for a window ending at `asof_ms`, so two calls with the same `asof_ms`
# produce byte-identical rankings. Strategy logic, thresholds, and scoring
# formulas are completely untouched — this only changes what universe is
# fed into that unchanged pipeline.

async def _get_all_usdt_perp_symbols(session: aiohttp.ClientSession) -> list[str]:
    """Returns every currently-tradable USDT-margined perpetual futures
    symbol. This is market metadata (which symbols exist), not price/volume
    data — Binance does not expose a historical "symbol existed as of date
    X" endpoint, so this is the one input to universe selection that is
    necessarily evaluated at request time. In practice this only matters for
    symbols listed/delisted very recently relative to the replay period;
    such symbols are naturally filtered out downstream anyway, since a
    symbol with no historical candle data for the requested window scores 0
    and is excluded (see calc_symbol_quality_score_asof / get_top_symbols_asof
    below). Sorted alphabetically so the CANDIDATE POOL itself never depends
    on whatever order Binance's API happens to return today.
    """
    STABLE = {"USDC", "BUSD", "TUSD", "DAI", "FDUSD", "USDP", "USDD", "USDT"}
    data = await binance_request(session, f"{BINANCE_BASE}/exchangeInfo", timeout=15)
    if not isinstance(data, dict) or "symbols" not in data:
        return []
    out = []
    for s in data["symbols"]:
        sym = s.get("symbol", "")
        if (s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL"
                and s.get("quoteAsset") == "USDT"
                and not any(sym.startswith(st) for st in STABLE if st != "USDT")):
            out.append(sym)
    return sorted(out)


async def get_historical_volume_score(session: aiohttp.ClientSession, symbol: str,
                                       asof_ms: int, window_hours: int = 24) -> float:
    """Deterministic replacement for live 24h quoteVolume ranking: sums
    volume*close over the `window_hours` of 1h candles ending at `asof_ms`,
    fetched via the same historical-window fetcher the rest of Replay uses
    (fixed startTime/endTime — no wall-clock dependency). A symbol with no
    candles in this window (didn't exist yet, or delisted) scores 0 and is
    naturally excluded from the top-N cut."""
    start_ms = asof_ms - window_hours * 3_600_000
    candles = await _fetch_historical_window(session, symbol, "1h", start_ms, asof_ms)
    if not candles:
        return 0.0
    return sum((c["volume"] or 0.0) * (c["close"] or 0.0) for c in candles)


def calc_historical_liquidity_proxy(candles: list[dict]) -> float:
    """Deterministic replacement for get_symbol_spread_quality()'s live
    bid/ask spread — true historical order-book spread isn't retrievable
    from klines, so this uses the average intrabar range (high-low)/close
    over the given historical candle set as a liquidity/noise proxy: tighter
    average ranges are the same intuition live spread-quality scoring used
    (a tight, liquid market prints tight bars), just computed entirely from
    data that already existed at the simulated timestamp. Same 0-1 output
    range and same normalization curve as get_symbol_spread_quality() (0%
    avg range -> 1.0, 0.5%+ -> 0.0) so it composes into calc_symbol_quality_
    score_asof()'s existing weights unchanged."""
    if len(candles) < 10:
        return 0.3
    ratios = [(c["high"] - c["low"]) / c["close"] for c in candles if c.get("close")]
    if not ratios:
        return 0.3
    avg_range_pct = (sum(ratios) / len(ratios)) * 100
    return max(0.0, min(1.0, 1.0 - (avg_range_pct / 0.5)))


async def get_top_symbols_asof(session: aiohttp.ClientSession, n: int, asof_ms: int) -> list[str]:
    """Historical equivalent of get_top_symbols(): ranks the full eligible
    symbol set by historical volume ending at `asof_ms` (get_historical_
    volume_score) instead of live 24h quoteVolume, and breaks ties on symbol
    name so the ordering is fully deterministic even for equal scores (e.g.
    two all-zero symbols)."""
    candidates = await _get_all_usdt_perp_symbols(session)
    semaphore = asyncio.Semaphore(PARALLEL_WORKERS)

    async def scored(symbol):
        async with semaphore:
            v = await get_historical_volume_score(session, symbol, asof_ms)
            return symbol, v

    results = await asyncio.gather(*[scored(s) for s in candidates], return_exceptions=True)
    valid = [r for r in results if isinstance(r, tuple)]
    valid.sort(key=lambda r: (-r[1], r[0]))
    return [sym for sym, v in valid[:n] if v > 0]


async def calc_symbol_quality_score_asof(session: aiohttp.ClientSession, symbol: str,
                                          asof_ms: int) -> dict:
    """Historical equivalent of calc_symbol_quality_score(): identical
    weighted-composite formula (spread 20% / volatility 25% / structure 30%
    / trend 25%) and identical calc_structure_cleanliness/calc_trend_
    clarity/calc_volatility_quality calls — the ONLY change is that candles
    come from _fetch_historical_window() ending at `asof_ms` instead of
    get_candles()'s live path, and spread quality comes from calc_
    historical_liquidity_proxy() instead of a live order-book call."""
    start_ms = asof_ms - 100 * 3_600_000
    candles = await _fetch_historical_window(session, symbol, "1h", start_ms, asof_ms)
    if len(candles) < 30:
        return {"symbol": symbol, "quality_score": 0, "valid": False}

    spread_q = calc_historical_liquidity_proxy(candles)
    structure_q = calc_structure_cleanliness(candles)
    trend_q = calc_trend_clarity(candles)
    volatility_q = calc_volatility_quality(candles)

    composite = (spread_q * 0.20) + (volatility_q * 0.25) + (structure_q * 0.30) + (trend_q * 0.25)
    score = round(composite * 100)

    return {
        "symbol": symbol, "quality_score": score, "valid": True,
        "spread_quality": round(spread_q, 2), "volatility_quality": round(volatility_q, 2),
        "structure_cleanliness": round(structure_q, 2), "trend_clarity": round(trend_q, 2),
    }


async def get_quality_ranked_symbols_asof(session: aiohttp.ClientSession, final_n: int,
                                           pool_size: int, asof_ms: int) -> list[str]:
    """Historical equivalent of get_quality_ranked_symbols(): same two-stage
    shape (top `pool_size` by volume, then top `final_n` by quality score)
    with the same tie-break-by-symbol-name determinism as get_top_symbols_
    asof(), but every input is anchored to `asof_ms`. Given the same
    `asof_ms`, `pool_size`, and `final_n`, this returns the exact same list
    on every call, since Binance's historical klines for a closed window
    don't change after the fact."""
    candidates = await get_top_symbols_asof(session, pool_size, asof_ms)

    semaphore = asyncio.Semaphore(PARALLEL_WORKERS)
    async def scored(symbol):
        async with semaphore:
            return await calc_symbol_quality_score_asof(session, symbol, asof_ms)

    results = await asyncio.gather(*[scored(s) for s in candidates], return_exceptions=True)
    valid_results = [r for r in results if isinstance(r, dict) and r.get("valid")]
    valid_results.sort(key=lambda r: (-r["quality_score"], r["symbol"]))

    return [r["symbol"] for r in valid_results[:final_n]]


# ═══════════════════════════════════════════════════════════════════════════
# HISTORICAL STRATEGY REPLAY — core engine
# ═══════════════════════════════════════════════════════════════════════════
# Completely separate from Advanced Backtest (_run_backtest_job /
# _load_closed_trades_from_db, both untouched). This engine re-runs the real
# strategy_* functions (via the get_candles() shim above) against pre-fetched
# historical candles to find hypothetical historical signals, then resolves
# their outcomes with the SAME _simulate_outcome_step()/
# _resolve_intracandle_priority() live monitoring uses.

# Per-interval lookback each strategy needs (see strategy_* get_candles calls):
# Stop Hunter: 1h(100)/4h(100). Hammer Fib 4H/1H: pattern(30) — covered by the
# 100-candle Stop Hunter lookback on the same interval. HB: 1h(100)/4h(60) —
# also covered. Trigger Fib: 1h(60)/15m(80)/5m(80). Exhaustion: 1h(120)/
# 4h(80)/15m(60). RSI Divergence: 15m(100). Entry timeframes (1m/3m/5m) need
# up to 80 (Hammer Fib/Trigger Fib's own limits).
REPLAY_LOOKBACK_CANDLES = {"4h": 100, "1h": 120, "15m": 100, "5m": 80, "3m": 80, "1m": 80}

# Cadence: a strategy can only produce a NEW result when its own finest
# (entry/trigger) timeframe closes a new candle — re-checking it more often
# just re-evaluates the same closed candles. Verified against each
# strategy's own get_candles() calls (see plan message for the audit).
STRATEGY_FINEST_TF = {
    "Stop Hunter": "1m", "Hammer Fib (4H)": "1m", "Hammer Fib (1H)": "1m",
    "Trigger Fibonacci": "1m", "HB": "15m", "Exhaustion": "15m", "RSI Divergence": "15m",
}

REPLAY_TRADE_EXPIRY_HOURS = 24  # same expiry live monitoring uses

def _generate_backtest_id() -> str:
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM replay_backtests WHERE backtest_id LIKE ?", (f"BT-{day}-%",)
        ).fetchone()
        seq = (row[0] if row else 0) + 1
    finally:
        conn.close()
    return f"BT-{day}-{seq:03d}"


async def _prefetch_replay_cache(session, symbol: str, period_start_ms: int,
                                  period_end_ms: int, backtest_id: str = None) -> HistoricalCandleCache:
    """
    Fetches every timeframe every active strategy needs for `symbol`, ONCE
    each, covering [period_start - own lookback, period_end], with 1m
    additionally extended `REPLAY_TRADE_EXPIRY_HOURS` past period_end so a
    signal detected near the end of the window still has real candles to
    resolve its outcome against (mirrors live's 24h expiry).
    """
    cache = HistoricalCandleCache()
    for interval, lookback_n in REPLAY_LOOKBACK_CANDLES.items():
        step_ms = INTERVAL_MS[interval]
        fetch_start = period_start_ms - lookback_n * step_ms
        fetch_end = period_end_ms
        if interval == "1m":
            fetch_end += REPLAY_TRADE_EXPIRY_HOURS * 3_600_000
        candles = await _fetch_historical_window(session, symbol, interval, fetch_start, fetch_end)
        cache.load(symbol, interval, candles)
        if DIAG_ENABLED and backtest_id:
            _diag_record_candle_hash(backtest_id, symbol, interval, candles)
    return cache


def _diag_record_candle_hash(backtest_id: str, symbol: str, interval: str, candles: list[dict]) -> None:
    """REPRODUCIBILITY DIAGNOSTIC (item 5): hashes the exact OHLCV series a
    symbol/timeframe used for this job. Comparing this hash between two runs
    isolates whether Binance returned different historical candles (it
    shouldn't, for a closed window) from every downstream stage."""
    try:
        payload = json.dumps(
            [[c.get("open_time"), c.get("open"), c.get("high"), c.get("low"), c.get("close"), c.get("volume")]
             for c in candles],
            sort_keys=True,
        )
        digest = hashlib.sha256(payload.encode()).hexdigest()
        first_ot = candles[0].get("open_time") if candles else None
        last_ot = candles[-1].get("open_time") if candles else None
        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO replay_diag_candles "
                "(backtest_id, symbol, timeframe, candle_count, first_open_ms, last_open_ms, sha256_hash) "
                "VALUES (?,?,?,?,?,?,?)",
                (backtest_id, symbol, interval, len(candles), first_ot, last_ot, digest),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.debug(f"diag candle hash failed {symbol}/{interval}: {e}")


def _record_replay_signal(backtest_id: str, symbol: str, result: dict, signal_time_iso: str) -> int:
    """Inserts a hypothetical signal immediately (PENDING) — fault-tolerant:
    even if the process crashes before outcome resolution finishes, this row
    already exists in the DB."""
    conn = get_db_connection()
    try:
        cur = conn.execute("""
            INSERT INTO replay_trades
                (backtest_id, symbol, strategy, direction, timeframe, score, market_regime,
                 signal_time, entry, stop, tp1, tp2, result, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',?)
        """, (
            backtest_id, symbol, result.get("strategy"), result.get("direction"),
            result.get("timeframe"), result.get("score"), result.get("market_regime"),
            signal_time_iso, result["entry"], result["stop"], result["tp1"], result["tp2"],
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()
        trade_id = cur.lastrowid
        visual = result.get("visual")
        if visual:
            try:
                conn.execute(
                    "UPDATE replay_trades SET visual_json=? WHERE id=?",
                    (json.dumps(visual), trade_id),
                )
                conn.commit()
            except Exception:
                pass
        return trade_id
    finally:
        conn.close()


# ─── Historical Replay — fixed reward model (real, outcome-based R) ────────
# Task 1 fix: the R written to replay_trades used to be re-derived from raw
# price distance (pnl_percent / risk_pct) via _simulate_outcome_step(), the
# same helper live trade monitoring uses. That happens to float around
# whatever calc_targets()'s TP1/TP2 multipliers are (currently 1.5R / 3.0R)
# rather than the actual partial-take-profit money-management model this
# bot's signals are meant to represent — TP1 is a partial exit that locks in
# a small, fixed reward; TP2 is the full remaining-position target; SL before
# any partial exit is a full, fixed loss of the original risk. So instead of
# a price-derived approximation, the R for a resolved historical trade is
# looked up from the ACTUAL terminal outcome that trade reached — the
# specific candle path decides WHICH of these three happened, but the value
# credited for that outcome is this fixed, deliberate table (not the
# fluctuating raw TP1/TP2 target distance):
REPLAY_R_TP1_ONLY = 0.25   # TP1 hit → partial profit booked, then closed (breakeven/no TP2)
REPLAY_R_TP2      = 2.25   # TP2 hit → full target reached
REPLAY_R_SL       = -1.0   # SL hit before TP1 → full initial risk lost
REPLAY_R_BY_STATUS = {
    "TP1_TOUCHSL": REPLAY_R_TP1_ONLY,
    "TP2_HIT":     REPLAY_R_TP2,
    "SL_HIT":      REPLAY_R_SL,
}


def _resolve_replay_outcome(cache: HistoricalCandleCache, symbol: str, trade_id: int,
                             entry: float, stop: float, tp1: float, tp2: float,
                             direction: str, signal_time_ms: int) -> None:
    """
    Replays the pre-fetched 1m series forward from `signal_time_ms`, through
    the SAME _simulate_outcome_step()/_resolve_intracandle_priority() live
    monitoring uses to determine WHICH outcome a trade actually reached
    (TP1-only / TP2 / SL / missed / still open), and writes the final
    outcome to replay_trades. Persisted immediately (fault-tolerant) — no
    batching until job end.

    Task 1: the PnL%/R actually stored is NOT the raw price-distance value
    _simulate_outcome_step() computed (that's only used internally, to
    detect intracandle event ordering/priority) — it's overridden below,
    from REPLAY_R_BY_STATUS, based on the real terminal outcome the
    simulation landed on. See REPLAY_R_BY_STATUS docstring above.
    """
    m1 = cache.series.get((symbol, "1m"), [])
    risk_pct = abs(entry - stop) / entry * 100 if entry else 0
    local = {
        "status": "PENDING", "tp1_hit": 0, "tp2_hit": 0,
        "entered_at": None, "tp1_hit_at": None, "tp2_hit_at": None,
        "closed_at": None, "close_price": None, "pnl_percent": None, "rr_multiple": None,
    }
    expiry_ms = signal_time_ms + REPLAY_TRADE_EXPIRY_HOURS * 3_600_000
    TERMINAL = {"MISSED", "SL_HIT", "TP1_TOUCHSL", "TP2_HIT"}
    last_candle_iso = None
    for c in m1:
        if c["close_time"] is None or c["close_time"] <= signal_time_ms:
            continue
        if c["close_time"] > expiry_ms:
            break
        candle_iso = _ms_to_iso(c["close_time"])
        last_candle_iso = candle_iso
        for _ in range(4):  # allow multiple transitions within one candle, same as live
            if local["status"] in TERMINAL:
                break
            ev = _simulate_outcome_step(local, c, candle_iso, entry, stop, tp1, tp2, direction, risk_pct)
            if ev is None:
                break
        if local["status"] in TERMINAL:
            break

    # ── Task 1: override with the fixed, outcome-based reward model ──
    # local["status"] is the real terminal state the candle-by-candle
    # simulation actually reached — only the R/PnL VALUE credited for it is
    # replaced, never which outcome occurred.
    if local["status"] in REPLAY_R_BY_STATUS:
        fixed_r = REPLAY_R_BY_STATUS[local["status"]]
        local["rr_multiple"] = fixed_r
        local["pnl_percent"] = round(fixed_r * risk_pct, 3)

    result_map = {"MISSED": "MISSED", "SL_HIT": "SL", "TP1_TOUCHSL": "TP1_TOUCHSL", "TP2_HIT": "TP2"}
    if local["status"] in result_map:
        final_result = result_map[local["status"]]
    elif local["status"] == "ENTERED" and local["tp1_hit"]:
        final_result = "OPEN"  # TP1 hit but window/expiry ended before final resolution
    elif local["status"] in ("PENDING", "OPEN"):
        final_result = "OPEN"
    else:
        final_result = "OPEN"

    conn = get_db_connection()
    try:
        conn.execute("""
            UPDATE replay_trades SET
                entry_time=?, exit_time=?, exit_price=?, result=?, pnl_percent=?, rr_multiple=?
            WHERE id=?
        """, (
            local.get("entered_at"), local.get("closed_at"), local.get("close_price"),
            final_result, local.get("pnl_percent"), local.get("rr_multiple"), trade_id,
        ))
        conn.commit()
    finally:
        conn.close()


async def _replay_symbol(session, symbol: str, period_start_ms: int, period_end_ms: int,
                          state: dict, backtest_id: str, replay_weights: dict,
                          replay_cfg: dict) -> tuple[int, int]:
    """
    Walk-forward replay for ONE symbol: pre-fetch every needed timeframe
    once, then step through simulated time re-running each strategy only
    when its own finest timeframe closes a new candle (see
    STRATEGY_FINEST_TF), recording + resolving any hypothetical signals
    found. Returns (signals_found, trades_resolved).

    FIX (requirement 2 audit — signal-count explosion): this previously
    recorded ANY non-None strategy result directly as a signal. That
    completely bypassed everything scan_symbol() applies live before a
    result becomes an actual signal: regime-type gating, duplicate/cooldown
    dedup, run_live_filters() (ADX/ATR/volume/session/EMA/3TF-alignment/
    OI/funding/BTC-trend/news/TP2-probability), the entry-volume
    score bonus, adaptive strategy weighting, and the final min-score gate.
    A raw strategy trigger is a CANDIDATE, not a signal — same as live. This
    now mirrors scan_symbol()'s pipeline step for step, using the same
    functions (run_live_filters, get_market_regime, get_strategy_weight,
    calc_tp2_potential_score, get_min_signal_score), just fed by the
    get_candles()/get_current_session() replay shims instead of live data.

    Dedup/cooldown uses a LOCAL dict scoped to this one symbol's replay run
    (keyed by simulated time), NOT the live global `_last_signal`/open_keys
    — reusing those would (a) use real wall-clock time instead of simulated
    time, and (b) contaminate live scanning's actual cooldown state if a
    replay job runs concurrently with it.

    REPRODUCIBILITY FIX (root cause B): `replay_weights` is a frozen
    snapshot of get_all_strategy_weights() taken ONCE at the start of the
    whole replay job (see _run_replay_backtest_job) and threaded down here
    unchanged. Previously this function called the live get_strategy_weight()
    per-candidate, which reads a 30s-TTL cache backed by the `strategy_weights`
    DB table — a table that update_all_strategy_weights() overwrites on a
    weekly background schedule (and on-demand via an admin action). That
    meant the exact same historical candidate could pass or fail the final
    min-score gate below purely depending on which wall-clock moment the
    replay job happened to run in, with no change to strategy logic,
    thresholds, or historical data. Freezing the weight per job removes that
    variable without changing the weighting formula itself.

    REPRODUCIBILITY FIX (root cause D): `state` here is now a frozen
    deep-copy taken once at job start (see _run_replay_backtest_job), not
    the live shared bot-state dict — so per-strategy settings
    (atr_multiplier, atr_stop_multiplier, adx_threshold, session_filters,
    hb_* thresholds, min_signal_score, etc., all read via
    get_strategy_settings(state, ...)) can no longer drift mid-job or
    between two "identical" replay requests because of a live admin action
    (/config, /setscore) or an auto-tuning feature (e.g. the HB
    spike-dominance auto-adjust) writing to the SAME live state object the
    running bot's live scanner also reads. `replay_cfg` is the equivalent
    frozen snapshot of load_bot_config() for the two getters
    (get_strategy_min_score's fallback, get_dynamic_score_threshold) that
    read bot_config.json directly rather than through `state`.
    """
    cache = await _prefetch_replay_cache(session, symbol, period_start_ms, period_end_ms, backtest_id)
    m1 = cache.series.get((symbol, "1m"), [])
    if not m1:
        return 0, 0

    signals_found = 0
    trades_resolved = 0
    disabled = state.get("disabled_strategies", [])
    _last_signal_sim: dict[str, int] = {}   # f"{strategy}_{direction}" -> sim_time_ms of last accepted signal
    _open_position: set[str] = set()        # f"{strategy}" -> currently has an unresolved replay trade for this symbol
    _regime_cache: dict[int, dict] = {}     # sim minute-bucket -> regime dict, avoid recomputing every single 1m tick

    token = _replay_ctx.set(cache)
    try:
        for c in m1:
            ct = c["close_time"]
            if ct is None or ct < period_start_ms:
                continue
            if ct > period_end_ms:
                break
            cache.sim_time_ms = ct
            # BUG FIX (caught by testing before shipping): Binance close_time
            # = open_time + duration - 1 (ends in ...999), so it is NEVER a
            # clean multiple of the interval — `ct % 15m == 0` is always
            # False for real data, which would have silently skipped HB/
            # Exhaustion/RSI Divergence for the ENTIRE replay run. The next
            # candle's open_time (= close_time + 1) IS interval-aligned.
            is_15m_close = ((ct + 1) % INTERVAL_MS["15m"]) == 0

            # Regime is only re-derived once per 15m bucket (matches how
            # infrequently it can actually change) rather than every 1m
            # tick, for the same reason strategy cadence is bucketed —
            # avoids thousands of redundant recomputations against the same
            # already-closed higher-timeframe candles.
            regime_bucket = ct // INTERVAL_MS["15m"]
            if state.get("regime_engine_enabled", True):
                if regime_bucket not in _regime_cache:
                    try:
                        _regime_cache[regime_bucket] = await get_market_regime(session, symbol)
                    except Exception:
                        _regime_cache[regime_bucket] = {"allowed_types": {"TREND", "REVERSAL", "HYBRID"}, "regime_label": None}
                regime = _regime_cache[regime_bucket]
                allowed_types = regime["allowed_types"]
            else:
                allowed_types = {"TREND", "REVERSAL", "HYBRID"}
                regime = None

            _tp2_candles_cache = None

            for strat_name, strat_fn in STRATEGIES:
                if strat_name in disabled:
                    continue
                cadence = STRATEGY_FINEST_TF.get(strat_name, "1m")
                if cadence == "15m" and not is_15m_close:
                    continue

                strat_type = STRATEGY_REGIME_TYPE.get(strat_name, "HYBRID")
                if strat_type not in allowed_types:
                    continue

                try:
                    result = await strat_fn(session, symbol, state)
                except Exception as e:
                    log.debug(f"Replay strategy error {strat_name} {symbol}: {e}")
                    continue
                if not result:
                    continue

                direction = result.get("direction", "")
                raw_score = result.get("score", 0)  # diagnostic item 6: BEFORE any bonus/weight/filter
                weight = replay_weights.get(strat_name, 1.0)

                def _diag(passed_filters, passed_gate, reason, final_score=None, min_required=None):
                    """REPRODUCIBILITY DIAGNOSTIC (items 6-8): one row per
                    candidate, whether accepted or rejected, with the exact
                    reason. Comparing this table between two runs finds the
                    first strategy/symbol/timestamp where a candidate's fate
                    diverges, and whether it diverged at the filter stage or
                    the score stage."""
                    if not DIAG_ENABLED:
                        return
                    try:
                        conn = get_db_connection()
                        try:
                            conn.execute(
                                "INSERT INTO replay_diag_candidates (backtest_id, symbol, strategy, "
                                "sim_time_ms, direction, regime_label, raw_score, weight_applied, "
                                "final_score, min_required, passed_filters, passed_score_gate, "
                                "rejection_reason, entry, stop, tp1, tp2) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (backtest_id, symbol, strat_name, ct, direction,
                                 result.get("market_regime"), raw_score, weight, final_score, min_required,
                                 int(bool(passed_filters)) if passed_filters is not None else None,
                                 int(bool(passed_gate)) if passed_gate is not None else None,
                                 reason, result.get("entry"), result.get("stop"),
                                 result.get("tp1"), result.get("tp2")),
                            )
                            conn.commit()
                        finally:
                            conn.close()
                    except Exception as e:
                        log.debug(f"diag candidate record failed {symbol}/{strat_name}: {e}")

                # ── Duplicate/cooldown guard (local, simulated-time — see
                # docstring) — same intent as live can_signal(). ──
                dedup_key = f"{strat_name}_{direction}"
                last_ms = _last_signal_sim.get(dedup_key)
                if last_ms is not None and (ct - last_ms) < SIGNAL_COOLDOWN_WINDOW * 1000:
                    _diag(None, None, "cooldown")
                    continue
                if strat_name in _open_position:
                    _diag(None, None, "open_position")
                    continue

                # ── TP2 potential score (same calc, same candles live uses) ──
                try:
                    if _tp2_candles_cache is None:
                        c1h_tp2 = await get_candles(session, symbol, "1h", 100)
                        c4h_tp2 = await get_candles(session, symbol, "4h", 80)
                        _tp2_candles_cache = (c1h_tp2, c4h_tp2)
                    else:
                        c1h_tp2, c4h_tp2 = _tp2_candles_cache
                    result["tp2_potential"] = calc_tp2_potential_score(c1h_tp2, c4h_tp2, direction)
                except Exception:
                    result["tp2_potential"] = 50

                if regime:
                    result["market_regime"] = regime.get("regime_label")

                # ── Layer A: full live filter chain — must all pass ──
                try:
                    passed, block_reasons = await run_live_filters(session, symbol, result, state, replay_cfg)
                except Exception as e:
                    log.debug(f"Replay run_live_filters error {strat_name} {symbol}: {e}")
                    _diag(None, None, f"filter_exception:{e}")
                    continue
                if not passed:
                    _diag(False, None, ",".join(block_reasons) if block_reasons else "filtered")
                    continue

                # ── Entry volume bonus (same calc as live) ──
                try:
                    _strat_cfg = get_strategy_settings(state, strat_name)
                    ma_period = _strat_cfg.get("entry_volume_ma_period", 20)
                    vol_multiplier = _strat_cfg.get("entry_volume_multiplier", 1.5)
                    c5m_vol = await get_candles(session, symbol, "5m", ma_period + 5)
                    vol_ratio = entry_volume_ratio(c5m_vol, ma_period)
                    bonus = min(10, int((vol_ratio - vol_multiplier) * 4)) if vol_ratio > vol_multiplier else 0
                    result["score"] = min(100, result.get("score", 0) + max(0, bonus))
                except Exception:
                    pass

                # ── Adaptive strategy weight (frozen per-job snapshot — see
                #    docstring above; same weighting formula as live, just
                #    not re-read from live, time-varying DB state mid-job) ──
                try:
                    if weight != 1.0:
                        result["score"] = max(1, min(100, round(result.get("score", 0) * weight)))
                except Exception:
                    pass

                # ── Final min-score gate (same as live, post-adjustments) ──
                # Uses the SAME per-strategy score as live scanning (Task 3):
                # get_strategy_min_score() reads strat_name's own
                # min_signal_score if set via /setscore, else the legacy
                # global fallback — so a Historical Backtest run judges each
                # strategy's signals against exactly the threshold that
                # strategy is actually configured with right now.
                _final_score = result.get("score", 0)
                _min_required = get_strategy_min_score(state, strat_name, replay_cfg)
                if not (math.ceil(_final_score) >= _min_required):
                    _diag(True, False, "below_min_score", _final_score, _min_required)
                    continue

                _diag(True, True, None, _final_score, _min_required)
                signal_time_iso = _ms_to_iso(ct)
                trade_id = _record_replay_signal(backtest_id, symbol, result, signal_time_iso)
                signals_found += 1
                _last_signal_sim[dedup_key] = ct
                _open_position.add(strat_name)
                _resolve_replay_outcome(cache, symbol, trade_id, result["entry"], result["stop"],
                                         result["tp1"], result["tp2"], result["direction"], ct)
                _open_position.discard(strat_name)
                trades_resolved += 1
    finally:
        _replay_ctx.reset(token)

    return signals_found, trades_resolved


REPLAY_PARALLEL_SYMBOLS = 6      # bounded concurrency across symbols
REPLAY_PROGRESS_INTERVAL_S = 25  # how often the progress message is edited

def _replay_build_summary(backtest_id: str) -> dict:
    """Aggregates replay_trades for `backtest_id` into overall + per-strategy stats."""
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT * FROM replay_trades WHERE backtest_id=?", (backtest_id,)).fetchall()
    finally:
        conn.close()

    def stats_for(rows_subset):
        total = len(rows_subset)
        tp1 = sum(1 for r in rows_subset if r["result"] == "TP1_TOUCHSL")
        tp2 = sum(1 for r in rows_subset if r["result"] == "TP2")
        sl  = sum(1 for r in rows_subset if r["result"] == "SL")
        missed = sum(1 for r in rows_subset if r["result"] == "MISSED")
        open_ = sum(1 for r in rows_subset if r["result"] == "OPEN")
        wins = tp1 + tp2
        losses = sl
        decided = wins + losses
        win_rate = round(wins / decided * 100, 1) if decided > 0 else 0.0
        total_pnl = round(sum(r["pnl_percent"] or 0 for r in rows_subset), 3)
        total_r = round(sum(r["rr_multiple"] or 0 for r in rows_subset), 3)
        symbols = sorted({r["symbol"] for r in rows_subset})
        return {"total": total, "tp1": tp1, "tp2": tp2, "sl": sl, "missed": missed,
                "open": open_, "win_rate": win_rate, "total_pnl": total_pnl,
                "total_r": total_r, "symbols": symbols}

    overall = stats_for(rows)
    by_strategy = {}
    for strat_name, _ in STRATEGIES:
        subset = [r for r in rows if r["strategy"] == strat_name]
        if subset:
            by_strategy[strat_name] = stats_for(subset)
    return {"overall": overall, "by_strategy": by_strategy}


def _replay_format_report(backtest_id: str, period_label: str, scope_label: str, summary: dict) -> str:
    o = summary["overall"]
    lines = [
        f"📊 <b>Historical Strategy Replay — <code>{backtest_id}</code></b>",
        f"⏱ Period: {period_label}   📦 Scope: {scope_label}",
        "",
        f"Total Signals: {o['total']}",
        f"TP1: {o['tp1']} | TP2: {o['tp2']} | SL: {o['sl']} | MISSED: {o['missed']} | OPEN: {o['open']}",
        f"🎯 Win Rate: {o['win_rate']}%",
        f"💵 Total PnL: {'+' if o['total_pnl'] >= 0 else ''}{o['total_pnl']}%",
        f"📐 Total R: {'+' if o['total_r'] >= 0 else ''}{o['total_r']}R",
        "",
        "── By Strategy ──",
    ]
    for strat_name, s in summary["by_strategy"].items():
        lines.append(
            f"\n<b>{strat_name}</b>\n"
            f"Total: {s['total']} | TP1: {s['tp1']} | TP2: {s['tp2']} | SL: {s['sl']} | "
            f"MISSED: {s['missed']} | OPEN: {s['open']}\n"
            f"Win Rate: {s['win_rate']}% | PnL: {'+' if s['total_pnl']>=0 else ''}{s['total_pnl']}% | "
            f"Total R: {'+' if s['total_r']>=0 else ''}{s['total_r']}R\n"
            f"Coins: {', '.join(s['symbols']) if s['symbols'] else '—'}"
        )
    return "\n".join(lines)


async def _run_replay_backtest_job(session: aiohttp.ClientSession, backtest_id: str,
                                    symbols: list[str], period_start_ms: int, period_end_ms: int,
                                    period_label: str, scope_label: str, state: dict,
                                    chat_id: str, progress_message_id: Optional[int],
                                    replay_weights: dict, replay_cfg: dict) -> None:
    """
    Orchestrates the full Replay Backtest run in the background: bounded
    concurrency across symbols, periodic progress edits, fault-tolerant
    incremental persistence (each symbol's trades are already committed to
    replay_trades by the time this function's own bookkeeping updates run —
    see _record_replay_signal/_resolve_replay_outcome), and the final report
    sent automatically on completion.

    If the process crashes mid-run, replay_backtests stays at status=RUNNING
    with whatever symbols_done/trades already got committed — see
    _recover_interrupted_replay_jobs() (called at startup) which marks any
    such stale row PARTIAL/INTERRUPTED instead of leaving it silently wrong.

    REPRODUCIBILITY FIX (root cause B/D): `state`, `replay_weights`, and
    `replay_cfg` are now frozen snapshots taken ONCE by the caller (the
    replay_range_ handler), BEFORE this job is even queued — not recomputed
    in here. This guarantees the exact values persisted to
    replay_backtests.{settings_snapshot_json,weights_snapshot_json,
    config_snapshot_json} for diagnostics are byte-identical to what this
    job actually evaluates every candidate against; there is no second read
    that could disagree with the first. See _replay_symbol()'s docstring for
    why these needed freezing at all (get_strategy_weight()/
    get_strategy_settings()/load_bot_config() are all live, TTL-cached, or
    disk-backed values that a concurrent admin action or auto-tuner can
    change mid-job or between two "identical" replay requests).
    """
    started = time.time()
    total = len(symbols)
    counters = {"done": 0, "signals": 0, "trades": 0}
    counters_lock = asyncio.Lock()
    sem = asyncio.Semaphore(REPLAY_PARALLEL_SYMBOLS)
    stop_progress = asyncio.Event()

    def _persist_progress():
        conn = get_db_connection()
        try:
            conn.execute(
                "UPDATE replay_backtests SET symbols_done=?, signals_found=?, trades_resolved=?, "
                "total_symbols=?, updated_at=? WHERE backtest_id=?",
                (counters["done"], counters["signals"], counters["trades"], total,
                 datetime.now(timezone.utc).isoformat(), backtest_id),
            )
            conn.commit()
        finally:
            conn.close()

    _persist_progress()

    async def worker(symbol: str):
        async with sem:
            try:
                sig, tr = await _replay_symbol(session, symbol, period_start_ms, period_end_ms,
                                                state, backtest_id, replay_weights, replay_cfg)
            except Exception as e:
                log.warning(f"Replay Backtest: symbol {symbol} failed: {e}")
                sig, tr = 0, 0
            async with counters_lock:
                counters["done"] += 1
                counters["signals"] += sig
                counters["trades"] += tr
                _persist_progress()

    async def progress_updater():
        while not stop_progress.is_set():
            try:
                await asyncio.wait_for(stop_progress.wait(), timeout=REPLAY_PROGRESS_INTERVAL_S)
            except asyncio.TimeoutError:
                pass
            if stop_progress.is_set():
                break
            elapsed = int(time.time() - started)
            mm, ss = divmod(elapsed, 60)
            text = (
                f"🔄 <b>Replay Backtest — <code>{backtest_id}</code></b>\n"
                f"Progress: {counters['done']}/{total} symbols\n"
                f"Signals: {counters['signals']}\n"
                f"Trades resolved: {counters['trades']}\n"
                f"Elapsed: {mm:02d}:{ss:02d}\n"
                f"Status: Running..."
            )
            if progress_message_id:
                try:
                    await edit_msg(session, chat_id, progress_message_id, text)
                except Exception:
                    pass

    updater_task = asyncio.create_task(progress_updater())
    try:
        tasks = [asyncio.create_task(worker(s)) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        crashed = sum(1 for r in results if isinstance(r, Exception))
        final_status = "DONE" if crashed == 0 else "PARTIAL"
    except Exception as e:
        log.error(f"Replay Backtest job {backtest_id} failed: {e}")
        final_status = "FAILED"
    finally:
        stop_progress.set()
        try:
            await updater_task
        except Exception:
            pass

    summary = _replay_build_summary(backtest_id)
    conn = get_db_connection()
    try:
        conn.execute(
            "UPDATE replay_backtests SET status=?, summary_json=?, completed_at=?, updated_at=? "
            "WHERE backtest_id=?",
            (final_status, json.dumps(summary), datetime.now(timezone.utc).isoformat(),
             datetime.now(timezone.utc).isoformat(), backtest_id),
        )
        conn.commit()
    finally:
        conn.close()

    report_text = _replay_format_report(backtest_id, period_label, scope_label, summary)
    if final_status != "DONE":
        report_text += f"\n\n⚠️ Status: {final_status} — some symbols may not have completed."
    try:
        if progress_message_id:
            await edit_msg(session, chat_id, progress_message_id,
                            f"✅ Replay Backtest <code>{backtest_id}</code> finished — sending report...")
        await send_telegram(session, report_text, chat_id,
                             reply_markup=_replay_report_inline_kb(backtest_id))
    except Exception as e:
        log.error(f"Replay Backtest {backtest_id}: failed to send final report: {e}")


def _replay_report_inline_kb(backtest_id: str) -> dict:
    rows = [[{"text": "📂 Browse by Strategy", "callback_data": f"replay_bystrat:{backtest_id}"}],
            [{"text": "🔎 Browse by Symbol", "callback_data": f"replay_bysym:{backtest_id}"}]]
    return {"inline_keyboard": rows}


async def _recover_interrupted_replay_jobs() -> None:
    """
    Called once at startup. Any replay_backtests row still marked RUNNING
    means the process died mid-job (crash, Railway restart, etc.) — mark it
    PARTIAL (if it has at least one saved trade) or INTERRUPTED (if it has
    none), so results are never silently lost or left in a misleading
    "still running" state. All trades already committed by
    _record_replay_signal/_resolve_replay_outcome before the crash remain
    fully intact and inspectable either way.
    """
    conn = get_db_connection()
    try:
        stale = conn.execute("SELECT backtest_id, trades_resolved FROM replay_backtests WHERE status='RUNNING'").fetchall()
        for row in stale:
            new_status = "PARTIAL" if (row["trades_resolved"] or 0) > 0 else "INTERRUPTED"
            summary = _replay_build_summary(row["backtest_id"])
            conn.execute(
                "UPDATE replay_backtests SET status=?, summary_json=?, updated_at=? WHERE backtest_id=?",
                (new_status, json.dumps(summary), datetime.now(timezone.utc).isoformat(), row["backtest_id"]),
            )
            log.warning(f"Replay Backtest {row['backtest_id']} was left RUNNING from a previous "
                        f"process — marked {new_status} (trades already saved remain intact).")
        conn.commit()
    finally:
        conn.close()


def _replay_query_trades(backtest_id: str, strategy: Optional[str] = None,
                          symbol: Optional[str] = None) -> list[sqlite3.Row]:
    """Trade inspection query — used by both the button drill-down and the
    /replay_trades slash command."""
    conn = get_db_connection()
    try:
        q = "SELECT * FROM replay_trades WHERE backtest_id=?"
        params = [backtest_id]
        if strategy:
            q += " AND strategy=?"
            params.append(strategy)
        if symbol:
            q += " AND symbol=?"
            params.append(symbol.upper())
        q += " ORDER BY signal_time"
        return conn.execute(q, params).fetchall()
    finally:
        conn.close()


def _replay_format_trade(r: sqlite3.Row) -> str:
    return (
        f"<b>{r['symbol']}</b>  ·  {r['strategy']}  ·  {r['direction']}\n"
        f"Entry: {r['entry']:.6g}   SL: {r['stop']:.6g}\n"
        f"TP1: {r['tp1']:.6g}   TP2: {r['tp2']:.6g}\n"
        f"Result: {r['result']}   PnL: {(r['pnl_percent'] or 0):+.3f}%   R: {(r['rr_multiple'] or 0):+.2f}R\n"
        f"Signal: {r['signal_time']}   Exit: {r['exit_time'] or '—'}"
    )


async def _send_replay_trade_chart(session: aiohttp.ClientSession, chat_id: str, trade_id: int) -> None:
    """
    Audit/visualization only — reuses the EXISTING chart_renderer/
    drawing_tools/analysis_snapshot pipeline (same one live signals use via
    build_and_save_chart_snapshot()/render_signal_chart()), pointed at a
    Replay Backtest trade instead of a live signal_id. Never recomputes
    entry/stop/tp1/tp2 or any strategy decision — purely renders what was
    already recorded, plus the trade's actual resolved exit for comparison.
    """
    if not CHART_FEATURE_AVAILABLE:
        await send_telegram(session, "Chart rendering isn't available on this deployment.", chat_id)
        return

    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM replay_trades WHERE id=?", (trade_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        await send_telegram(session, "Trade not found.", chat_id)
        return

    try:
        conn2 = get_db_connection()
        try:
            snap_row = conn2.execute(
                "SELECT snapshot_json FROM replay_chart_snapshots WHERE trade_id=?", (trade_id,)
            ).fetchone()
        finally:
            conn2.close()

        if snap_row:
            snapshot = json.loads(snap_row["snapshot_json"])
        else:
            visual = json.loads(row["visual_json"]) if row["visual_json"] else {}
            candles = visual.get("candles")
            if not candles:
                await send_telegram(session,
                    f"No stored candle data for this trade (older run or strategy without visual data).",
                    chat_id)
                return
            visual_for_storage = {k: v for k, v in visual.items() if k != "candles"}
            # Mark the actual exit on the chart, distinct from the theoretical
            # TP1/TP2/SL lines already drawn from entry/stop/tp1/tp2 — audit
            # value is seeing what ACTUALLY happened vs. what was targeted.
            visual_for_storage["replay_result"] = row["result"]
            visual_for_storage["replay_exit_price"] = row["exit_price"]
            visual_for_storage["replay_exit_time"] = row["exit_time"]
            result_for_snapshot = {
                "symbol": row["symbol"], "strategy": f"{row['strategy']} [Replay]",
                "direction": row["direction"], "score": row["score"],
                "entry": row["entry"], "stop": row["stop"], "tp1": row["tp1"], "tp2": row["tp2"],
                "market_regime": row["market_regime"], "visual": visual_for_storage,
            }
            snapshot = _analysis_snapshot.build_snapshot(
                result_for_snapshot, candles,
                entry_timeframe=row["timeframe"] or "", structure_timeframe=None,
                trigger_idx=visual.get("trigger_idx"),
            )
            snapshot_json = json.dumps(snapshot)
            conn3 = get_db_connection()
            try:
                conn3.execute(
                    "INSERT OR REPLACE INTO replay_chart_snapshots (trade_id, snapshot_json, created_at) VALUES (?,?,?)",
                    (trade_id, snapshot_json, datetime.now(timezone.utc).isoformat()),
                )
                conn3.commit()
            finally:
                conn3.close()

        png = render_signal_chart(snapshot)
        if not png:
            await send_telegram(session, "Chart rendering failed for this trade.", chat_id)
            return
        caption = (
            f"{row['symbol']} · {row['strategy']} · {row['direction']}\n"
            f"Result: {row['result']}   PnL: {(row['pnl_percent'] or 0):+.3f}%   "
            f"R: {(row['rr_multiple'] or 0):+.2f}R"
        )
        await send_telegram_photo(session, png, caption, chat_id)
    except Exception as e:
        log.error(f"_send_replay_trade_chart error for trade_id={trade_id}: {e}")
        await send_telegram(session, f"❌ Could not render chart: {e}", chat_id)

def _ms_to_iso(ts) -> str:
    """
    تبدیل timestamp به رشته ISO 8601 UTC.
    - اگر رشته ISO باشد (از دیتابیس واقعی مثل '2024-01-15T10:30:00'): همان را برمی‌گرداند.
    - اگر عدد باشد: از millisecond یا second به ISO تبدیل می‌کند.
    - اگر None باشد: None برمی‌گرداند.
    """
    try:
        if ts is None:
            return None
        # رشته ISO از دیتابیس: فقط 16 کاراکتر اول برای نمایش YY-MM-DDTHH:MM
        if isinstance(ts, str):
            return ts[:16] if len(ts) >= 16 else ts
        # عدد: millisecond یا second
        ts_int = int(ts)
        if ts_int > 32503680000:  # بیشتر از سال 3000 → millisecond
            ts_int = ts_int // 1000
        return datetime.fromtimestamp(ts_int, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M")
    except Exception:
        return str(ts) if ts is not None else None

async def _simulate_trade_outcome(session, symbol: str, result: dict, timerange_hours: int) -> dict:
    """
    شبیه‌سازی نتیجه یک سیگنال روی کندل‌های آینده (نسبت به لحظه سیگنال).
    منطق:
      - کندل‌های 1h را می‌گیریم (از timerange_hours ساعت قبل تا الان).
      - کندل آخر = کندل سیگنال (همین لحظه). از کندل بعد از آن شروع می‌کنیم.
      - برای هر کندل بعدی: ابتدا Entry باید لمس شود، سپس TP/SL چک می‌شود.
      - اگر در بازه timerange_hours نتیجه مشخص نشد → OPEN.
    """
    try:
        # یک بافر اضافی (50 کندل) برای اینکه مطمئن شویم کندل‌های "بعد از سیگنال" داریم
        c = await _fetch_candles_for_backtest(session, symbol, "1h", timerange_hours + 5)
        if not c or len(c) < 2:
            return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                    "entry_ts": None, "exit_ts": None}

        entry     = result["entry"]
        stop      = result["stop"]
        tp1       = result["tp1"]
        tp2       = result["tp2"]
        direction = result["direction"]
        risk_pct  = abs(entry - stop) / entry * 100 if entry else 1

        # FIX v3.4: منطق صحیح future_candles برای بک‌تست
        # بک‌تست می‌گوید: "اگر این سیگنال ~timerange_hours ساعت پیش صادر می‌شد، چه نتیجه‌ای داشت؟"
        # c = کندل‌های 1H از گذشته تا الان (آخرین = کندل جاری)
        # نقطه فرضی سیگنال = اولین کندل (قدیمی‌ترین)
        # future_candles = همه کندل‌های بعد از نقطه فرضی سیگنال = c[1:] (همه به جز اولین)
        # entry تقریباً = close اولین کندل که باید در محدوده کندل‌های بعدی باشد

        if len(c) < 3:
            return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                    "entry_ts": None, "exit_ts": None}

        # کندل‌های "آینده" = همه کندل‌ها به جز اولین (که نماینده لحظه سیگنال است)
        # محدود به timerange_hours کندل تا timeframe بک‌تست رعایت شود
        max_candles = min(len(c) - 1, max(timerange_hours, 1))
        future_candles = c[1: 1 + max_candles]

        if not future_candles:
            return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                    "entry_ts": None, "exit_ts": None}

        entry_candle_ts = None
        entered = False
        tp1_reached = False   # FIX v3.4: track TP1 independently

        for candle in future_candles:
            h, l = candle["high"], candle["low"]
            ts_iso = _ms_to_iso(candle.get("open_time"))

            # ── مرحله ۱: آیا Entry لمس شده؟ ──
            if not entered:
                entry_touched = (direction == "BUY" and l <= entry <= h) or \
                                (direction == "SELL" and l <= entry <= h)
                if entry_touched:
                    entered = True
                    entry_candle_ts = ts_iso
                    # FIX v3.4: چک فوری TP/SL در همان کندل ورود
                    _ht2 = (direction == "BUY" and h >= tp2) or (direction == "SELL" and l <= tp2)
                    _ht1 = (direction == "BUY" and h >= tp1) or (direction == "SELL" and l <= tp1)
                    _hsl = (direction == "BUY" and l <= stop) or (direction == "SELL" and h >= stop)
                    # FIX (backtest audit, high severity): resolve same-candle TP/SL
                    # ambiguity the same way the live engine does (see
                    # _resolve_intracandle_priority docstring) instead of a fixed
                    # TP2>TP1>SL check order, which always favored the best-looking
                    # outcome and inflated backtest win-rate/PnL.
                    _candidates = []
                    if _ht2: _candidates.append((tp2, "TP2"))
                    if _ht1: _candidates.append((tp1, "TP1"))
                    if _hsl: _candidates.append((stop, "SL"))
                    _winner = _resolve_intracandle_priority(candle["open"], _candidates) if _candidates else None
                    if _winner == "TP2":
                        pnl = ((tp2 - entry) / entry * 100) if direction == "BUY" else ((entry - tp2) / entry * 100)
                        rr  = pnl / risk_pct if risk_pct else 0
                        return {**result, "outcome": "TP2", "pnl_percent": round(pnl, 2),
                                "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}
                    elif _winner == "TP1":
                        tp1_reached = True  # TP1 در همان کندل ورود — ادامه دهیم
                    elif _winner == "SL":
                        pnl = ((stop - entry) / entry * 100) if direction == "BUY" else ((entry - stop) / entry * 100)
                        rr  = -abs(pnl / risk_pct) if risk_pct else -1.0
                        return {**result, "outcome": "SL", "pnl_percent": round(-abs(pnl), 2),
                                "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}
                # FIX v3.4: اگر قیمت مستقیم به TP1/TP2/SL رفت بدون لمس Entry → MISSED (No Trade)
                # یعنی معامله اصلاً باز نشد — نه سود، نه ضرر
                elif (direction == "BUY" and (h >= tp1 or h >= tp2 or l <= stop)) or \
                     (direction == "SELL" and (l <= tp1 or l <= tp2 or h >= stop)):
                    return {**result, "outcome": "MISSED", "pnl_percent": None, "rr_multiple": None,
                            "entry_ts": None, "exit_ts": ts_iso}
                continue

            # ── مرحله ۲: بعد از Entry → چک TP/SL ──
            hit_tp2 = (direction == "BUY" and h >= tp2) or (direction == "SELL" and l <= tp2)
            hit_tp1 = (direction == "BUY" and h >= tp1) or (direction == "SELL" and l <= tp1)
            hit_sl  = (direction == "BUY" and l <= stop) or (direction == "SELL" and h >= stop)

            # FIX (backtest audit, high severity): same same-candle ambiguity fix as
            # the within-entry-candle block above — see _resolve_intracandle_priority.
            _candidates = []
            if hit_tp2: _candidates.append((tp2, "TP2"))
            if hit_tp1 and not tp1_reached: _candidates.append((tp1, "TP1"))
            if hit_sl: _candidates.append((stop, "SL"))
            _winner = _resolve_intracandle_priority(candle["open"], _candidates) if _candidates else None

            if _winner == "TP2":
                # FIX v3.4: TP2 زده شد — نتیجه نهایی TP2 (چه TP1 قبلاً زده شده باشد چه نه)
                pnl = ((tp2 - entry) / entry * 100) if direction == "BUY" else ((entry - tp2) / entry * 100)
                rr  = pnl / risk_pct if risk_pct else 0
                return {**result, "outcome": "TP2", "pnl_percent": round(pnl, 2),
                        "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}
            elif _winner == "TP1":
                # FIX v3.4: TP1 زده شد — loop ادامه می‌دهد برای TP2 یا SL (break even)
                tp1_reached = True
                # ادامه می‌دهیم — منتظر TP2 یا SL می‌مانیم
            elif _winner == "SL":
                if tp1_reached:
                    # FIX v3.4: TP1 زده شد سپس SL لمس شد → TP1 (TOUCH SL) = Break Even
                    pnl = ((tp1 - entry) / entry * 100) if direction == "BUY" else ((entry - tp1) / entry * 100)
                    rr  = pnl / risk_pct if risk_pct else 0
                    return {**result, "outcome": "TP1_TOUCHSL", "pnl_percent": round(pnl, 2),
                            "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}
                else:
                    # SL مستقیم بدون TP1 → ضرر
                    pnl = ((stop - entry) / entry * 100) if direction == "BUY" else ((entry - stop) / entry * 100)
                    rr  = -abs(pnl / risk_pct) if risk_pct else -1.0
                    return {**result, "outcome": "SL", "pnl_percent": round(-abs(pnl), 2),
                            "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}

        # بازه تمام شد بدون نتیجه — اگر TP1 زده شده بود آن را برگردان
        if tp1_reached:
            pnl = ((tp1 - entry) / entry * 100) if direction == "BUY" else ((entry - tp1) / entry * 100)
            rr  = pnl / risk_pct if risk_pct else 0
            return {**result, "outcome": "TP1", "pnl_percent": round(pnl, 2),
                    "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": None}
        return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                "entry_ts": entry_candle_ts, "exit_ts": None}
    except Exception as e:
        log.debug(f"Backtest simulate error {symbol}: {e}")
        return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                "entry_ts": None, "exit_ts": None}

def _rows_to_trades(rows) -> list:
    """Shared row→trade mapping used by every closed-trades reader
    (_load_closed_trades_from_db for the hours-based Signal Performance
    Report, and _load_closed_trades_by_range for the calendar-based
    Weekly/Monthly reports) — extracted verbatim from the original
    _load_closed_trades_from_db loop body so every report keeps using
    IDENTICAL status-mapping/pnl-fallback/rr-fallback logic, never a
    separate calculation system."""
    trades = []
    # نگاشت status دیتابیس به outcome بک‌تست
    _status_to_outcome = {
        "TP1": "TP1", "TP1_HIT": "TP1",
        "TP2": "TP2", "TP2_HIT": "TP2",
        "TP1_TOUCHSL": "TP1_TOUCHSL",
        "SL": "SL", "SL_HIT": "SL",
        "MISSED": "MISSED",
        "EXPIRED": "OPEN",
    }

    for row in rows:
        raw_status = row["status"] or "OPEN"
        outcome = _status_to_outcome.get(raw_status, "OPEN")

        # pnl محاسبه می‌شود اگر در DB موجود است، وگرنه از entry/close_price محاسبه می‌کنیم
        pnl = row["pnl_percent"]
        if pnl is None and row["close_price"] and row["entry"]:
            direction = row["direction"]
            if direction == "BUY":
                pnl = (row["close_price"] - row["entry"]) / row["entry"] * 100
            else:
                pnl = (row["entry"] - row["close_price"]) / row["entry"] * 100
            pnl = round(pnl, 2)

        rr = row["rr_multiple"]
        if rr is None and pnl is not None and row["entry"] and row["stop"]:
            risk_pct = abs(row["entry"] - row["stop"]) / row["entry"] * 100
            if risk_pct > 0:
                rr = round(pnl / risk_pct, 3)

        trades.append({
            "symbol":      row["symbol"],
            "strategy":    row["strategy"],
            "direction":   row["direction"],
            "entry":       row["entry"],
            "stop":        row["stop"],
            "tp1":         row["tp1"],
            "tp2":         row["tp2"],
            "score":       row["score"],
            "outcome":     outcome,
            "pnl_percent": pnl,
            "rr_multiple": rr,
            # entry_ts: از entered_at یا opened_at
            "entry_ts":    row["entered_at"] or row["opened_at"],
            # exit_ts: از closed_at
            "exit_ts":     row["closed_at"],
            "trend_state": "",
            "adx_value":   0,
            "ema200_slope": "",
            "vol_regime":  "",
            "session":     "",
            "entry_reason": "",
        })

    return trades


def _load_closed_trades_from_db(timerange_hours: int) -> list:
    """
    معاملات واقعی بسته‌شده را از دیتابیس می‌خواند.
    فقط سیگنال‌هایی که در بازه timerange_hours گذشته بسته شده‌اند برمی‌گردند.

    نکات مهم:
    - فیلتر زمانی بر اساس closed_at (UTC ISO string) انجام می‌شود.
    - PENDING/ENTERED/TP1_HIT (هنوز باز) را حذف می‌کنیم.
    - opened_at برای سیگنال‌هایی که هنوز بسته نشده‌اند به عنوان fallback استفاده می‌شود.
    - timestamp مقایسه: هر دو طرف UTC هستند → مشکل timezone وجود ندارد.

    Rolling window (last N hours from now) — used by the existing "7 Days"
    / new "30 Days" Signal Performance Report admin-panel options. For
    calendar-based windows (exact Saturday→Friday week, exact calendar
    month) see _load_closed_trades_by_range() below, which shares the same
    query shape and the same _rows_to_trades() mapping — only the WHERE
    bounds differ.
    """
    cutoff_dt = datetime.now(timezone.utc) - __import__('datetime').timedelta(hours=timerange_hours)
    # SQLite ISO strings: "2024-01-15T10:30:00+00:00" یا "2024-01-15T10:30:00"
    # هر دو فرمت با مقایسه string کار می‌کنند چون ISO 8601 lexicographically مرتب است
    # برای اطمینان از سازگاری، cutoff را به فرمت ساده ISO بدون timezone تبدیل می‌کنیم
    cutoff_str = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S")

    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy,
                   s.entry, s.stop, s.tp1, s.tp2, s.score,
                   s.opened_at,
                   r.status, r.entered_at, r.tp1_hit, r.tp2_hit,
                   r.tp1_hit_at, r.tp2_hit_at,
                   r.closed_at, r.close_price, r.pnl_percent, r.rr_multiple
            FROM signals s
            JOIN results r ON r.signal_id = s.id
            WHERE r.status NOT IN ('PENDING', 'ENTERED', 'TP1_HIT', 'OPEN')
              AND r.closed_at IS NOT NULL
              AND substr(r.closed_at, 1, 19) >= ?
            ORDER BY r.closed_at ASC
        """, (cutoff_str,)).fetchall()
    finally:
        conn.close()

    return _rows_to_trades(rows)


def _load_closed_trades_by_range(start_utc: datetime, end_utc: datetime) -> list:
    """
    Same DB read as _load_closed_trades_from_db (identical query shape,
    identical NOT IN(...)/closed_at-not-null filter, identical
    _rows_to_trades() mapping) but bounded by an explicit [start_utc,
    end_utc] closed_at window instead of "last N hours from now" — used by
    the Weekly/Monthly CALENDAR reports (exact Saturday→Friday week, exact
    calendar month) so they reuse exactly the same trade-loading and
    metric logic as the existing hours-based Signal Performance Report,
    per spec §5/§8 (no separate calculation system).

    start_utc/end_utc must already be timezone-aware UTC datetimes — see
    get_previous_calendar_week_range_tehran() / get_previous_calendar_month_range_tehran(),
    which compute the Asia/Tehran calendar boundary and convert it to UTC
    before this function ever sees it (all timestamps in the DB are UTC —
    see _load_closed_trades_from_db's own comment above).
    """
    start_str = start_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy,
                   s.entry, s.stop, s.tp1, s.tp2, s.score,
                   s.opened_at,
                   r.status, r.entered_at, r.tp1_hit, r.tp2_hit,
                   r.tp1_hit_at, r.tp2_hit_at,
                   r.closed_at, r.close_price, r.pnl_percent, r.rr_multiple
            FROM signals s
            JOIN results r ON r.signal_id = s.id
            WHERE r.status NOT IN ('PENDING', 'ENTERED', 'TP1_HIT', 'OPEN')
              AND r.closed_at IS NOT NULL
              AND substr(r.closed_at, 1, 19) >= ?
              AND substr(r.closed_at, 1, 19) <= ?
            ORDER BY r.closed_at ASC
        """, (start_str, end_str)).fetchall()
    finally:
        conn.close()

    return _rows_to_trades(rows)


def _summarize_trades(trades: list) -> dict:
    """Shared summary-metric calculation — extracted verbatim from
    _run_backtest_job's own summary block (same wins/losses/decisive/
    win_rate/total_pnl formula, including the same "only decisive TP/SL
    trades count toward win_rate's denominator" fix already in place there)
    so every report (hours-based 7/30-day, and the new calendar-based
    Weekly/Monthly) computes win rate identically. _run_backtest_job below
    calls this too, so nothing behaves differently than before."""
    wins = [t for t in trades if t.get("outcome") in ("TP1", "TP1_TOUCHSL", "TP2")]
    losses = [t for t in trades if t.get("outcome") == "SL"]
    total_pnl = sum(t.get("pnl_percent", 0) or 0 for t in trades)
    decisive = len(wins) + len(losses)
    return {
        "total":     len(trades),
        "wins":      len(wins),
        "losses":    len(losses),
        "win_rate":  round(len(wins) / decisive * 100, 1) if decisive > 0 else 0,
        "total_pnl": round(total_pnl, 2),
    }


async def _run_backtest_job(session, job: dict) -> dict:
    """
    اجرای یک job بک‌تست با خواندن معاملات واقعی بسته‌شده از دیتابیس.

    رویکرد جدید (v3.4 fix):
    - به جای اجرای مجدد استراتژی‌ها و شبیه‌سازی، معاملاتی که واقعاً در دیتابیس
      ثبت و بسته شده‌اند را می‌خوانیم.
    - فیلتر زمانی: فقط معاملاتی که closed_at آن‌ها در بازه timerange_hours گذشته
      باشد وارد گزارش می‌شود.
    - هیچ معامله‌ای به دلیل مشکل timestamp، UTC/Local یا query اشتباه حذف نمی‌شود.
    """
    global _backtest_running
    _backtest_running = True

    job_id    = job["job_id"]
    timerange = job["timerange_hours"]

    # ── به‌روزرسانی وضعیت job در DB ──
    conn = get_db_connection()
    try:
        conn.execute("UPDATE backtest_jobs SET status='RUNNING' WHERE job_id=?", (job_id,))
        conn.commit()
    finally:
        conn.close()

    # ── خواندن معاملات واقعی بسته‌شده از دیتابیس ──
    try:
        all_trades = _load_closed_trades_from_db(timerange)
    except Exception as e:
        log.error(f"BT DB read error: {e}")
        all_trades = []

    log.info(f"BT job {job_id}: {len(all_trades)} closed trades found in last {timerange}h")
    if not all_trades:
        log.info(f"BT job {job_id}: No closed trades in DB for last {timerange}h — backtest result will be empty")

    # ── Summary (FIX/bug audit note preserved: win_rate's denominator is
    # only decisive TP/SL trades, not the full trade count which would also
    # include MISSED/EXPIRED — see _summarize_trades()) ──
    summary = _summarize_trades(all_trades)

    # ── به‌روزرسانی DB ──
    conn = get_db_connection()
    try:
        conn.execute(
            "UPDATE backtest_jobs SET status='DONE', completed_at=?, result_summary=? WHERE job_id=?",
            (datetime.now(timezone.utc).isoformat(), json.dumps(summary), job_id)
        )
        conn.commit()
    finally:
        conn.close()

    _backtest_cache[job_id] = {"trades": all_trades, "summary": summary}
    _backtest_cache[f"{job_id}_ts"] = time.time()
    _backtest_running = False
    return {"job_id": job_id, "trades": all_trades, "summary": summary}

async def enqueue_backtest(session, job_params: dict) -> str:
    """
    یک job بک‌تست را در صف قرار می‌دهد (یا از cache برمی‌گرداند اگر تکراری باشد).
    خروجی: job_id
    """
    job_id = _bt_job_id(job_params)

    # ── cache hit ──
    if job_id in _backtest_cache:
        cached = _backtest_cache[job_id]
        cached_time = _backtest_cache.get(f"{job_id}_ts", 0)
        if time.time() - cached_time < BACKTEST_CACHE_TTL:
            return job_id

    # ── ثبت در DB ──
    now_str = datetime.now(timezone.utc).isoformat()
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO backtest_jobs (job_id,status,requested_by,params,created_at) VALUES (?,?,?,?,?)",
            (job_id, "QUEUED", str(job_params.get("requested_by","")), json.dumps(job_params), now_str)
        )
        conn.commit()
    finally:
        conn.close()

    # ── queue ──
    if _backtest_queue and not _backtest_queue.full():
        job_params["job_id"] = job_id
        await _backtest_queue.put(job_params)

    return job_id

async def backtest_worker_loop(session, state):
    """حلقه background که jobهای صف بک‌تست را یکی‌یکی اجرا می‌کند (بدون block کردن Live Engine)."""
    while True:
        try:
            if _backtest_queue is None:
                await asyncio.sleep(5)
                continue
            job = await asyncio.wait_for(_backtest_queue.get(), timeout=10)
            job["state"] = state
            log.info(f"BT job starting: {job['job_id']}")
            await _run_backtest_job(session, job)
            log.info(f"BT job done: {job['job_id']}")
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            log.error(f"BT worker error: {e}")
        await asyncio.sleep(1)


# ─── CHART GENERATION (simple ASCII/text chart for Telegram) ──────────────────
def build_trade_chart(symbol: str, entry: float, stop: float, tp1: float, tp2: float,
                      direction: str, outcome: str = None) -> str:
    """
    ساخت یک نمودار متنی ساده برای نمایش Entry/SL/TP در تلگرام.
    (برای chart تصویری واقعی، نیاز به matplotlib دارد که در محیط production ممکن است موجود نباشد)
    """
    decimals = 8 if entry < 0.01 else (4 if entry < 10 else 2)
    f = lambda v: f"{v:.{decimals}f}"
    arrow = "↑" if direction == "BUY" else "↓"
    outcome_str = f" ➜ {outcome}" if outcome else ""
    lines = [
        f"📊 <b>{symbol} {direction} {arrow}</b>{outcome_str}",
        f"",
        f"   🎯 TP2: {f(tp2)}",
        f"   ─────────────────",
        f"   🎯 TP1: {f(tp1)}",
        f"   ─────────────────",
        f"   💰 Entry: {f(entry)}  ◄",
        f"   ─────────────────",
        f"   🛑 SL: {f(stop)}",
    ]
    if direction == "SELL":
        lines = [
            f"📊 <b>{symbol} {direction} {arrow}</b>{outcome_str}",
            f"",
            f"   🛑 SL: {f(stop)}",
            f"   ─────────────────",
            f"   💰 Entry: {f(entry)}  ◄",
            f"   ─────────────────",
            f"   🎯 TP1: {f(tp1)}",
            f"   ─────────────────",
            f"   🎯 TP2: {f(tp2)}",
        ]
    return "\n".join(lines)


# ─── BACKTEST REPORT BUILDER ──────────────────────────────────────────────────
def build_backtest_report(trades: list, summary: dict, timerange_hours: Optional[int],
                           period_label: str = None, report_title: str = "AFEE TRADER Backtest Report") -> list[str]:
    """
    ساخت گزارش بک‌تست به صورت لیستی از پیام‌ها (هر پیام حداکثر ۳۸۰۰ کاراکتر — chunked).
    برای هر trade: symbol، strategy، direction، entry/SL/TP، outcome، PnL، R-multiple، context.

    period_label/report_title (both optional, additive): let calendar-based
    callers (Weekly/Monthly Signal Performance Report) reuse this exact
    function — same header layout, same per-trade formatting, same chunking
    — with a calendar date-range label instead of "N hours" and a
    distinguishing title, WITHOUT touching a single existing caller: every
    existing call site only ever passed the first 3 positional args, so
    period_label stays None and report_title keeps its old default there,
    producing byte-for-byte the same output as before.
    """
    messages = []
    period_str = period_label if period_label else f"{timerange_hours} hours"
    header = (
        f"📊 <b>{report_title}</b>\n"
        f"⏱ Period: {period_str}\n"
        f"📦 Total Signals: {summary['total']} | ✅ Wins: {summary['wins']} | ❌ Losses: {summary['losses']}\n"
        f"🎯 Win Rate: {summary['win_rate']}% | 💵 Total PnL: {summary['total_pnl']:+.2f}%\n"
        f"━━━━━━━━━━━━━━━\n"
    )
    messages.append(header)

    chunk = ""
    for t in trades:
        decimals = 8 if t.get("entry", 1) < 0.01 else (4 if t.get("entry", 1) < 10 else 2)
        f = lambda v: f"{v:.{decimals}f}" if v else "—"
        outcome_emoji = {"TP1": "✅", "TP1_TOUCHSL": "🔄", "TP2": "✅✅", "SL": "❌", "OPEN": "⏳", "MISSED": "⚠️"}.get(t.get("outcome",""), "")
        pnl_str = f"{t['pnl_percent']:+.2f}%" if t.get("pnl_percent") is not None else "—"
        rr_str  = f"{t['rr_multiple']:+.2f}R" if t.get("rr_multiple") is not None else "—"
        trade_txt = (
            f"{outcome_emoji} <b>{t.get('symbol','')} | {t.get('strategy','')} | {t.get('direction','')}</b>\n"
            f"  Entry: {f(t.get('entry'))} | SL: {f(t.get('stop'))} | TP1: {f(t.get('tp1'))} | TP2: {f(t.get('tp2'))}\n"
            f"  Result: {t.get('outcome','—')} | PnL: {pnl_str} | RR: {rr_str}\n"
            f"  Entry Time: {_ms_to_iso(t.get('entry_ts')) or '—'} | Exit Time: {_ms_to_iso(t.get('exit_ts')) or '—'}\n"
        )
        if t.get("trend_state"):
            trade_txt += f"  🌐 Trend: {t['trend_state']}\n"
        if t.get("session"):
            trade_txt += f"  🕐 Session: {t['session']}\n"
        if t.get("entry_reason"):
            trade_txt += f"  📝 {t['entry_reason']}\n"
        trade_txt += "─────\n"

        if len(chunk) + len(trade_txt) > 3800:
            messages.append(chunk)
            chunk = trade_txt
        else:
            chunk += trade_txt

    if chunk:
        messages.append(chunk)

    return messages


# ═══════════════════════════════════════════════════════════════════════════════
# 🧠 LAYER C — PERSONAL ANALYZER  (unfiltered — filters become warnings only)
# ═══════════════════════════════════════════════════════════════════════════════
async def _build_analyzer_warnings(session, symbol: str, result: dict, state: dict) -> list[str]:
    """
    فیلترها را روی سیگنال اجرا می‌کند ولی هیچ‌کدام را بلاک نمی‌کند.
    فقط لیستی از هشدارها برمی‌گرداند.
    """
    warnings = []
    direction = result["direction"]

    # Session warning
    if not is_session_allowed(state):
        warnings.append(f"⚠️ Session mismatch ({get_current_session().upper()})")

    # Volume warning
    try:
        ma_period = state.get("entry_volume_ma_period", 20)
        vol_multiplier = state.get("entry_volume_multiplier", 1.5)
        c5m_vol = await get_candles(session, symbol, "5m", ma_period + 5)
        if not has_sufficient_entry_volume(c5m_vol, ma_period, vol_multiplier):
            warnings.append("⚠️ Volume anomaly detected (entry candle volume below threshold)")
    except Exception:
        pass

    # ATR volatility warning
    try:
        atr_mult = get_atr_multiplier()
        c5m_atr = await get_candles(session, symbol, "5m", 30)
        if is_volatility_abnormal(c5m_atr, threshold=atr_mult):
            warnings.append("⚠️ High volatility regime")
    except Exception:
        pass

    # Opposing volume warning
    try:
        c5m = await get_candles(session, symbol, "5m", 30)
        if has_suspicious_opposing_volume(c5m, direction):
            warnings.append("⚠️ News / macro risk zone (suspicious opposing volume)")
    except Exception:
        pass

    # Trend conflict warning
    try:
        if not await is_aligned_with_higher_trend(session, symbol, direction):
            warnings.append("⚠️ Trend conflict (signal opposes 1H trend)")
    except Exception:
        pass

    # Regime mismatch warning
    if result.get("regime_mismatch"):
        warnings.append("⚠️ Strategy type mismatches current market regime (ADX zone)")

    return warnings

async def normalize_and_validate_symbol(session, raw_text: str) -> Optional[str]:
    """اسم ارز ورودی کاربر را به فرمت نماد بایننس (مثل BTCUSDT) تبدیل و اعتبارسنجی می‌کند."""
    sym = raw_text.strip().upper().replace(" ", "")
    if not sym:
        return None
    if not sym.endswith("USDT"):
        sym += "USDT"
    try:
        data = await binance_request(session, f"{BINANCE_BASE}/ticker/price", params={"symbol": sym}, timeout=10)
        if not isinstance(data, dict) or "price" not in data:
            return None
    except Exception:
        return None
    return sym

async def analyze_symbol_manually(session, symbol: str, state: dict = None) -> str:
    """
    🧠 LAYER C — PERSONAL ANALYZER
    تحلیل دستی یک ارز با تمام استراتژی‌ها.
    فیلترها هیچ‌گاه سیگنال را بلاک نمی‌کنند — فقط هشدار به خروجی اضافه می‌شود.
    """
    state = state or {}
    found_signals = []

    try:
        regime = await get_market_regime(session, symbol)
    except Exception:
        regime = None

    for strat_name, strat_fn in STRATEGIES:
        try:
            result = await strat_fn(session, symbol, state)
            if not result:
                continue

            # ── Regime mismatch: فقط flag می‌گذاریم، بلاک نمی‌کنیم ──
            if regime:
                strat_type = STRATEGY_REGIME_TYPE.get(strat_name, "HYBRID")
                if strat_type not in regime["allowed_types"]:
                    result["regime_mismatch"] = True

            # ── اعمال وزن آداپتیو روی Score ──
            try:
                weight = get_strategy_weight(strat_name)
                if weight != 1.0:
                    result["score"] = max(1, min(100, round(result.get("score", 0) * weight)))
                    result["strategy_weight"] = weight
            except Exception:
                pass

            # ── جمع‌آوری هشدارها (بدون بلاک) ──
            result["analyzer_warnings"] = await _build_analyzer_warnings(session, symbol, result, state)

            found_signals.append(result)
        except Exception as e:
            log.debug(f"Analyzer error {strat_name} {symbol}: {e}")

    # قیمت فعلی
    current_price = await get_live_futures_price(session, symbol)

    decimals = 8 if (current_price and current_price < 0.01) else (4 if (current_price and current_price < 10) else 2)
    price_str = f"{current_price:.{decimals}f}" if current_price is not None else "Unknown"
    regime_str = f"🌐 Market Regime: {regime['regime_label']}\n" if regime else ""

    if not found_signals:
        return (
            f"🔍 <b>Personal Analyzer — {symbol}</b>\n\n"
            f"🔰 Current Price: {price_str} USDT\n"
            f"{regime_str}"
            f"❌ No setup identified by any strategy."
        )

    lines = [f"🔍 <b>Personal Analyzer — {symbol}</b>", f"🔰 Current Price: {price_str} USDT"]
    if regime_str:
        lines.append(regime_str.strip())
    lines.append("")

    for result in found_signals:
        emoji = "🟢" if result["direction"] == "BUY" else "🔴"
        lines.append(f"{emoji} <b>{result['strategy']}</b> — {result['direction']}")
        lines.append(f"   💰 Entry: {result['entry']:.{decimals}f}")
        lines.append(f"   🛑 Stop: {result['stop']:.{decimals}f}")
        lines.append(f"   🎯 TP1: {result['tp1']:.{decimals}f} (1.5R)")
        lines.append(f"   🎯 TP2: {result['tp2']:.{decimals}f} (3R)")
        lines.append(f"   ⭐️ Score: {result['score']}/100")
        if result.get("rsi") is not None:
            lines.append(f"   📐 RSI: {result['rsi']}")
        lines.append(f"   📈 {result['timeframe']}")
        # ── هشدارها (فقط نمایش، هیچ سیگنالی بلاک نمی‌شود) ──
        for w in result.get("analyzer_warnings", []):
            lines.append(f"   {w}")
        lines.append("")

    return "\n".join(lines)

# ─── PERMISSIONS ──────────────────────────────────────────────────────────────
PERMISSIONS = {
    "scan_toggle":   "Turn scanning on/off",
    "strategies":    "Manage strategies",
    "blacklist":     "Manage blacklist",
    "logs":          "View logs",
    "channels":      "Manage channels/groups",
    "admins":        "Manage admins",
}
ALL_PERMS = list(PERMISSIONS.keys())

# ─── BOT STATE ────────────────────────────────────────────────────────────────
# bot_config.json holds only PERMANENT user settings (the kind you'd want to
# carry over when moving the bot to another server) — strategy toggles,
# filters, thresholds, admins, channels, session filters, etc., alongside the
# TOP_N_COINS / Dynamic Score values from BOT_CONFIG_DEFAULTS above.
#
# bot_runtime.json holds only TRANSIENT/operational data that is *not* a user
# setting — pending Telegram flows (pending_admin_add, pending_analysis,
# pending_replay, pending_backtest) and last-execution bookkeeping timestamps
# (last_report_date, last_reweight_date). RUNTIME_STATE_KEYS (defined near
# BOT_CONFIG_FILE, above) is the authoritative list of which fields go there.

def load_state() -> dict:
    default = {
        "scanning": True,
        "disabled_strategies": [],
        "admins": {},
        "channels": [],
        "pending_admin_add": None,
        "pending_analysis": None,
        "pending_replay": None,
        "pending_backtest": None,       # uid منتظر تنظیمات بک‌تست
        "pending_setscore": None,       # {"uid":..., "idx":...} منتظر عدد جدید min score یک استراتژی
        "backtest_enabled": True,
        "last_report_date": None,
        "daily_report_msg_ids": {},
        "daily_report_msg_date": None,
        "daily_report_last_text": None,
        "daily_report_collecting_day": None,
        # Weekly/Monthly Signal Performance Report (feature): id of the last
        # calendar week/month a report was already sent for (see
        # RUNTIME_STATE_KEYS comment above). None = never sent yet.
        "weekly_report_last_week_id": None,
        "monthly_report_last_month_id": None,
        "volume_filter_enabled": True,
        "entry_volume_filter_enabled": True,
        "entry_volume_ma_period": 20,
        "entry_volume_multiplier": 1.5,
        "adaptive_ranking_enabled": True,
        "last_reweight_date": None,
        "regime_engine_enabled": True,
        "quality_ranking_enabled": True,
        "session_filters": SESSION_FILTERS_DEFAULT.copy(),   # جدید: فیلتر session
        "adx_threshold": ADX_THRESHOLD_DEFAULT,              # جدید: آستانه ADX (قابل تنظیم)
        "atr_multiplier": ATR_MULTIPLIER_DEFAULT,            # جدید: ضریب ATR
        "strategy_settings": {},   # تنظیمات فیلترهای مخصوص هر استراتژی (Strategy Settings)
        "news_events": [],         # لیست زمان‌بندی اخبار مهم (ISO timestamps) برای News Filter
    }

    # ── Load the permanent-settings half from bot_config.json ──
    cfg_loaded = None
    if os.path.exists(BOT_CONFIG_FILE):
        try:
            with open(BOT_CONFIG_FILE, encoding="utf-8") as f:
                cfg_loaded = json.load(f)
        except Exception as e:
            log.error(f"⚠️ bot_config.json خراب یا ناقص بود: {e}")
            # نسخه خراب رو برای بررسی بعدی نگه می‌داریم به جای از دست دادنش
            try:
                os.replace(BOT_CONFIG_FILE, BOT_CONFIG_FILE + ".corrupted")
            except Exception:
                pass
            # تلاش برای ریکاوری از آخرین بکاپ سالم، به جای ریست کامل
            backup_file = BOT_CONFIG_FILE + ".bak"
            if os.path.exists(backup_file):
                try:
                    with open(backup_file, encoding="utf-8") as f:
                        cfg_loaded = json.load(f)
                    log.info("✅ اطلاعات از روی فایل بکاپ (bot_config.json.bak) با موفقیت بازیابی شد.")
                except Exception as e2:
                    log.error(f"بکاپ هم خراب بود، با تنظیمات پیش‌فرض شروع شد: {e2}")
    if cfg_loaded:
        # bot_config.json should only ever hold permanent settings, but guard
        # against a stray runtime key ending up in there anyway (e.g. a hand
        # edit) — those still get picked up here so nothing silently vanishes,
        # save_state() below will route them back out to bot_runtime.json.
        default.update(cfg_loaded)

    # ── Load the transient/runtime half from bot_runtime.json ──
    rt_loaded = None
    if os.path.exists(BOT_RUNTIME_FILE):
        try:
            with open(BOT_RUNTIME_FILE, encoding="utf-8") as f:
                rt_loaded = json.load(f)
        except Exception as e:
            log.error(f"⚠️ bot_runtime.json خراب یا ناقص بود: {e}")
            try:
                os.replace(BOT_RUNTIME_FILE, BOT_RUNTIME_FILE + ".corrupted")
            except Exception:
                pass
            backup_file = BOT_RUNTIME_FILE + ".bak"
            if os.path.exists(backup_file):
                try:
                    with open(backup_file, encoding="utf-8") as f:
                        rt_loaded = json.load(f)
                    log.info("✅ اطلاعات از روی فایل بکاپ (bot_runtime.json.bak) با موفقیت بازیابی شد.")
                except Exception as e2:
                    log.error(f"بکاپ هم خراب بود، با تنظیمات پیش‌فرض شروع شد: {e2}")
    if rt_loaded:
        default.update(rt_loaded)

    log.info(
        f"State loaded | config_file={'found' if cfg_loaded else 'defaults'} "
        f"runtime_file={'found' if rt_loaded else 'defaults'} "
        f"admins={len(default.get('admins', {}))} channels={len(default.get('channels', []))} "
        f"scanning={default.get('scanning')}"
    )
    return default

def _atomic_write_json_with_backup(path: str, data: dict):
    """Shared helper: back up the current (valid) file to .bak, write the new
    data to a .tmp file, then atomically replace the real file with it. Same
    safety pattern save_state() always used, factored out so both
    bot_config.json and bot_runtime.json get identical crash-safety."""
    tmp_file = path + ".tmp"
    backup_file = path + ".bak"
    # بکاپ از نسخه فعلی (در صورت سالم بودن) قبل از رونویسی
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                json.load(f)  # فقط برای تست سالم بودن JSON فعلی
            import shutil
            shutil.copyfile(path, backup_file)
        except Exception:
            pass  # اگر فایل فعلی خودش خراب بود، بکاپ قدیمی‌تر دست‌نخورده می‌ماند

    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_file, path)

def save_state(state: dict):
    """نوشتن امن (Atomic) + بکاپ برای هر دو فایل persistence:
    bot_config.json فقط تنظیمات دائمی کاربر را نگه می‌دارد (چیزهایی که موقع
    انتقال ربات به سرور دیگر باید حفظ شوند)، و bot_runtime.json فقط داده‌های
    موقت/عملیاتی (pending_* و last_*_date) را. کلیدهای هرکدام در
    RUNTIME_STATE_KEYS مشخص شده‌اند. هر دو فایل جداگانه با همان الگوی
    tmp-file + os.replace + .bak نوشته می‌شوند تا حتی در صورت قطع برنامه یا
    خرابی فایل، همیشه یک نسخه قبلی سالم برای ریکاوری وجود داشته باشد."""
    config_part  = {k: v for k, v in state.items() if k not in RUNTIME_STATE_KEYS}
    runtime_part = {k: v for k, v in state.items() if k in RUNTIME_STATE_KEYS}
    # BUG FIX (random config reset): TOP_N_COINS / QUALITY_POOL_SIZE / Dynamic
    # Score thresholds (BOT_CONFIG_DEFAULTS keys) are owned exclusively by the
    # "/config" command handler, which reads bot_config.json, changes one key,
    # and writes it straight back to disk — it never updates `state` in
    # memory. `state` only ever picks these keys up opportunistically, at
    # load_state() time. If a user later ran /config to change one of these
    # values, and then ANY unrelated action triggered save_state() (toggling
    # a strategy filter, adding an admin, etc.), this function would persist
    # state's stale in-memory copy of these keys, silently reverting the
    # user's /config change back to whatever value was in memory at startup
    # — with no /config command involved in that particular save. Always
    # re-read the current on-disk values for these specific keys immediately
    # before writing, so save_state() can never regress them.
    # Task 3: min_signal_score (/setscore) and blacklist now live directly in
    # bot_config.json the same way (owned by their own read/write helpers,
    # not by `state`) — same belt-and-suspenders re-read applies to them so
    # an unrelated save_state() call (toggling a strategy, adding an admin,
    # etc.) can never silently wipe them either.
    _PRESERVED_DIRECT_CONFIG_KEYS = BOT_CONFIG_DEFAULTS.keys() | {"min_signal_score", "blacklist"}
    try:
        config_part.update({k: v for k, v in load_bot_config().items() if k in _PRESERVED_DIRECT_CONFIG_KEYS})
    except Exception:
        pass
    try:
        _atomic_write_json_with_backup(BOT_CONFIG_FILE, config_part)
    except Exception as e:
        log.error(f"خطا در ذخیره bot_config.json: {e}")
    try:
        _atomic_write_json_with_backup(BOT_RUNTIME_FILE, runtime_part)
    except Exception as e:
        log.error(f"خطا در ذخیره bot_runtime.json: {e}")
    log.debug(f"State saved | {BOT_CONFIG_FILE} + {BOT_RUNTIME_FILE} written")

def is_super_admin(uid: int, state: dict) -> bool:
    """اولین ادمین (سازنده ربات) همیشه دسترسی کامل دارد و قابل حذف نیست."""
    admins = state.get("admins", {})
    if not admins:
        return False
    first_id = list(admins.keys())[0]
    return str(uid) == first_id

def has_permission(uid: int, perm: str, state: dict) -> bool:
    if is_super_admin(uid, state):
        return True
    admin = state.get("admins", {}).get(str(uid))
    if not admin:
        return False
    return perm in admin.get("perms", [])

def is_admin(uid: int, state: dict) -> bool:
    return str(uid) in state.get("admins", {})

async def finalize_config_import(session, uid: int, cid, state: dict):
    """Runs after the Super Admin taps ✅ Yes on the import-confirmation prompt.
    Validates the previously-uploaded file, backs up the current
    bot_config.json, replaces it, and reloads settings immediately — no bot
    restart needed. On any validation/write failure the previous configuration
    is kept (or restored from backup) and an error is shown; nothing else
    about the bot's behavior is touched."""
    content = _PENDING_IMPORT_DATA.pop(uid, None)
    if content is None:
        await send_msg(session, cid, "❌ No pending configuration file found. Please upload bot_config.json again.")
        return

    # ── Validation (Section 5): size, JSON validity, required keys ──
    if len(content) > CONFIG_IMPORT_MAX_BYTES:
        await send_msg(session, cid, "❌ Import failed: file larger than 1 MB. Current configuration unchanged.")
        return
    try:
        new_cfg = json.loads(content.decode("utf-8"))
    except Exception:
        await send_msg(session, cid, "❌ Import failed: the file is not valid JSON. Current configuration unchanged.")
        return
    if not isinstance(new_cfg, dict):
        await send_msg(session, cid, "❌ Import failed: invalid configuration format. Current configuration unchanged.")
        return
    missing = sorted(k for k in CONFIG_IMPORT_REQUIRED_KEYS if k not in new_cfg)
    if missing:
        await send_msg(session, cid,
            "❌ Import failed: missing required configuration keys:\n" +
            "\n".join(f"• <code>{k}</code>" for k in missing) +
            "\n\nCurrent configuration unchanged.")
        return

    # ── Security (Section 6): never import secrets/tokens/keys ──
    for sk in CONFIG_IMPORT_SECRET_BLOCKLIST:
        new_cfg.pop(sk, None)

    # ── Backup current config before touching anything ──
    backup_path = BOT_CONFIG_FILE + ".importbak"
    try:
        if os.path.exists(BOT_CONFIG_FILE):
            shutil.copyfile(BOT_CONFIG_FILE, backup_path)
    except Exception as e:
        log.error(f"Config import: pre-import backup failed: {e}")

    # ── Replace bot_config.json ──
    try:
        _atomic_write_json_with_backup(BOT_CONFIG_FILE, new_cfg)
    except Exception as e:
        log.error(f"Config import: write failed, restoring backup: {e}")
        try:
            if os.path.exists(backup_path):
                shutil.copyfile(backup_path, BOT_CONFIG_FILE)
        except Exception as e2:
            log.error(f"Config import: backup restore failed: {e2}")
        await send_msg(session, cid, "❌ Import failed while saving. Previous configuration has been restored.")
        return

    # ── Reload configuration immediately, no restart required ──
    try:
        fresh = load_state()
        state.clear()
        state.update(fresh)
        # Backward compatibility (Section 8): an older export may predate a
        # setting that exists in the current code (e.g. a filter added after
        # the backup was taken). Fill in only the MISSING keys from the
        # current defaults — get_strategy_settings() never touches a key
        # that's already present, so nothing the imported file actually
        # specified is ever reset — then persist immediately so
        # bot_config.json is complete right after import, not just once some
        # unrelated setting is later touched.
        backfill_all_strategy_settings(state)
        save_state(state)
    except Exception as e:
        log.error(f"Config import: reload after import failed, restoring backup: {e}")
        try:
            if os.path.exists(backup_path):
                shutil.copyfile(backup_path, BOT_CONFIG_FILE)
                fresh = load_state()
                state.clear()
                state.update(fresh)
        except Exception as e2:
            log.error(f"Config import: backup restore after reload failure failed: {e2}")
        await send_msg(session, cid, "❌ Import failed while reloading. Previous configuration has been restored.")
        return

    log.info(f"bot_config.json imported and reloaded by super admin uid={uid}")
    await send_msg(session, cid,
        "✅ <b>Configuration imported and reloaded.</b>\n"
        "No restart was needed — the new settings are active now.\n"
        f"A backup of the previous configuration was saved as <code>{backup_path}</code>.")

# ─── TELEGRAM CONTROL PANEL ───────────────────────────────────────────────────

def iran_time_str() -> str:
    from datetime import timedelta
    iran_tz = timezone(timedelta(hours=3, minutes=30))
    now = datetime.now(iran_tz)
    try:
        import jdatetime
        jdt = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
        return jdt.strftime("%Y/%-m/%-d %H:%M:%S")
    except ImportError:
        return now.strftime("%Y-%m-%d %H:%M:%S")

def iran_date_str() -> str:
    """تاریخ امروز به وقت ایران، برای نمایش در عنوان گزارش (شمسی اگر jdatetime موجود باشد)."""
    from datetime import timedelta
    iran_tz = timezone(timedelta(hours=3, minutes=30))
    now = datetime.now(iran_tz)
    try:
        import jdatetime
        jdt = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
        return jdt.strftime("%Y/%-m/%-d")
    except ImportError:
        return now.strftime("%Y-%m-%d")

def utc_date_str() -> str:
    """تاریخ امروز به وقت UTC — همان مبنایی که opened_at/closed_at معاملات با آن ذخیره می‌شوند."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def tehran_date_str() -> str:
    """Today's calendar date in Asia/Tehran (UTC+03:30). Used both to decide
    *when* (wall-clock-wise) the automatic Daily Report's midnight rollover
    check should fire, and — via tehran_date_of_iso() below — as the actual
    date boundary that decides which trades belong to which Daily Report
    (see get_trades_for_report(), build_daily_report())."""
    from datetime import timedelta
    tehran_tz = timezone(timedelta(hours=3, minutes=30))
    return datetime.now(tehran_tz).strftime("%Y-%m-%d")

def tehran_date_of_iso(iso_str: Optional[str]) -> str:
    """CORRECTION: converts a stored UTC ISO timestamp (opened_at/closed_at,
    e.g. '2026-08-08T21:45:00.123456+00:00') into its Asia/Tehran (UTC+03:30)
    calendar date, e.g. '2026-08-09'. This is what get_trades_for_report()
    and build_daily_report() now use (instead of slicing the first 10 chars
    of the raw UTC timestamp) so the Daily Report's date boundary is the
    Tehran calendar day, not the UTC calendar day. Returns "" for a missing/
    unparseable timestamp, mirroring the previous `[:10]` slicing's behavior
    on empty input."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        from datetime import timedelta
        tehran_tz = timezone(timedelta(hours=3, minutes=30))
        return dt.astimezone(tehran_tz).strftime("%Y-%m-%d")
    except Exception:
        return (iso_str or "")[:10]

async def send_msg(session, chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    try:
        async with session.post(url, json=payload, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as r:
            return await r.json()
    except Exception as e:
        log.error(f"send_msg error: {e}")

async def edit_msg(session, chat_id, message_id, text, reply_markup=None):
    """FIX (Daily Report duplicate-message bug): previously had no retry on
    Telegram 429 rate limits — a single rate-limited edit call returned None,
    which sync_daily_report() misread as "this message can no longer be
    edited" and reacted by broadcasting a brand-new report message. Retrying
    with Telegram's own retry_after hint (same pattern as send_telegram)
    means a rate limit blip is absorbed here instead of looking like a
    permanent failure to every caller."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            async with session.post(url, json=payload, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                if data.get("ok"):
                    return data
                retry_after = (data.get("parameters") or {}).get("retry_after")
                if data.get("error_code") == 429 and retry_after is not None and attempt < max_attempts:
                    wait_s = min(float(retry_after), 30) + 0.5
                    log.warning(f"editMessageText rate-limited (429); retrying in {wait_s:.1f}s")
                    await asyncio.sleep(wait_s)
                    continue
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_attempts:
                await asyncio.sleep(1.5 * attempt)
                continue
            log.debug(f"editMessageText failed after {max_attempts} attempts: {e}")
            return None
        except Exception:
            return None
    return None

async def answer_callback(session, callback_id, text=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
    try:
        async with session.post(url, json={"callback_query_id": callback_id, "text": text},
                                proxy=PROXY, timeout=aiohttp.ClientTimeout(total=5)) as r:
            pass
    except Exception:
        pass

async def react_to_message(session, chat_id, message_id, emoji="❤️"):
    """با ایموجی به یک پیام ری‌اکشن میزند (تأیید دریافت دستور)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setMessageReaction"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reaction": json.dumps([{"type": "emoji", "emoji": emoji}]),
        "is_big": False,
    }
    try:
        async with session.post(url, json=payload, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if not data.get("ok"):
                log.debug(f"Reaction failed: {data}")
    except Exception as e:
        log.debug(f"react_to_message error: {e}")

async def download_telegram_file(session, file_id: str) -> Optional[bytes]:
    """Downloads a file previously uploaded to the bot (used by /importconfig
    and the auto-detected bot_config.json upload). Returns the raw bytes, or
    None on any failure (network error, file too big for the bot API, etc.)."""
    try:
        get_file_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getFile"
        async with session.get(get_file_url, params={"file_id": file_id}, proxy=PROXY,
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            info = await r.json()
        if not info.get("ok"):
            log.error(f"getFile failed: {info}")
            return None
        file_path = info["result"]["file_path"]
        dl_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
        async with session.get(dl_url, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=30)) as r:
            return await r.read()
    except Exception as e:
        log.error(f"download_telegram_file error: {e}")
        return None

async def send_document(session, chat_id, file_path: str, filename: str = None, caption: str = None):
    """Sends a local file as a Telegram document (used by /exportconfig)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    try:
        with open(file_path, "rb") as fobj:
            form = aiohttp.FormData()
            form.add_field("chat_id", str(chat_id))
            if caption:
                form.add_field("caption", caption)
                form.add_field("parse_mode", "HTML")
            form.add_field("document", fobj, filename=filename or os.path.basename(file_path))
            async with session.post(url, data=form, proxy=PROXY,
                                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                return await r.json()
    except Exception as e:
        log.error(f"send_document error: {e}")
        return None

async def get_chat_info(session, chat_id):
    """اطلاعات یک چت (کانال/گروه) را میگیرد — برای چک کردن عضویت/ادمین بودن ربات."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getChat"
    try:
        async with session.get(url, params={"chat_id": chat_id}, proxy=PROXY,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            return await r.json()
    except Exception:
        return None

async def get_chat_member(session, chat_id, user_id):
    """بررسی نقش ربات (یا کاربر) در یک چت."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getChatMember"
    try:
        async with session.get(url, params={"chat_id": chat_id, "user_id": user_id}, proxy=PROXY,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            return await r.json()
    except Exception:
        return None

def main_menu_kb(uid, state):
    icon = "🟢" if state["scanning"] else "🔴"
    bt_icon = "🟢" if state.get("backtest_enabled", True) else "🔴"
    vf_icon = "🟢" if state.get("volume_filter_enabled", True) else "🔴"
    ar_icon = "🟢" if state.get("adaptive_ranking_enabled", True) else "🔴"
    re_icon = "🟢" if state.get("regime_engine_enabled", True) else "🔴"
    qr_icon = "🟢" if state.get("quality_ranking_enabled", True) else "🔴"
    rows = []
    rows.append([{"text": "🔍 Personal Analyzer", "callback_data": "analyzer_start"}])
    rows.append([{"text": "📊 Signal Performance Report", "callback_data": "backtest_menu"}])
    rows.append([{"text": "🧪 Historical Candle Simulation", "callback_data": "replay_menu"}])
    rows.append([{"text": "🎬 Replay Mode (signal history)", "callback_data": "replay_start"}])
    if has_permission(uid, "scan_toggle", state):
        rows.append([{"text": f"{icon} Scan: {'ON' if state['scanning'] else 'OFF'}", "callback_data": "toggle_scan"}])
    if has_permission(uid, "strategies", state):
        rows.append([{"text": "📊 Strategies", "callback_data": "strategies"}])
    if has_permission(uid, "scan_toggle", state):
        rows.append([{"text": f"{bt_icon} Auto Backtest: {'ON' if state.get('backtest_enabled', True) else 'OFF'}", "callback_data": "toggle_backtest"}])
        rows.append([{"text": f"{vf_icon} Suspicious Volume Filter: {'ON' if state.get('volume_filter_enabled', True) else 'OFF'}", "callback_data": "toggle_volume_filter"}])
        rows.append([{"text": f"{ar_icon} Adaptive Strategy Ranking", "callback_data": "adaptive_menu"}])
        rows.append([{"text": f"{re_icon} Market Regime Engine", "callback_data": "regime_menu"}])
        rows.append([{"text": f"{qr_icon} Symbol Quality Ranking", "callback_data": "quality_menu"}])
        rows.append([{"text": "🧩 Strategy Settings", "callback_data": "strategy_settings_menu"}])
        rows.append([{"text": "⭐️ Set Minimum Score", "callback_data": "setscore_menu"}])
    if has_permission(uid, "blacklist", state):
        rows.append([{"text": "🚫 Blacklist", "callback_data": "bl_menu"}])
    if has_permission(uid, "channels", state):
        rows.append([{"text": "📢 Channels & Groups", "callback_data": "channels_menu"}])
    if has_permission(uid, "admins", state):
        rows.append([{"text": "👥 Manage Admins", "callback_data": "admins_menu"}])
    if has_permission(uid, "logs", state):
        rows.append([{"text": "📋 Recent Logs", "callback_data": "show_logs"}])
        rows.append([{"text": "📊 Today's Report (Manual)", "callback_data": "manual_report"}])
        rows.append([{"text": "📈 Advanced Strategy Statistics", "callback_data": "show_stats"}])
        rows.append([{"text": "🗂 Pending Signals", "callback_data": "pending_signals_menu"}])
    rows.append([{"text": "⚙️ Settings", "callback_data": "settings"}])
    return {"inline_keyboard": rows}

def backtest_menu_kb(state):
    sf = state.get("session_filters", SESSION_FILTERS_DEFAULT)
    rows = [
        [{"text": "⏱ Period: 1 hour", "callback_data": "bt_range_1"}],
        [{"text": "⏱ Period: 4 hours", "callback_data": "bt_range_4"}],
        [{"text": "⏱ Period: 12 hours", "callback_data": "bt_range_12"}],
        [{"text": "⏱ Period: 24 hours", "callback_data": "bt_range_24"}],
        [{"text": "⏱ Period: 7 days", "callback_data": "bt_range_168"}],
        # ADDED (requirement 4): rolling last-30-days option, directly below
        # 7 Days. Reuses the exact same generic "bt_range_<hours>" handler
        # (hours = int(data.split("_")[-1])) already used for every other
        # period here — no new handler needed. This is deliberately a
        # ROLLING 30-day window (720 hours from "now"), NOT the calendar
        # Monthly Report — see get_previous_calendar_month_range_tehran()
        # for the separate calendar-month calculation.
        [{"text": "⏱ Period: 30 days", "callback_data": "bt_range_720"}],
        [{"text": "📦 scope: all USDT pairs", "callback_data": "bt_scope_all"}],
        [{"text": "◀️ Back", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def replay_menu_kb():
    """
    Historical Candle Simulation / Strategy Replay Backtest — completely
    separate menu/callback namespace (replay_*) from Advanced Backtest's
    (bt_*), per the required architecture separation. Scope is the SAME
    for every period, 1h through 30 days: the live-configured symbol
    universe (TOP_N_COINS / quality-ranked pool exactly like scan_once()).

    CHANGED (v6.21.0, requirement 2): 7-day replay is no longer silently
    capped at a special 30-symbol subset. It now resolves the same
    universe as every other window here (see the replay_range_ handler).
    The job just takes longer for larger windows (7 or 30 days) of 1m
    data — the existing performance architecture (bounded concurrency,
    historical candle caching, fetch-once/reuse, pagination,
    rate-limit/backoff, progress updates, incremental persistence) is
    what makes that feasible, not a reduced symbol scope.

    ADDED (v6.22.0): a new 720-hour (30-day) window alongside
    1h/4h/12h/24h/7d. Uses the exact same code path as every other
    window — same universe resolution, same _prefetch_replay_cache()
    per-interval warm-up (independent of overall period length, so
    higher-timeframe strategies still get their full lookback), same
    walk-forward engine, same live filter/scoring pipeline. No
    30-day-specific branching exists anywhere in the replay engine.
    """
    rows = [
        [{"text": "⏱ 1 hour", "callback_data": "replay_range_1"}],
        [{"text": "⏱ 4 hours", "callback_data": "replay_range_4"}],
        [{"text": "⏱ 12 hours", "callback_data": "replay_range_12"}],
        [{"text": "⏱ 24 hours", "callback_data": "replay_range_24"}],
        [{"text": "⏱ 7 days", "callback_data": "replay_range_168"}],
        [{"text": "⏱ 30 days", "callback_data": "replay_range_720"}],
        [{"text": "◀️ Back", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def setscore_menu_kb(state):
    """Task 3: /setscore panel — one button per strategy, showing its
    current effective minimum score (its own override, or the legacy
    global default if it has none yet)."""
    rows = []
    for idx, (name, _) in enumerate(STRATEGIES):
        eff = get_strategy_min_score(state, name)
        cfg = get_strategy_settings(state, name)
        custom = cfg.get("min_signal_score") is not None
        tag = "" if custom else " (default)"
        rows.append([{"text": f"⭐️ {name}: {eff}/100{tag}", "callback_data": f"sc_open:{idx}"}])
    rows.append([{"text": "◀️ Back", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}


def strategy_settings_menu_kb():
    """List of strategies — each with its own card to open its dedicated settings."""
    rows = []
    for idx, (name, _) in enumerate(STRATEGIES):
        rows.append([{"text": f"⚙️ {name}", "callback_data": f"sf_open:{idx}"}])
    rows.append([{"text": "◀️ Back", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}

def strategy_settings_detail_kb(state, idx: int):
    """Settings page for a single strategy: legacy filters + new filters."""
    name, _ = STRATEGIES[idx]
    cfg = get_strategy_settings(state, name)
    sf = cfg.get("session_filters", SESSION_FILTERS_DEFAULT)
    lon = "🟢" if sf.get("london", True) else "🔴"
    ny  = "🟢" if sf.get("ny", True) else "🔴"
    asi = "🟢" if sf.get("asian", True) else "🔴"

    evf_icon = "🟢" if cfg.get("entry_volume_filter_enabled", True) else "🔴"
    oi_icon = "🟢" if cfg.get("oi_filter_enabled", False) else "🔴"
    fund_icon = "🟢" if cfg.get("funding_filter_enabled", False) else "🔴"
    btc_icon = "🟢" if cfg.get("btc_trend_filter_enabled", False) else "🔴"
    news_icon = "🟢" if cfg.get("news_filter_enabled", False) else "🔴"
    ema_icon = "🟢" if cfg.get("ema_filter_enabled", False) else "🔴"
    if cfg.get("ema_filter_mode", "manual") == "auto":
        ema_label = f"Auto EMA{cfg.get('ema_filter_period',50)}"
    else:
        ema_label = f"{cfg.get('ema_filter_direction','above')} EMA{cfg.get('ema_filter_period',50)}"
    ema_auto_atr_icon = "🟢" if cfg.get("ema_filter_auto_atr_enabled", True) else "🔴"
    adx_thresh_icon = "🟢" if cfg.get("adx_threshold_enabled", True) else "🔴"
    atr_range_icon = "🟢" if cfg.get("atr_range_filter_enabled", False) else "🔴"
    volma_icon = "🟢" if cfg.get("volume_ma_filter_enabled", False) else "🔴"
    atie_icon = "🟢" if cfg.get("ai_trade_intelligence_enabled", False) else "🔴"
    atr_sl_icon = "🟢" if cfg.get("atr_stop_buffer_enabled", True) else "🔴"

    p = f"sf:{idx}:"
    rows = [
        # ── Filters moved from the old "advanced filters" screen ──
        [{"text": f"ADX Threshold: {cfg.get('adx_threshold', ADX_THRESHOLD_DEFAULT)}", "callback_data": "noop"}],
        [{"text": "➖ ADX", "callback_data": p+"adx_dec"}, {"text": "➕ ADX", "callback_data": p+"adx_inc"}],
        [{"text": f"{adx_thresh_icon} ADX Threshold Filter", "callback_data": p+"adxthresh_toggle"}],
        [{"text": f"ATR Multiplier: {cfg.get('atr_multiplier', ATR_MULTIPLIER_DEFAULT)}", "callback_data": "noop"}],
        [{"text": "➖ ATR", "callback_data": p+"atr_dec"}, {"text": "➕ ATR", "callback_data": p+"atr_inc"}],
        [{"text": f"{lon} London Session", "callback_data": p+"sess_london"}],
        [{"text": f"{ny} NY Session",      "callback_data": p+"sess_ny"}],
        [{"text": f"{asi} Asian Session",  "callback_data": p+"sess_asian"}],
        # ── Entry candle volume filter ──
        [{"text": f"{evf_icon} Entry Volume Filter", "callback_data": p+"evf_toggle"}],
        [{"text": f"Volume MA Period: {cfg.get('entry_volume_ma_period', 20)}", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"evf_ma_dec"}, {"text": "➕", "callback_data": p+"evf_ma_inc"}],
        [{"text": f"Volume Multiplier: {cfg.get('entry_volume_multiplier', 1.5)}", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"evf_mult_dec"}, {"text": "➕", "callback_data": p+"evf_mult_inc"}],
        # ── Open Interest Filter ──
        [{"text": f"{oi_icon} Open Interest Filter", "callback_data": p+"oi_toggle"}],
        [{"text": f"Min OI Change: {cfg.get('oi_threshold', 5.0)}%", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"oi_dec"}, {"text": "➕", "callback_data": p+"oi_inc"}],
        # ── Funding Rate Filter ──
        [{"text": f"{fund_icon} Funding Rate Filter", "callback_data": p+"fund_toggle"}],
        [{"text": f"Max Funding Rate: {cfg.get('funding_rate_limit', 0.05)}%", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"fund_dec"}, {"text": "➕", "callback_data": p+"fund_inc"}],
        # ── BTC Trend Filter ──
        [{"text": f"{btc_icon} BTC Trend Filter", "callback_data": p+"btc_toggle"}],
        [{"text": f"Timeframe: {cfg.get('btc_trend_timeframe', '4h')}", "callback_data": p+"btc_tf"}],
        # ── News Filter ──
        [{"text": f"{news_icon} News Filter", "callback_data": p+"news_toggle"}],
        [{"text": f"Before News: {cfg.get('news_before_minutes', 30)} min", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"news_before_dec"}, {"text": "➕", "callback_data": p+"news_before_inc"}],
        [{"text": f"After News: {cfg.get('news_after_minutes', 30)} min", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"news_after_dec"}, {"text": "➕", "callback_data": p+"news_after_inc"}],
        # ── EMA Filter (Task 2) ──
        [{"text": f"{ema_icon} EMA Filter ({ema_label})", "callback_data": p+"ema_toggle"}],
        [{"text": "➖ Period", "callback_data": p+"ema_period_dec"}, {"text": "➕ Period", "callback_data": p+"ema_period_inc"}],
        [{"text": "🔀 Direction (Above/Below) [Manual mode]", "callback_data": p+"ema_dir_toggle"}],
        [{"text": f"🔀 EMA Mode: {cfg.get('ema_filter_mode','manual').capitalize()} (tap for Manual/Auto)", "callback_data": p+"ema_mode_toggle"}],
        # ── Auto EMA Trend Filter: ATR-based neutral zone (only used when mode=Auto) ──
        [{"text": f"{ema_auto_atr_icon} Auto EMA ATR Neutral Zone", "callback_data": p+"ema_auto_atr_toggle"}],
        [{"text": f"Auto ATR Multiplier: {cfg.get('ema_filter_auto_atr_multiplier', 1.0)}", "callback_data": "noop"}],
        [{"text": "➖ ATR Mult", "callback_data": p+"ema_auto_atr_dec"}, {"text": "➕ ATR Mult", "callback_data": p+"ema_auto_atr_inc"}],
        # ── ADX Maximum (Task 2) ──
        [{"text": f"ADX Max: {cfg.get('adx_max') if cfg.get('adx_max') is not None else 'off'}", "callback_data": "noop"}],
        [{"text": "➖ ADX Max", "callback_data": p+"adxmax_dec"}, {"text": "➕ ADX Max", "callback_data": p+"adxmax_inc"}, {"text": "🚫 Off", "callback_data": p+"adxmax_off"}],
        # ── ATR Range Filter (Task 2) — min/max ATR as % of price ──
        [{"text": f"{atr_range_icon} ATR Range Filter", "callback_data": p+"atrrange_toggle"}],
        [{"text": f"ATR Min%: {cfg.get('atr_min_pct') if cfg.get('atr_min_pct') is not None else 'off'}", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"atrmin_dec"}, {"text": "➕", "callback_data": p+"atrmin_inc"}, {"text": "🚫 Off", "callback_data": p+"atrmin_off"}],
        [{"text": f"ATR Max%: {cfg.get('atr_max_pct') if cfg.get('atr_max_pct') is not None else 'off'}", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"atrmax_dec"}, {"text": "➕", "callback_data": p+"atrmax_inc"}, {"text": "🚫 Off", "callback_data": p+"atrmax_off"}],
        # ── Volume Filter: SMA20 / EMA20 option (Task 2) ──
        [{"text": f"{volma_icon} Volume Filter (>{cfg.get('volume_ma_type','sma').upper()}20)", "callback_data": p+"volma_toggle"}],
        [{"text": "🔀 SMA20/EMA20", "callback_data": p+"volma_type_toggle"}],
        # ── AI Trade Intelligence (ATIE) — NEW: post-signal trade-management monitor ──
        [{"text": f"{atie_icon} AI Trade Intelligence", "callback_data": p+"atie_toggle"}],
        # ── ATR Stop Buffer (SL: exact reference vs. reference +/- ATR) ──
        [{"text": f"{atr_sl_icon} ATR Stop Buffer (OFF = exact SL reference)", "callback_data": p+"atrsl_toggle"}],
        [{"text": f"ATR Stop Multiplier: {cfg.get('atr_stop_multiplier', 1.5)}", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"atrsl_dec"}, {"text": "➕", "callback_data": p+"atrsl_inc"}],
        # ── Global Breakout Validation (min body % outside zone) ──
        [{"text": f"Breakout Min Body %: {int(cfg.get('breakout_min_body_ratio', 0.6) * 100)}%", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": p+"bkmb_dec"}, {"text": "➕", "callback_data": p+"bkmb_inc"}],
    ]
    # ── HB-only score-component thresholds (audit fix: previously hardcoded
    # inside strategy_hb(), now genuinely configurable per-strategy) ──
    if name == "HB":
        rows += [
            [{"text": f"HB Zone Tolerance: {cfg.get('hb_zone_tolerance', 0.008)*100:.2f}%", "callback_data": "noop"}],
            [{"text": "➖", "callback_data": p+"hbzone_dec"}, {"text": "➕", "callback_data": p+"hbzone_inc"}],
            [{"text": f"HB Spike Dominance: {cfg.get('hb_spike_dominance_multiplier', 3.0)}x avg body", "callback_data": "noop"}],
            [{"text": "➖", "callback_data": p+"hbspike_dec"}, {"text": "➕", "callback_data": p+"hbspike_inc"}],
            [{"text": f"HB Tight Touch Ratio: {cfg.get('hb_tight_level_touch_ratio', 0.5)}", "callback_data": "noop"}],
            [{"text": "➖", "callback_data": p+"hbtight_dec"}, {"text": "➕", "callback_data": p+"hbtight_inc"}],
        ]
    rows.append([{"text": "◀️ Back to strategy list", "callback_data": "strategy_settings_menu"}])
    return {"inline_keyboard": rows}

def strategies_kb(state):
    disabled = state.get("disabled_strategies", [])
    rows = []
    for name, _ in STRATEGIES:
        icon = "🔴" if name in disabled else "🟢"
        rows.append([{"text": f"{icon} {name}", "callback_data": f"ts:{name}"}])
    rows.append([{"text": "◀️ Back", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}

def adaptive_kb(state):
    enabled = state.get("adaptive_ranking_enabled", True)
    icon = "🟢" if enabled else "🔴"
    rows = [
        [{"text": f"{icon} Adaptive Ranking: {'ON' if enabled else 'OFF'}", "callback_data": "ar_toggle"}],
        [{"text": "🔄 Recalculate Weights Now", "callback_data": "ar_reweight_now"}],
        [{"text": "📋 View Current Weights", "callback_data": "ar_show_weights"}],
        [{"text": "◀️ Back", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def regime_kb(state):
    enabled = state.get("regime_engine_enabled", True)
    icon = "🟢" if enabled else "🔴"
    rows = [
        [{"text": f"{icon} Market Regime Engine: {'ON' if enabled else 'OFF'}", "callback_data": "re_toggle"}],
        [{"text": "📋 Strategy Classification", "callback_data": "re_show_types"}],
        [{"text": "◀️ Back", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def quality_kb(state):
    enabled = state.get("quality_ranking_enabled", True)
    icon = "🟢" if enabled else "🔴"
    rows = [
        [{"text": f"{icon} Quality Ranking: {'ON' if enabled else 'OFF'}", "callback_data": "qr_toggle"}],
        [{"text": "🏆 View Current Top 20 Symbols", "callback_data": "qr_show_top"}],
        [{"text": "◀️ Back", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def bl_kb(bl):
    rows = [[{"text": f"❌ {s}", "callback_data": f"blr:{s}"}] for s in sorted(bl)]
    rows.append([{"text": "◀️ Back", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}

def channels_kb(state):
    rows = []
    for ch in state.get("channels", []):
        icon = "🟢" if ch.get("active", True) else "🔴"
        title = ch.get("title", str(ch["id"]))
        rows.append([{"text": f"{icon} {title}", "callback_data": f"ch_toggle:{ch['id']}"},
                     {"text": "🗑", "callback_data": f"ch_remove:{ch['id']}"}])
    rows.append([{"text": "➕ Add New Channel/Group", "callback_data": "ch_add"}])
    rows.append([{"text": "◀️ Back", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}

def admins_kb(state):
    rows = []
    admins = state.get("admins", {})
    first_id = list(admins.keys())[0] if admins else None
    for aid, info in admins.items():
        tag = " 👑" if aid == first_id else ""
        rows.append([{"text": f"👤 {info.get('name', aid)}{tag}", "callback_data": f"admin_view:{aid}"}])
    rows.append([{"text": "➕ Add New Admin", "callback_data": "admin_add"}])
    rows.append([{"text": "◀️ Back", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}

def admin_detail_kb(state, aid):
    admins = state.get("admins", {})
    first_id = list(admins.keys())[0] if admins else None
    info = admins.get(aid, {})
    perms = info.get("perms", [])
    rows = []
    for pkey, pname in PERMISSIONS.items():
        icon = "🟢" if pkey in perms else "🔴"
        rows.append([{"text": f"{icon} {pname}", "callback_data": f"admin_perm:{aid}:{pkey}"}])
    if aid != first_id:
        rows.append([{"text": "🗑 Remove This Admin", "callback_data": f"admin_remove:{aid}"}])
    rows.append([{"text": "◀️ Back", "callback_data": "admins_menu"}])
    return {"inline_keyboard": rows}

async def handle_update(session, update, state):

    # ── Bot added to or removed from a channel/group ──
    if "my_chat_member" in update:
        cm = update["my_chat_member"]
        chat = cm["chat"]
        new_status = cm["new_chat_member"]["status"]  # member/administrator/left/kicked
        chat_id = chat["id"]
        title = chat.get("title", str(chat_id))
        channels = state.get("channels", [])
        existing = next((c for c in channels if c["id"] == chat_id), None)

        if new_status in ("administrator", "member"):
            if not existing:
                channels.append({"id": chat_id, "title": title, "active": True})
                state["channels"] = channels
                save_state(state)
                log.info(f"Channel registered: {title} ({chat_id}) status={new_status}")
                # به همه ادمین‌ها اطلاع بده
                for aid in state.get("admins", {}):
                    await send_msg(session, aid,
                        f"✅ The bot has been added to <b>{title}</b> and signals will now be sent there too.\n"
                        f"If the bot isn't an admin yet, make sure to give it admin access (send messages).")
            elif existing and new_status == "member":
                existing["title"] = title
                save_state(state)
        elif new_status in ("left", "kicked"):
            state["channels"] = [c for c in channels if c["id"] != chat_id]
            save_state(state)
            log.info(f"Channel removed: {title} ({chat_id})")
        return

    # ── نرمال‌سازی: در کانال‌ها، تلگرام پیام را زیر کلید "channel_post" می‌فرستد،
    # نه "message". با کپی کردن آن زیر همان کلید "message"، کل منطق پایین
    # (که خودش chat.type == "channel" را هم تشخیص می‌دهد) بدون تغییر کار می‌کند. ──
    if "channel_post" in update and "message" not in update:
        update = dict(update)
        update["message"] = update["channel_post"]

    if "message" in update:
        msg  = update["message"]
        uid  = msg.get("from", {}).get("id")
        cid  = msg["chat"]["id"]
        chat_type = msg["chat"].get("type", "private")
        text = msg.get("text", "")

        # ── پیام در کانال/گروه (نه چت خصوصی) ──
        if chat_type in ("group", "supergroup", "channel"):
            channels = state.get("channels", [])
            existing = next((c for c in channels if c["id"] == cid), None)
            msg_id = msg.get("message_id")
            title = msg["chat"].get("title", str(cid))

            # ── ثبت خودکار اگر هنوز ثبت نشده (پشتیبان برای my_chat_member که ممکنه از دست رفته باشه) ──
            if text in ("/start", "/stop") and not existing:
                existing = {"id": cid, "title": title, "active": True}
                channels.append(existing)
                state["channels"] = channels
                save_state(state)
                log.info(f"Channel auto-registered via command: {title} ({cid})")

            if text == "/stop" and existing:
                existing["active"] = False
                save_state(state)
                await react_to_message(session, cid, msg_id, "❤️")
                await send_msg(session, cid,
                    f"🔴 Signal transmission in this channel/group has stopped.\n"
                    f"Type /start to restart.\n\n"
                    f"🐍 AFEE TRADER — Version  {BOT_VERSION}")
                return
            if text == "/start" and existing:
                existing["active"] = True
                save_state(state)
                await react_to_message(session, cid, msg_id, "❤️")
                await send_msg(session, cid,
                    f"🟢 Signal transmission has been enabled in this channel/group.\n\n"
                    f"🐍 AFEE TRADER — Version  {BOT_VERSION}")
                return
            return  # سایر پیام‌های گروه/کانال نادیده گرفته میشه

        # ── چت خصوصی ──
        if text == "/start":
            admins = state.get("admins", {})
            if not admins:
                # FIX (Pass 3 audit, critical severity): previously, WHOEVER sent
                # /start first — before the real owner ever did — silently
                # became the permanent super-admin with every permission
                # (scan on/off, blacklist, admins, channels, logs). Since this
                # bot broadcasts to a public channel (@AFEETRADER per the
                # startup message), anyone who finds it and DMs /start first —
                # e.g. right after a redeploy that resets bot_state.json, or
                # simply before the owner gets around to it — could seize full
                # control with zero authentication. If OWNER_TELEGRAM_ID is
                # configured, only that Telegram user ID may bootstrap as
                # super-admin; anyone else is refused instead of silently
                # granted root.
                if OWNER_TELEGRAM_ID and str(uid) != OWNER_TELEGRAM_ID:
                    log.warning(
                        f"Blocked super-admin bootstrap attempt from uid={uid} "
                        f"(expected OWNER_TELEGRAM_ID={OWNER_TELEGRAM_ID})"
                    )
                    await send_msg(session, cid, "⛔️ You don't have access."); return
                if not OWNER_TELEGRAM_ID:
                    log.warning(
                        f"OWNER_TELEGRAM_ID is not set — granting super-admin to the FIRST "
                        f"uid={uid} to send /start. Set OWNER_TELEGRAM_ID to lock this down."
                    )
                # اولین نفر = سوپر ادمین
                name = msg.get("from", {}).get("first_name", "Admin")
                admins[str(uid)] = {"name": name, "perms": ALL_PERMS.copy()}
                state["admins"] = admins
                save_state(state)
                log.info(f"Super admin registered: {uid}")
            if not is_admin(uid, state):
                await send_msg(session, cid, "⛔️ You don't have access."); return
            await send_msg(session, cid,
                f"🐍 <b>AFEE TRADER - Control Panel</b>\n\n"
                f"🔖 Version: {BOT_VERSION}\n"
                f"🕒 {iran_time_str()}\n"
                f"📡 Scan: {'ON ✅' if state['scanning'] else 'OFF ❌'}",
                reply_markup=main_menu_kb(uid, state)); return

        if not is_admin(uid, state): return

        # ── منتظر اسم ارز برای تحلیلگر شخصی ──
        if state.get("pending_analysis") == uid and text and not text.startswith("/"):
            state["pending_analysis"] = None
            save_state(state)
            wait_msg = await send_msg(session, cid, f"⏳ Analyzing {text.strip().upper()}...")
            wait_msg_id = wait_msg.get("result", {}).get("message_id") if wait_msg else None

            symbol = await normalize_and_validate_symbol(session, text)
            if not symbol:
                err_text = f"❌ Symbol \"{text.strip()}\" not found on Binance. Re-enter the name without typos (e.g. BTC or BTCUSDT)."
                if wait_msg_id:
                    await edit_msg(session, cid, wait_msg_id, err_text)
                else:
                    await send_msg(session, cid, err_text)
                return

            report = await analyze_symbol_manually(session, symbol, state)
            if wait_msg_id:
                await edit_msg(session, cid, wait_msg_id, report,
                    reply_markup={"inline_keyboard": [
                        [{"text": "🔍 Analyze another coin", "callback_data": "analyzer_start"}],
                        [{"text": "◀️ Back to menu", "callback_data": "main_menu"}],
                    ]})
            else:
                await send_msg(session, cid, report)
            return

        # ── منتظر اسم ارز برای Replay Mode ──
        if state.get("pending_replay") == uid and text and not text.startswith("/"):
            state["pending_replay"] = None
            save_state(state)
            raw_sym = text.strip().upper().replace(" ", "")
            sym = raw_sym if raw_sym.endswith("USDT") else raw_sym + "USDT"
            wait_msg = await send_msg(session, cid, f"⏳ Retrieving history for {sym}...")
            wait_msg_id = wait_msg.get("result", {}).get("message_id") if wait_msg else None

            try:
                report = build_replay_report(sym)
            except Exception as e:
                log.error(f"Replay (pending) error: {e}")
                report = "Error retrieving history."

            if wait_msg_id:
                await edit_msg(session, cid, wait_msg_id, report,
                    reply_markup={"inline_keyboard": [
                        [{"text": "🎬 Check another coin", "callback_data": "replay_start"}],
                        [{"text": "◀️ Back to menu", "callback_data": "main_menu"}],
                    ]})
            else:
                await send_msg(session, cid, report)
            return

        # ── Task 3: waiting for a typed number after opening a strategy in
        # the /setscore panel ──
        _pend_sc = state.get("pending_setscore")
        if _pend_sc and _pend_sc.get("uid") == uid and text and not text.startswith("/"):
            idx = _pend_sc.get("idx")
            state["pending_setscore"] = None
            if idx is None or not (0 <= idx < len(STRATEGIES)):
                save_state(state)
                await send_msg(session, cid, "❌ That strategy selection expired. Please use /setscore again.")
                return
            name, _fn = STRATEGIES[idx]
            try:
                new_sc = int(text.strip())
                if not (1 <= new_sc <= 100):
                    raise ValueError("out of range")
                set_strategy_min_score(state, name, new_sc)
                save_state(state)
                relaxed = 86 <= new_sc < 95
                strict = new_sc >= 95
                await send_msg(session, cid,
                    f"✅ Minimum Score for <b>{name}</b> set to <b>{new_sc}/100</b>.\n"
                    + (f"🔓 Reminder: the threshold you set is between 86 and 94, but the relaxation of the ADX/EMA/Volume filters is applied based on each signal's own score (>= 85), not based on this threshold."
                       if relaxed else "")
                    + (f"\n🔒 Strict mode active: since the score is >= 95, no relaxation is applied to the final Score threshold for this strategy (only signals with an actual score >= {new_sc} pass). Note: this does not make the other filters (ADX/Volume/ATR, etc.) stricter — they are still relaxed based on the signal's own score (>= 85)."
                       if strict else ""),
                    reply_markup=setscore_menu_kb(state))
                log.info(f"Per-strategy min_signal_score set | strategy={name} value={new_sc} by admin {uid}")
            except ValueError:
                save_state(state)
                await send_msg(session, cid,
                    f"❌ Invalid number for <b>{name}</b>. Must be between 1 and 100. Please use /setscore again.")
            return

        # ── Waiting for a forwarded message to add an admin ──
        if state.get("pending_admin_add") == uid and msg.get("forward_from"):
            fwd = msg["forward_from"]
            new_id = str(fwd["id"])
            name = fwd.get("first_name", new_id)
            admins = state.get("admins", {})
            if new_id not in admins:
                admins[new_id] = {"name": name, "perms": []}
                state["admins"] = admins
            state["pending_admin_add"] = None
            save_state(state)
            await send_msg(session, cid, f"✅ <b>{name}</b> added as an admin (no permissions).\nSet their permissions from the Admins menu.")
            return

        # ── Config import: file upload (Method A: after /importconfig, or ──
        # ── Method B: an unsolicited upload of any JSON file) — Super Admin only ──
        if msg.get("document"):
            doc = msg["document"]
            fname = doc.get("file_name") or ""
            mime_type = doc.get("mime_type") or ""
            waiting_for_it = uid in _PENDING_IMPORT_WAIT
            looks_like_json = fname.lower().endswith(".json") or mime_type == "application/json"
            if waiting_for_it or looks_like_json:
                if not is_super_admin(uid, state):
                    # Only the Super Admin may import configuration.
                    if waiting_for_it:
                        _PENDING_IMPORT_WAIT.discard(uid)
                    return
                _PENDING_IMPORT_WAIT.discard(uid)
                file_size = doc.get("file_size") or 0
                if file_size and file_size > CONFIG_IMPORT_MAX_BYTES:
                    await send_msg(session, cid, "❌ File too large. Maximum allowed size is 1 MB.")
                    return
                content = await download_telegram_file(session, doc.get("file_id"))
                if content is None:
                    await send_msg(session, cid, "❌ Could not download the file. Please try again.")
                    return
                if len(content) > CONFIG_IMPORT_MAX_BYTES:
                    await send_msg(session, cid, "❌ File too large. Maximum allowed size is 1 MB.")
                    return

                if not waiting_for_it:
                    # Method B (auto-detect): this upload wasn't an explicit import
                    # request, so only surface the confirmation prompt if the JSON
                    # actually passes the existing configuration validation — any
                    # other JSON file is ignored silently. The real, authoritative
                    # validation (used by both methods) still runs again in
                    # finalize_config_import() after the Super Admin taps Yes.
                    try:
                        precheck = json.loads(content.decode("utf-8"))
                        is_valid_config = isinstance(precheck, dict) and all(
                            k in precheck for k in CONFIG_IMPORT_REQUIRED_KEYS)
                    except Exception:
                        is_valid_config = False
                    if not is_valid_config:
                        return

                _PENDING_IMPORT_DATA[uid] = content
                await send_msg(session, cid,
                    "⚠️ <b>Replace the current configuration?</b>\n\n"
                    "This will overwrite your current bot_config.json (a backup is kept automatically).",
                    reply_markup={"inline_keyboard": [
                        [{"text": "✅ Yes", "callback_data": "importcfg_yes"},
                         {"text": "❌ No", "callback_data": "importcfg_no"}],
                    ]})
                return

        if text.startswith("/bl ") and has_permission(uid, "blacklist", state):
            sym = text.split()[1].upper()
            if not sym.endswith("USDT"): sym += "USDT"
            add_to_blacklist(sym)
            await send_msg(session, cid, f"✅ <b>{sym}</b> added to the blacklist.")
        elif text.startswith("/unbl ") and has_permission(uid, "blacklist", state):
            sym = text.split()[1].upper()
            if not sym.endswith("USDT"): sym += "USDT"
            bl = load_blacklist(); bl.discard(sym); save_blacklist(bl)
            await send_msg(session, cid, f"✅ <b>{sym}</b> removed from the blacklist.")
        elif text == "/logs" and has_permission(uid, "logs", state):
            try:
                with open("afee_bot.log", encoding="utf-8") as f:
                    lines = f.readlines()
                await send_msg(session, cid, f"<pre>{''.join(lines[-20:])[-3500:]}</pre>")
            except Exception:
                await send_msg(session, cid, "Log file not found.")
        elif text == "/stats" and has_permission(uid, "logs", state):
            await send_msg(session, cid, "⏳ Calculating advanced statistics...")
            try:
                report = build_stats_report()
                await send_msg(session, cid, report)
            except Exception as e:
                log.error(f"/stats error: {e}")
                await send_msg(session, cid, "Error calculating statistics. Check the log.")
        elif text == "/weights" and has_permission(uid, "scan_toggle", state):
            weights = get_all_strategy_weights()
            lines = ["📋 <b>Current Strategy Weights</b>", ""]
            for strat_name, _ in STRATEGIES:
                w = weights.get(strat_name, 1.0)
                tag = "📈 Boosted" if w > 1.0 else ("📉 Penalized" if w < 1.0 else "➖ Neutral")
                lines.append(f"▫️ {strat_name}: <b>{w:.2f}</b> ({tag})")
            lines.append("")
            # Task 3: minimum Score is now per-strategy — show each
            # strategy's OWN effective threshold instead of a single global
            # number (a strategy still shown without its own override is
            # using the legacy global fallback).
            lines.append("⭐️ <b>Minimum Score per strategy</b> (to change: /setscore):")
            for strat_name, _ in STRATEGIES:
                cfg = get_strategy_settings(state, strat_name)
                eff = get_strategy_min_score(state, strat_name)
                custom = cfg.get("min_signal_score") is not None
                lines.append(f"▫️ {strat_name}: <b>{eff}/100</b>{'' if custom else ' (default)'}")
            await send_msg(session, cid, "\n".join(lines))
        elif text.startswith("/override") and has_permission(uid, "scan_toggle", state):
            # Signal Override (feature): change ANY signal's result by its
            # Signal ID (e.g. /override 250803-03), regardless of whether
            # it's still pending or already has an automatic result — the
            # actual write goes through apply_manual_trade_result(), the
            # exact same path Pending Signal Management uses, so it's
            # processed identically to a real bot result and flagged
            # manual for audit.
            parts = text.split()
            if len(parts) < 2:
                await send_msg(session, cid, "Correct format: /override 250803-03")
            else:
                signal_uid = parts[1].strip().lstrip("#")
                sig = get_signal_by_uid(signal_uid)
                if not sig:
                    await send_msg(session, cid, f"❌ No signal found with ID \"{signal_uid}\".")
                else:
                    dir_label = "🟢 LONG" if sig["direction"] == "BUY" else "🔴 SHORT"
                    opened_str = (sig.get("opened_at") or "")[:16].replace("T", " ")
                    pnl_line = f"\n💹 <b>Current PnL:</b> {sig['pnl_percent']:.2f}%" if sig.get("pnl_percent") is not None else ""
                    text_out = (
                        f"🆔 <b>Signal ID</b>\n<code>{sig.get('signal_uid') or sig['id']}</code>\n\n"
                        f"💎 <b>Symbol:</b> {sig['symbol']}\n"
                        f"{dir_label}\n"
                        f"📊 <b>Strategy:</b> {sig['strategy']}\n"
                        f"🕒 <b>Time:</b> {opened_str} UTC\n"
                        f"📌 <b>Current status:</b> {sig['status']}"
                        f"{pnl_line}\n\n"
                        f"Choose the new result:"
                    )
                    await send_msg(session, cid, text_out,
                        reply_markup=manual_result_actions_kb(sig["id"], "main_menu"))

        elif text.startswith("/replay") and has_permission(uid, "logs", state):
            parts = text.split()
            if len(parts) < 2:
                await send_msg(session, cid, "Correct format: /replay BTCUSDT")
            else:
                raw_sym = parts[1].strip().upper()
                sym = raw_sym if raw_sym.endswith("USDT") else raw_sym + "USDT"
                await send_msg(session, cid, f"⏳ Retrieving history for {sym}...")
                try:
                    report = build_replay_report(sym)
                except Exception as e:
                    log.error(f"/replay error: {e}")
                    report = "Error retrieving history. Check the log."
                await send_msg(session, cid, report)

        # Task 3: /setscore is now a per-strategy interactive panel instead
        # of one global number — /setscore [number] (the old free-text
        # format) is intentionally no longer accepted; any trailing text is
        # ignored and the strategy picker is shown instead. Selecting a
        # strategy and typing a number is handled by the "sc_open:" callback
        # and the "pending_setscore" text-input block further down.
        elif text.startswith("/setscore") and has_permission(uid, "scan_toggle", state):
            await send_msg(session, cid,
                "⭐️ <b>Set Minimum Signal Score</b>\n\n"
                "Each strategy now has its own minimum score. Pick a strategy below, "
                "then send the new minimum score for it.\n\n"
                "💡 The higher a strategy's score is set, the higher-quality (fewer, "
                "stricter) signals it will send.\n"
                "Note: regardless of this threshold, any signal whose own score is >= 85 "
                "is checked more leniently by some filters (ADX/Volume/ATR, etc.); this "
                "depends on the signal's own score, not the threshold set here.",
                reply_markup=setscore_menu_kb(state))

        elif text.startswith("/diagon") and has_permission(uid, "scan_toggle", state):
            # Enables the temporary signal-funnel diagnostic instrumentation
            # (see the DIAGNOSTIC INSTRUMENTATION block near the top of this
            # file). Zero effect on trading logic — only adds in-memory
            # counters. Intended to be turned on right before a Historical
            # Replay run, then exported with /diagexport.
            set_diag_enabled(True)
            state["diagnostics_enabled"] = True
            await send_msg(session, cid,
                "🔬 Diagnostics ON. Run a Historical Replay now, then use "
                "/diagexport to get the funnel/score/ADX report, or /diagreset "
                "to clear counters and start a fresh measurement window.")

        elif text.startswith("/diagoff") and has_permission(uid, "scan_toggle", state):
            set_diag_enabled(False)
            state["diagnostics_enabled"] = False
            await send_msg(session, cid, "🔬 Diagnostics OFF.")

        elif text.startswith("/diagreset") and has_permission(uid, "scan_toggle", state):
            diag_reset()
            await send_msg(session, cid, "🔬 Diagnostic counters cleared.")

        elif text.startswith("/diagexport") and has_permission(uid, "logs", state):
            try:
                out_path = diag_export("/tmp/afee_diagnostics_report.json")
                await send_document(session, cid, out_path, filename="afee_diagnostics_report.json",
                                     caption="🔬 Signal-funnel diagnostics: per-strategy stage counts, "
                                             "raw score distribution, raw ADX distribution. "
                                             "Generated from whatever ran with /diagon enabled "
                                             "(a Historical Replay is the fastest way to accumulate "
                                             "a meaningful sample).")
            except Exception as e:
                await send_msg(session, cid, f"❌ Diagnostics export failed: {e}")

        elif text.startswith("/config") and has_permission(uid, "scan_toggle", state):
            # /config key value  — directly change bot_config.json from Telegram
            # Example: /config TOP_N_COINS 50
            #          /config DYNAMIC_SCORE_THRESHOLD_TREND 80
            parts_cfg = text.split(None, 2)
            if len(parts_cfg) < 3:
                cfg_now = load_bot_config()
                cfg_lines = ["⚙️ <b>bot_config.json (editable settings)</b>", ""]
                # Only show the small numeric config keys here — cfg_now may also
                # contain the rest of the bot's state (admins, channels, strategy
                # toggles, etc.) since bot_config.json is now the single unified
                # persistence file (Task 1); those are managed via their own menus,
                # not via /config, so we don't want to dump them all into a chat.
                for k in sorted(BOT_CONFIG_DEFAULTS.keys()):
                    v = cfg_now.get(k, BOT_CONFIG_DEFAULTS[k])
                    cfg_lines.append(f"• <code>{k}</code> = <b>{v}</b>")
                cfg_lines.append("")
                cfg_lines.append("To change: /config KEY VALUE")
                cfg_lines.append("Example: /config TOP_N_COINS 50")
                await send_msg(session, cid, "\n".join(cfg_lines))
            else:
                cfg_key = parts_cfg[1].strip().upper()
                cfg_val_str = parts_cfg[2].strip()
                valid_keys = {
                    "TOP_N_COINS", "QUALITY_POOL_SIZE",
                    "DYNAMIC_SCORE_THRESHOLD_TREND",
                    "DYNAMIC_SCORE_THRESHOLD_RANGE",
                    "DYNAMIC_SCORE_THRESHOLD_HIGH_VOLATILITY",
                    "DYNAMIC_SCORE_THRESHOLD_WHIPSAW",
                    "DYNAMIC_SCORE_THRESHOLD_TRANSITIONAL",
                }
                if cfg_key not in valid_keys:
                    await send_msg(session, cid,
                        f"❌ Invalid key: <code>{cfg_key}</code>\n"
                        f"Allowed keys:\n" + "\n".join(f"• <code>{k}</code>" for k in sorted(valid_keys)))
                else:
                    try:
                        cfg_val = int(cfg_val_str) if cfg_val_str.isdigit() else float(cfg_val_str)
                        cfg_now = load_bot_config()
                        cfg_now[cfg_key] = cfg_val
                        # FIX (Pass 3 audit, low severity): write atomically (tmp file +
                        # os.replace) instead of writing straight to bot_config.json, so a
                        # crash or power loss mid-write can't leave a half-written / corrupt
                        # config file. load_bot_config() already tolerates a corrupt file by
                        # falling back to defaults, but there's no reason to risk it when an
                        # atomic write is just as cheap.
                        import json as _json2
                        tmp_cfg_file = BOT_CONFIG_FILE + ".tmp"
                        with open(tmp_cfg_file, "w", encoding="utf-8") as _cf:
                            _json2.dump(cfg_now, _cf, indent=2, ensure_ascii=False)
                        os.replace(tmp_cfg_file, BOT_CONFIG_FILE)
                        # BUG FIX (random config reset, belt-and-suspenders): keep the
                        # in-memory `state` dict in sync too, so it can never hold a
                        # stale copy of this key that a later save_state() call (from
                        # an unrelated action) might otherwise persist back to disk.
                        state[cfg_key] = cfg_val
                        await send_msg(session, cid,
                            f"✅ <code>{cfg_key}</code> → <b>{cfg_val}</b> saved in bot_config.json\n"
                            f"The change is immediate — do not restart the bot.")
                        log.info(f"bot_config: {cfg_key}={cfg_val} set by admin {uid}")
                    except (ValueError, TypeError):
                        await send_msg(session, cid, f"❌ Invalid value: {cfg_val_str}. Must be a number.")
                    except Exception as e:
                        await send_msg(session, cid, f"❌ Error saving: {e}")

        elif text == "/exportconfig":
            if not is_super_admin(uid, state):
                await send_msg(session, cid, "⛔️ Super Admin only."); return
            if not os.path.exists(BOT_CONFIG_FILE):
                # Auto-create requirement: guarantee it exists before export too.
                load_bot_config()
            try:
                # Completeness (Section 2/6): guarantee every strategy has a
                # full settings dict — including strategies never opened in
                # the Strategy Settings UI, and any filter keys added since
                # the last time a strategy's panel was opened (e.g. ATR Stop
                # Buffer/Multiplier, Breakout Min Body %) — then flush the
                # current in-memory `state` straight to bot_config.json so
                # the exported file always matches exactly what the live
                # engine is using right now, not a possibly-stale on-disk
                # copy from before the most recent settings access.
                backfill_all_strategy_settings(state)
                save_state(state)
            except Exception as e:
                log.error(f"/exportconfig pre-export backfill/save failed: {e}")
            try:
                from datetime import timedelta as _timedelta
                _iran_tz = timezone(_timedelta(hours=3, minutes=30))
                _ts = datetime.now(_iran_tz).strftime("%Y%m%d_%H%M")
                export_filename = f"AFEE_Config_{_ts}.json"
                result = await send_document(session, cid, BOT_CONFIG_FILE,
                    filename=export_filename, caption="📤 Current bot configuration")
                if result and result.get("ok"):
                    await send_msg(session, cid, "✅ Configuration exported successfully.")
                else:
                    await send_msg(session, cid, "❌ Failed to send the configuration file.")
            except Exception as e:
                log.error(f"/exportconfig error: {e}")
                await send_msg(session, cid, "❌ Failed to export the configuration file.")

        elif text == "/importconfig":
            if not is_super_admin(uid, state):
                await send_msg(session, cid, "⛔️ Super Admin only."); return
            _PENDING_IMPORT_WAIT.add(uid)
            await send_msg(session, cid,
                f"📥 Please upload the configuration JSON file now to import it.")

        elif text.startswith("/backtest") and has_permission(uid, "logs", state):
            # /backtest [hours] [symbol1,symbol2,...]
            parts = text.split()
            hours = 24
            symbols_override = None
            if len(parts) >= 2:
                try:
                    hours = int(parts[1])
                except ValueError:
                    pass
            if len(parts) >= 3:
                symbols_override = [s.strip().upper() for s in parts[2].split(",")]
                symbols_override = [s if s.endswith("USDT") else s + "USDT" for s in symbols_override]

            await send_msg(session, cid, f"⏳ Starting backtest for the last {hours} hours...")
            try:
                if symbols_override:
                    symbols = symbols_override
                else:
                    symbols = await get_quality_ranked_symbols(session, get_top_n_coins(), get_quality_pool_size()) \
                        if state.get("quality_ranking_enabled", True) \
                        else await get_top_symbols(session, get_top_n_coins())
                    symbols = symbols[:50]

                job_params = {
                    "symbols": symbols,
                    "timerange_hours": hours,
                    "strategies": [s for s, _ in STRATEGIES],
                    "requested_by": str(uid),
                }
                job_id = await enqueue_backtest(session, job_params)
                # انتظار با timeout ۲۴۰ ثانیه
                for _ in range(120):
                    await asyncio.sleep(2)
                    if job_id in _backtest_cache:
                        break
                cached = _backtest_cache.get(job_id)
                if cached:
                    parts_msgs = build_backtest_report(cached["trades"], cached["summary"], hours)
                    for part in parts_msgs:
                        await send_msg(session, cid, part)
                        await asyncio.sleep(1)
                else:
                    await send_msg(session, cid, "⏳ The backtest is queued. Results will be sent shortly.")
            except Exception as e:
                log.error(f"/backtest error: {e}")
                await send_msg(session, cid, f"❌ Error: {e}")

        # ─── HISTORICAL STRATEGY REPLAY — trade inspection command ──────────
        # Deliberately NOT named /replay* — that prefix is already the
        # existing per-symbol signal-history command above (line ~8507) and
        # elif chains stop at the first startswith() match, so reusing it
        # would silently swallow this command into the wrong handler.
        elif text.startswith("/simtrades") and has_permission(uid, "logs", state):
            # /simtrades <BACKTEST_ID> [strategy] [symbol]
            parts = text.split(maxsplit=3)
            if len(parts) < 2:
                await send_msg(session, cid,
                    "Correct format: <code>/simtrades BT-20260815-001 [strategy] [symbol]</code>\n"
                    "Examples:\n"
                    "<code>/simtrades BT-20260815-001</code>\n"
                    "<code>/simtrades BT-20260815-001 \"Stop Hunter\"</code>\n"
                    "<code>/simtrades BT-20260815-001 \"Stop Hunter\" BTCUSDT</code>\n"
                    "<code>/simtrades BT-20260815-001 - BTCUSDT</code>")
            else:
                backtest_id = parts[1].strip()
                strat_arg = parts[2].strip().strip('"') if len(parts) >= 3 else None
                sym_arg = parts[3].strip().strip('"') if len(parts) >= 4 else None
                if strat_arg == "-":
                    strat_arg = None
                trades = _replay_query_trades(backtest_id, strategy=strat_arg, symbol=sym_arg)
                if not trades:
                    await send_msg(session, cid, f"No trades found for <code>{backtest_id}</code> "
                                                   f"(check the ID and filters).")
                else:
                    await send_msg(session, cid,
                        f"<b><code>{backtest_id}</code></b> — {len(trades)} trade(s)"
                        f"{' · ' + strat_arg if strat_arg else ''}{' · ' + sym_arg if sym_arg else ''}")
                    for t in trades[:25]:
                        kb = {"inline_keyboard": [[{"text": "📊 Show Chart",
                                                     "callback_data": f"replay_chart:{t['id']}"}]]}
                        await send_telegram(session, _replay_format_trade(t), cid, reply_markup=kb)
                    if len(trades) > 25:
                        await send_msg(session, cid,
                            f"...and {len(trades) - 25} more. Narrow with a strategy/symbol filter.")

    elif "callback_query" in update:
        cb   = update["callback_query"]
        uid  = cb["from"]["id"]
        cid  = cb["message"]["chat"]["id"]
        mid  = cb["message"]["message_id"]
        data = cb.get("data", "")

        # ── AI CHART ANALYSIS — "View Chart Analysis" button (additive) ───
        # NECESSARY, SCOPED EXCEPTION to the admin gate below: this button
        # is sent on every public signal, to every channel/group subscriber
        # (see scan_once()) — not just to admins/operators of the bot's
        # control panel. Every other callback in this handler is an
        # admin-panel action and stays gated exactly as before; only this
        # one read-only, harmless action (replay a previously-sent chart)
        # is exempted, because gating it here would silently break the
        # entire feature for its actual intended audience.
        if data.startswith("chartreplay:"):
            await answer_callback(session, cb["id"])
            try:
                signal_id = int(data.split(":", 1)[1])
            except (ValueError, IndexError):
                signal_id = None
            photo_bytes = _analysis_replay.render_replay(signal_id, db_path=DB_FILE) if (CHART_FEATURE_AVAILABLE and signal_id) else None
            if photo_bytes:
                try:
                    await send_telegram_photo(session, photo_bytes,
                        f"📈 <b>Chart Analysis Replay</b>\nOriginal signal snapshot — not current market data.",
                        chat_id=cid)
                except Exception as e:
                    log.error(f"chartreplay send failed for signal_id={signal_id}: {e}")
                    await send_msg(session, cid, "⚠️ Could not send the chart replay. Please try again later.")
            else:
                # FAILSAFE (per spec): missing/unavailable snapshot must
                # never crash or interrupt the Telegram interaction.
                await send_msg(session, cid,
                    "⚠️ The analysis snapshot for this signal is unavailable "
                    "(chart replay may not have been enabled when this signal was sent).")
            return

        # ── AI CHART ANALYSIS — "📸 Live Chart Update" button (additive) ──
        # Same scoped admin-gate exception as chartreplay: above, and same
        # posture — this is a read-only, visualization-only action available
        # to every signal recipient. It downloads the LATEST candles on the
        # signal's original entry timeframe (via the bot's existing,
        # unmodified get_candles()) and re-renders ONLY the chart — it never
        # re-runs strategy logic, never recalculates entry/stop/tp1/tp2/
        # score, never touches the database, and never sends a new signal or
        # modifies the original message. The result is sent as a reply to
        # the ORIGINAL signal text message (same tg_message_id mechanism
        # send_outcome_reply() already uses for TP/SL/MISSED notifications).
        if data.startswith("livechart:"):
            await answer_callback(session, cb["id"])
            try:
                signal_id = int(data.split(":", 1)[1])
            except (ValueError, IndexError):
                signal_id = None

            photo_bytes = None
            snapshot = None
            if CHART_FEATURE_AVAILABLE and signal_id:
                try:
                    snapshot = _analysis_snapshot.load_snapshot(signal_id, db_path=DB_FILE)
                    if snapshot:
                        fresh_candles = await get_candles(
                            session, snapshot.get("symbol"), snapshot.get("entry_timeframe"), limit=200)
                        if fresh_candles:
                            photo_bytes = _chart_renderer.render_live_chart(snapshot, fresh_candles)
                except Exception as e:
                    log.error(f"livechart render failed for signal_id={signal_id}: {e}")
                    photo_bytes = None

            reply_target_message_id = None
            if signal_id:
                notif = get_signal_notification_row(signal_id)
                if notif:
                    reply_target_message_id = notif.get("tg_message_id")

            if photo_bytes:
                id_line = f"🆔 <code>{snapshot.get('signal_uid')}</code>\n" if snapshot and snapshot.get("signal_uid") else ""
                try:
                    await send_telegram_photo(session, photo_bytes,
                        f"📸 <b>Live Chart Update</b>\n{id_line}Current market vs. the original signal levels.",
                        chat_id=cid, reply_to_message_id=reply_target_message_id)
                except Exception as e:
                    log.error(f"livechart send failed for signal_id={signal_id}: {e}")
                    await send_msg(session, cid, "⚠️ Could not send the live chart update. Please try again later.")
            else:
                # FAILSAFE (per spec): this is visualization-only housekeeping —
                # a missing snapshot or a fetch/render failure must never crash
                # or interrupt the Telegram interaction, and must never fall
                # back to re-running strategy logic.
                await send_msg(session, cid,
                    "⚠️ Live chart update is unavailable for this signal right now.")
            return

        if not is_admin(uid, state):
            await answer_callback(session, cb["id"], "⛔️ You don't have access"); return

        await answer_callback(session, cb["id"])

        if data == "main_menu":
            await edit_msg(session, cid, mid,
                f"🐍 <b>AFEE TRADER - Control Panel</b>\n🔖 Version: {BOT_VERSION}\n🕒 {iran_time_str()}",
                reply_markup=main_menu_kb(uid, state))

        elif data == "toggle_scan" and has_permission(uid, "scan_toggle", state):
            state["scanning"] = not state["scanning"]
            save_state(state)
            await edit_msg(session, cid, mid,
                f"📡 Scan: <b>{'ON ✅' if state['scanning'] else 'OFF ❌'}</b>",
                reply_markup=main_menu_kb(uid, state))

        elif data == "toggle_backtest" and has_permission(uid, "scan_toggle", state):
            state["backtest_enabled"] = not state.get("backtest_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid,
                f"🧪 Auto Backtest: <b>{'ON ✅' if state['backtest_enabled'] else 'OFF ❌'}</b>\n\n"
                f"When on, every signal issued is recorded and its result is checked in the 00:00 daily report.",
                reply_markup=main_menu_kb(uid, state))

        elif data == "toggle_volume_filter" and has_permission(uid, "scan_toggle", state):
            state["volume_filter_enabled"] = not state.get("volume_filter_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid,
                f"🛡 Suspicious Volume Filter: <b>{'ON ✅' if state['volume_filter_enabled'] else 'OFF ❌'}</b>\n\n"
                f"When on, if a strong high-volume move exactly against the signal is seen before the signal is issued, that signal is rejected (to reduce stop-outs).",
                reply_markup=main_menu_kb(uid, state))

        elif data == "strategy_settings_menu" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid,
                "🧩 <b>Strategy Settings</b>\n\n"
                "Pick a strategy to view and change its own filters.",
                reply_markup=strategy_settings_menu_kb())

        # ── Task 3: /setscore panel ──────────────────────────────────────
        elif data == "setscore_menu" and has_permission(uid, "scan_toggle", state):
            # Cancel any pending numeric-entry from a previously-opened
            # strategy card so an old prompt can't be answered against the
            # wrong strategy after navigating back.
            if state.get("pending_setscore", {}) and state["pending_setscore"].get("uid") == uid:
                state["pending_setscore"] = None
                save_state(state)
            await edit_msg(session, cid, mid,
                "⭐️ <b>Set Minimum Signal Score</b>\n\n"
                "Each strategy now has its own minimum score. Pick a strategy below, "
                "then send the new minimum score for it.",
                reply_markup=setscore_menu_kb(state))

        elif data.startswith("sc_open:") and has_permission(uid, "scan_toggle", state):
            idx = int(data.split(":")[1])
            name, _ = STRATEGIES[idx]
            current = get_strategy_min_score(state, name)
            state["pending_setscore"] = {"uid": uid, "idx": idx}
            save_state(state)
            await edit_msg(session, cid, mid,
                f"⭐️ <b>{name}</b>\n\n"
                f"Current minimum score: <b>{current}/100</b>\n\n"
                f"Enter the new minimum score:\n"
                f"(reply with a number between 1 and 100)",
                reply_markup={"inline_keyboard": [
                    [{"text": "◀️ Back", "callback_data": "setscore_menu"}],
                ]})

        elif data.startswith("sf_open:") and has_permission(uid, "scan_toggle", state):
            idx = int(data.split(":")[1])
            name, _ = STRATEGIES[idx]
            await edit_msg(session, cid, mid,
                f"🧩 <b>Strategy Settings: {name}</b>\n\n"
                "ADX/ATR/Session: this strategy's advanced filters\n"
                "Entry candle volume filter: Volume ≥ Multiplier × SMA(Volume, Period)\n"
                "Open Interest / Funding Rate / BTC Trend / News: filters specific to this strategy",
                reply_markup=strategy_settings_detail_kb(state, idx))

        elif data == "noop":
            pass

        elif data == "adaptive_menu" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid,
                "⚖️ <b>Adaptive Strategy Ranking</b>\n\n"
                "Every 7 days, each strategy's performance over the last 30 days (Win Rate + Expectancy + Profit Factor) is reviewed "
                "and it's assigned a weight between 0.5 (penalty) and 1.5 (boost). This weight is multiplied directly into that strategy's final signal Score.\n\n"
                f"Minimum closed trades needed for a real weight: {MIN_TRADES_FOR_WEIGHTING} closed trades (otherwise the neutral weight 1.0 is kept).",
                reply_markup=adaptive_kb(state))

        elif data == "ar_toggle" and has_permission(uid, "scan_toggle", state):
            state["adaptive_ranking_enabled"] = not state.get("adaptive_ranking_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid, "⚖️ <b>Adaptive Strategy Ranking</b>", reply_markup=adaptive_kb(state))

        elif data == "ar_reweight_now" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid, "⏳ Recalculating weights...")
            try:
                old_weights = get_all_strategy_weights()
                new_weights = update_all_strategy_weights()
                state["last_reweight_date"] = datetime.now(timezone.utc).isoformat()
                save_state(state)
                report = build_weight_update_report(old_weights, new_weights)
            except Exception as e:
                log.error(f"Manual reweight error: {e}")
                report = "Error recalculating weights."
            await edit_msg(session, cid, mid, report,
                reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "adaptive_menu"}]]})

        elif data == "ar_show_weights" and has_permission(uid, "scan_toggle", state):
            weights = get_all_strategy_weights()
            lines = ["📋 <b>Current Strategy Weights</b>", ""]
            for strat_name, _ in STRATEGIES:
                w = weights.get(strat_name, 1.0)
                tag = "📈 Boosted" if w > 1.0 else ("📉 Penalized" if w < 1.0 else "➖ Neutral")
                lines.append(f"▫️ {strat_name}: <b>{w:.2f}</b> ({tag})")
            await edit_msg(session, cid, mid, "\n".join(lines),
                reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "adaptive_menu"}]]})

        elif data == "regime_menu" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid,
                "🌐 <b>Market Regime Engine</b>\n\n"
                "Before each strategy runs, the overall market regime (ADX, EMA200 Slope, ATR Regime, Volatility Regime, Volume Regime) "
                "is checked:\n\n"
                "• ADX above 25 → only trend (TREND) and hybrid strategies run\n"
                "• ADX below 20 → only reversal (REVERSAL) and hybrid strategies run\n"
                "• Between 20 and 25 → all strategies are allowed (transitional zone)",
                reply_markup=regime_kb(state))

        elif data == "re_toggle" and has_permission(uid, "scan_toggle", state):
            state["regime_engine_enabled"] = not state.get("regime_engine_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid, "🌐 <b>Market Regime Engine</b>", reply_markup=regime_kb(state))

        elif data == "re_show_types" and has_permission(uid, "scan_toggle", state):
            lines = ["📋 <b>Strategy Classification</b>", ""]
            type_fa = {"TREND": "Trend 📈", "REVERSAL": "Reversal 🔄", "HYBRID": "Hybrid ⚖️"}
            for strat_name, _ in STRATEGIES:
                t = STRATEGY_REGIME_TYPE.get(strat_name, "HYBRID")
                lines.append(f"▫️ {strat_name}: <b>{type_fa.get(t, t)}</b>")
            await edit_msg(session, cid, mid, "\n".join(lines),
                reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "regime_menu"}]]})

        elif data == "quality_menu" and has_permission(uid, "scan_toggle", state):
            _qr_top_n = get_top_n_coins()
            _qr_pool  = get_quality_pool_size()
            await edit_msg(session, cid, mid,
                "🏆 <b>Symbol Quality Ranking</b>\n\n"
                f"Instead of choosing purely by trading volume, the top {_qr_pool} symbols by volume are chosen first; "
                f"then a quality score is calculated for those same candidates and only the top {_qr_top_n} by quality are chosen for the final scan.\n\n"
                "Quality score criteria:\n"
                "• Spread Quality (20%) — bid/ask distance\n"
                "• Volatility Quality (25%) — neither too flat nor too volatile\n"
                "• Structure Cleanliness (30%) — number of valid S/R levels\n"
                "• Trend Clarity (25%) — trend or range clarity based on ADX",
                reply_markup=quality_kb(state))

        elif data == "qr_toggle" and has_permission(uid, "scan_toggle", state):
            state["quality_ranking_enabled"] = not state.get("quality_ranking_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid, "🏆 <b>Symbol Quality Ranking</b>", reply_markup=quality_kb(state))

        elif data == "qr_show_top" and has_permission(uid, "scan_toggle", state):
            _qr_pool2 = get_quality_pool_size()
            await edit_msg(session, cid, mid, f"⏳ Calculating quality for {_qr_pool2} symbols... (this may take a moment)")
            try:
                candidates = await get_top_symbols(session, _qr_pool2)
                semaphore_local = asyncio.Semaphore(PARALLEL_WORKERS)
                async def scored(symbol):
                    async with semaphore_local:
                        return await calc_symbol_quality_score(session, symbol)
                results = await asyncio.gather(*[scored(s) for s in candidates], return_exceptions=True)
                valid = [r for r in results if isinstance(r, dict) and r.get("valid")]
                valid.sort(key=lambda r: r["quality_score"], reverse=True)
                lines = ["🏆 <b>Top 20 Symbols by Quality</b>", ""]
                for i, r in enumerate(valid[:20], 1):
                    lines.append(f"{i}. {r['symbol']}: <b>{r['quality_score']}/100</b>")
                report = "\n".join(lines)
            except Exception as e:
                log.error(f"qr_show_top error: {e}")
                report = "Error calculating quality ranking."
            await edit_msg(session, cid, mid, report,
                reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "quality_menu"}]]})

        elif data == "manual_report" and has_permission(uid, "logs", state):
            # Admin on-demand report: purely read-only, generated straight from
            # stored data. Per spec this is INDEPENDENT of the automatic Daily
            # Report — it must never publish/edit the public channel message,
            # so (unlike before) it does NOT call sync_daily_report() here.
            await edit_msg(session, cid, mid, "⏳ Preparing today's report...")
            trades = await update_trade_outcomes(session)
            # CORRECTION: "today" for this on-demand report must use the same
            # Asia/Tehran calendar-day boundary that get_trades_for_report()/
            # build_daily_report() now use, not the UTC day.
            today_str = tehran_date_str()
            report_text = build_daily_report(trades, today_str)
            await edit_msg(session, cid, mid, report_text,
                reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "main_menu"}]]})

        elif data == "pending_signals_menu" and has_permission(uid, "logs", state):
            try:
                await update_trade_outcomes(session)  # refresh statuses before listing
            except Exception as e:
                log.error(f"pending_signals_menu update_trade_outcomes error: {e}")
            rows = get_pending_signals()
            if not rows:
                await edit_msg(session, cid, mid,
                    "🗂 <b>Pending Signals</b>\n\nNo signals without a final result right now.",
                    reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "main_menu"}]]})
            else:
                await edit_msg(session, cid, mid,
                    f"🗂 <b>Pending Signals</b> ({len(rows)}, oldest first)\n\nTap a signal to set its result:",
                    reply_markup=pending_signals_kb(rows))

        elif data.startswith("psig_open:") and has_permission(uid, "logs", state):
            try:
                signal_id = int(data.split(":", 1)[1])
            except (ValueError, IndexError):
                signal_id = None
            sig = get_signal_by_id(signal_id) if signal_id else None
            if not sig:
                await edit_msg(session, cid, mid, "⚠️ Signal not found.",
                    reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "pending_signals_menu"}]]})
            else:
                dir_label = "🟢 LONG" if sig["direction"] == "BUY" else "🔴 SHORT"
                opened_str = (sig.get("opened_at") or "")[:16].replace("T", " ")
                text = (
                    f"🆔 <b>Signal ID</b>\n<code>{sig.get('signal_uid') or sig['id']}</code>\n\n"
                    f"💎 <b>Symbol:</b> {sig['symbol']}\n"
                    f"{dir_label}\n"
                    f"📊 <b>Strategy:</b> {sig['strategy']}\n"
                    f"🕒 <b>Time:</b> {opened_str} UTC\n"
                    f"📌 <b>Current status:</b> {sig['status']}\n\n"
                    f"Choose the result:"
                )
                await edit_msg(session, cid, mid, text,
                    reply_markup=manual_result_actions_kb(signal_id, "pending_signals_menu"))

        elif data.startswith("mres:") and has_permission(uid, "scan_toggle", state):
            try:
                _, sid_str, action_key = data.split(":", 2)
                signal_id = int(sid_str)
            except (ValueError, IndexError):
                signal_id, action_key = None, None
            if not signal_id or action_key not in MANUAL_RESULT_ACTIONS:
                await answer_callback(session, cb["id"], "Invalid action.")
            else:
                await answer_callback(session, cb["id"])
                await edit_msg(session, cid, mid, "⏳ Applying result...")
                result = await apply_manual_trade_result(session, signal_id, action_key, uid, state)
                if not result:
                    await edit_msg(session, cid, mid, "⚠️ Could not apply result — signal not found.",
                        reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "pending_signals_menu"}]]})
                else:
                    pnl_line = f"\nPnL: {result['pnl_percent']:.2f}%" if result["pnl_percent"] is not None else ""
                    await edit_msg(session, cid, mid,
                        f"✅ Result applied.\n\n"
                        f"🆔 <code>{result['signal_uid']}</code>\n"
                        f"💎 {result['symbol']}\n"
                        f"{result['label']}{pnl_line}\n\n"
                        f"Database, statistics, reports, and strategy performance have all been "
                        f"updated exactly as they would be for an automatic result. "
                        f"(Flagged as a manual result for audit.)",
                        reply_markup={"inline_keyboard": [
                            [{"text": "🗂 Pending Signals", "callback_data": "pending_signals_menu"}],
                            [{"text": "◀️ Main Menu", "callback_data": "main_menu"}],
                        ]})

        elif data == "show_stats" and has_permission(uid, "logs", state):
            await edit_msg(session, cid, mid, "⏳ Calculating advanced statistics...")
            try:
                report = build_stats_report()
            except Exception as e:
                log.error(f"show_stats error: {e}")
                report = "Error calculating statistics."
            await edit_msg(session, cid, mid, report,
                reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "main_menu"}]]})

        elif data == "analyzer_start":
            state["pending_analysis"] = uid
            save_state(state)
            await edit_msg(session, cid, mid,
                "🔍 <b>Personal Analyzer</b>\n\n"
                "Send a coin name (e.g. BTC or BTCUSDT) and I'll analyze it with all strategies and quality filters.",
                reply_markup={"inline_keyboard": [[{"text": "◀️ Cancel", "callback_data": "main_menu"}]]})

        elif data == "replay_start" and has_permission(uid, "logs", state):
            state["pending_replay"] = uid
            save_state(state)
            await edit_msg(session, cid, mid,
                "🎬 <b>Replay Mode</b>\n\n"
                "Send a coin name (e.g. BTC or BTCUSDT) and I'll show you its most recently recorded signals with entry reason, strategy, score, and final result.",
                reply_markup={"inline_keyboard": [[{"text": "◀️ Cancel", "callback_data": "main_menu"}]]})

        elif data == "strategies" and has_permission(uid, "strategies", state):
            await edit_msg(session, cid, mid,
                "📊 <b>Strategies</b> — click to turn on/off:",
                reply_markup=strategies_kb(state))

        elif data.startswith("ts:") and has_permission(uid, "strategies", state):
            name = data[3:]
            disabled = state.get("disabled_strategies", [])
            if name in disabled: disabled.remove(name); icon = "🟢 Active"
            else: disabled.append(name); icon = "🔴 Inactive"
            state["disabled_strategies"] = disabled
            save_state(state)
            await edit_msg(session, cid, mid,
                f"<b>{name}</b>: {icon}", reply_markup=strategies_kb(state))

        elif data == "bl_menu" and has_permission(uid, "blacklist", state):
            bl = load_blacklist()
            txt = "\n".join(sorted(bl)) if bl else "Blacklist is empty"
            await edit_msg(session, cid, mid,
                f"🚫 <b>Blacklist</b>\n\n{txt}\n\n/bl SYMBOL — add\n/unbl SYMBOL — remove",
                reply_markup=bl_kb(bl))

        elif data.startswith("blr:") and has_permission(uid, "blacklist", state):
            sym = data[4:]
            bl = load_blacklist(); bl.discard(sym); save_blacklist(bl)
            await edit_msg(session, cid, mid, f"✅ {sym} removed.", reply_markup=bl_kb(bl))

        # ── Channels and groups ──
        elif data == "channels_menu" and has_permission(uid, "channels", state):
            channels = state.get("channels", [])
            if channels:
                lines = "\n".join(f"• {c.get('title', c['id'])} — {'Active ✅' if c.get('active', True) else 'Inactive ❌'}" for c in channels)
            else:
                lines = "No channel or group registered yet."
            await edit_msg(session, cid, mid,
                f"📢 <b>Channels & Groups</b>\n\n{lines}",
                reply_markup=channels_kb(state))

        elif data == "ch_add" and has_permission(uid, "channels", state):
            await edit_msg(session, cid, mid,
                "➕ <b>Add a New Channel or Group</b>\n\n"
                "1. Add the bot by its username (@AFEE_SignalBot) to the channel or group.\n"
                "2. Make sure to give the bot <b>Admin</b> access so it can send messages.\n"
                "3. As soon as it's added, the bot will detect and register that channel itself.\n"
                "4. Once registered, you can enable/disable it from this same menu.\n\n"
                "💡 You can also use the /start and /stop commands directly inside the channel or group to turn signal delivery on or off.",
                reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "channels_menu"}]]})

        elif data.startswith("ch_toggle:") and has_permission(uid, "channels", state):
            ch_id = int(data.split(":", 1)[1])
            for c in state.get("channels", []):
                if c["id"] == ch_id:
                    c["active"] = not c.get("active", True)
            save_state(state)
            await edit_msg(session, cid, mid, "📢 <b>Channels & Groups</b>", reply_markup=channels_kb(state))

        elif data.startswith("ch_remove:") and has_permission(uid, "channels", state):
            ch_id = int(data.split(":", 1)[1])
            state["channels"] = [c for c in state.get("channels", []) if c["id"] != ch_id]
            save_state(state)
            await edit_msg(session, cid, mid, "✅ Removed.", reply_markup=channels_kb(state))

        # ── Manage admins ──
        elif data == "admins_menu" and has_permission(uid, "admins", state):
            await edit_msg(session, cid, mid,
                "👥 <b>Manage Admins</b>\n\nClick an admin to view and change their permissions.",
                reply_markup=admins_kb(state))

        elif data == "admin_add" and has_permission(uid, "admins", state):
            state["pending_admin_add"] = uid
            save_state(state)
            await edit_msg(session, cid, mid,
                "➕ <b>Add New Admin</b>\n\n"
                "Forward me a message from the person you want to make an admin.\n"
                "(They must have already messaged this bot, or you must have a message from them that you can forward)",
                reply_markup={"inline_keyboard": [[{"text": "◀️ Cancel", "callback_data": "admins_menu"}]]})

        elif data.startswith("admin_view:") and has_permission(uid, "admins", state):
            aid = data.split(":", 1)[1]
            info = state.get("admins", {}).get(aid, {})
            perms = info.get("perms", [])
            perm_txt = "\n".join(f"• {PERMISSIONS[p]}" for p in perms) if perms else "No permissions"
            await edit_msg(session, cid, mid,
                f"👤 <b>{info.get('name', aid)}</b>\n\nCurrent permissions:\n{perm_txt}\n\nClick any of them to change:",
                reply_markup=admin_detail_kb(state, aid))

        elif data.startswith("admin_perm:") and has_permission(uid, "admins", state):
            _, aid, pkey = data.split(":", 2)
            admins = state.get("admins", {})
            if aid in admins:
                perms = admins[aid].get("perms", [])
                if pkey in perms: perms.remove(pkey)
                else: perms.append(pkey)
                admins[aid]["perms"] = perms
                state["admins"] = admins
                save_state(state)
            info = admins.get(aid, {})
            perms = info.get("perms", [])
            perm_txt = "\n".join(f"• {PERMISSIONS[p]}" for p in perms) if perms else "No permissions"
            await edit_msg(session, cid, mid,
                f"👤 <b>{info.get('name', aid)}</b>\n\nCurrent permissions:\n{perm_txt}",
                reply_markup=admin_detail_kb(state, aid))

        elif data.startswith("admin_remove:") and has_permission(uid, "admins", state):
            aid = data.split(":", 1)[1]
            admins = state.get("admins", {})
            if is_super_admin(int(aid), state):
                await edit_msg(session, cid, mid, "⛔️ The primary admin (bot creator) cannot be removed.", reply_markup=admins_kb(state))
                return
            admins.pop(aid, None)
            state["admins"] = admins
            save_state(state)
            await edit_msg(session, cid, mid, "✅ Admin removed.", reply_markup=admins_kb(state))

        elif data == "show_logs" and has_permission(uid, "logs", state):
            try:
                with open("afee_bot.log", encoding="utf-8") as f:
                    lines = f.readlines()
                txt = "".join(lines[-15:])[-3000:]
                await edit_msg(session, cid, mid, f"📋 <b>Logs:</b>\n<pre>{txt}</pre>",
                    reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "main_menu"}]]})
            except Exception:
                await edit_msg(session, cid, mid, "Log not found.",
                    reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "main_menu"}]]})

        elif data == "settings":
            _cfg_now = load_bot_config()
            _top_n_now = _cfg_now.get("TOP_N_COINS", TOP_N_COINS_DEFAULT)
            _pool_now  = _cfg_now.get("QUALITY_POOL_SIZE", QUALITY_POOL_SIZE_DEFAULT)
            _sc_trend  = _cfg_now.get("DYNAMIC_SCORE_THRESHOLD_TREND", 75)
            _sc_range  = _cfg_now.get("DYNAMIC_SCORE_THRESHOLD_RANGE", 85)
            _sc_hv     = _cfg_now.get("DYNAMIC_SCORE_THRESHOLD_HIGH_VOLATILITY", 90)
            await edit_msg(session, cid, mid,
                f"⚙️ <b>Settings</b>\n\n"
                f"🔍 Coins (TOP_N_COINS): <b>{_top_n_now}</b> (from bot_config.json)\n"
                f"📦 Pool Size: <b>{_pool_now}</b>\n"
                f"📊 Min Score — Trend: <b>{_sc_trend}</b> | Range: <b>{_sc_range}</b> | High-Vol: <b>{_sc_hv}</b>\n"
                f"⏱ Cooldown: <b>{SIGNAL_COOLDOWN//60} minutes</b>\n"
                f"👷 Workers: <b>{PARALLEL_WORKERS}</b>\n"
                f"⏰ Scan interval: <b>{SCAN_INTERVAL}s</b>\n\n"
                f"💡 To change TOP_N_COINS or the Dynamic Score thresholds, edit the <b>bot_config.json</b> file.",
                reply_markup={"inline_keyboard": [[{"text": "◀️ Back", "callback_data": "main_menu"}]]})

        # ─── BACKTEST MENU ─────────────────────────────────────────────────────
        elif data == "backtest_menu":
            await edit_msg(session, cid, mid,
                "📊 <b>Signal Performance Report</b>\n\n"
                "Choose a time period and scope. Filters are applied exactly like the Live Engine.\n"
                "Results are sent as a detailed (chunked) report.",
                reply_markup=backtest_menu_kb(state))

        elif data.startswith("bt_range_"):
            hours = int(data.split("_")[-1])
            await edit_msg(session, cid, mid, f"⏳ Running backtest for the last {hours} hours...")
            try:
                symbols = await get_quality_ranked_symbols(session, get_top_n_coins(), get_quality_pool_size()) \
                    if state.get("quality_ranking_enabled", True) \
                    else await get_top_symbols(session, get_top_n_coins())
                job_params = {
                    "symbols": symbols[:50],  # limit of 50 symbols per job
                    "timerange_hours": hours,
                    "strategies": [s for s, _ in STRATEGIES],
                    "requested_by": str(uid),
                }
                job_id = await enqueue_backtest(session, job_params)
                # wait for the job to finish (with a timeout)
                for _ in range(120):
                    await asyncio.sleep(2)
                    if job_id in _backtest_cache:
                        break
                cached = _backtest_cache.get(job_id)
                if cached:
                    parts = build_backtest_report(cached["trades"], cached["summary"], hours)
                    for part in parts:
                        await send_msg(session, cid, part)
                    # Chart for the first trade
                    if cached["trades"]:
                        t = cached["trades"][0]
                        chart_txt = build_trade_chart(
                            t.get("symbol",""), t.get("entry",0), t.get("stop",0),
                            t.get("tp1",0), t.get("tp2",0),
                            t.get("direction","BUY"), t.get("outcome"))
                        await send_msg(session, cid, chart_txt)
                else:
                    await send_msg(session, cid, "⏳ The backtest is queued. Results will be sent later.")
            except Exception as e:
                log.error(f"Backtest handler error: {e}")
                await send_msg(session, cid, f"❌ Error running backtest: {e}")

        elif data == "bt_scope_all":
            await edit_msg(session, cid, mid,
                "📦 scope: all USDT pairs (default)\nChoose a time period to get started.",
                reply_markup=backtest_menu_kb(state))

        # ─── HISTORICAL STRATEGY REPLAY — fully separate namespace (replay_*) ──
        elif data == "replay_menu":
            await edit_msg(session, cid, mid,
                "🧪 <b>Historical Candle Simulation</b>\n\n"
                "Re-runs the REAL strategy logic against historical candles to find "
                "hypothetical signals — different from Signal Performance Report, which "
                "only reports on signals the bot actually generated live.\n\n"
                "1h/4h/12h/24h/7d/30d → same configured symbol universe as live scanning "
                "(TOP_N_COINS / quality ranking, exactly like /scan).",
                reply_markup=replay_menu_kb())

        elif data.startswith("replay_range_") and has_permission(uid, "scan_toggle", state):
            hours = int(data.split("_")[-1])
            period_label = {168: "7 days", 720: "30 days"}.get(hours, f"{hours} hour" + ("s" if hours != 1 else ""))
            await edit_msg(session, cid, mid, f"🔍 Resolving symbol scope for {period_label}...")
            try:
                backtest_id = _generate_backtest_id()
                now_ms = int(time.time() * 1000)
                # FIX (requirement 3 audit — OPEN-count bug): the window
                # previously ended at "now", so any signal generated near
                # the end of the requested period had close to zero real
                # elapsed time to resolve (a trade needs up to
                # REPLAY_TRADE_EXPIRY_HOURS=24h of real future price action
                # to genuinely reach TP1/TP2/SL/expiry). That guaranteed a
                # huge OPEN count regardless of whether the pipeline itself
                # was correct — it was an artifact of the window, not signal
                # over-generation. Fix: end the window
                # REPLAY_TRADE_EXPIRY_HOURS before "now", so every signal in
                # the requested period has already had its FULL possible
                # resolution window elapse for real — "OPEN" then only means
                # genuinely still-unresolved at 24h, exactly matching what
                # live monitoring would show for an old, still-open trade.
                #
                # NOTE: this timestamp pair is now computed BEFORE universe
                # selection (moved up from below) specifically so
                # period_start_ms can be passed into universe selection as
                # the historical "as of" anchor — see the reproducibility
                # fix directly below.
                period_end_ms = now_ms - REPLAY_TRADE_EXPIRY_HOURS * 3_600_000
                period_start_ms = period_end_ms - hours * 3_600_000

                # REPRODUCIBILITY FIX (root cause A — see investigation):
                # get_quality_ranked_symbols()/get_top_symbols() rank by LIVE
                # 24h volume / live order-book spread / live recent candles,
                # none of which are anchored to the replay period. That made
                # the universe reflect "right now" — a different "right now"
                # on every call — instead of the market as it stood at the
                # start of the simulated period. get_quality_ranked_symbols_
                # asof()/get_top_symbols_asof() are historical equivalents:
                # every ranking input is fetched for a fixed window ending at
                # `period_start_ms`, so the same period_start_ms always
                # produces the same universe. Same TOP_N_COINS/
                # QUALITY_POOL_SIZE/quality_ranking_enabled configuration as
                # before — only WHEN the ranking data is measured has changed.
                if state.get("quality_ranking_enabled", True):
                    pool_size = get_quality_pool_size()
                    symbols = await get_quality_ranked_symbols_asof(
                        session, get_top_n_coins(), pool_size, period_start_ms)
                    scope_label = (f"Historical universe as of period start (TOP_N_COINS={get_top_n_coins()}, "
                                   f"quality-ranked from pool of {pool_size})")
                else:
                    symbols = await get_top_symbols_asof(session, get_top_n_coins(), period_start_ms)
                    scope_label = f"Historical universe as of period start (TOP_N_COINS={get_top_n_coins()}, quality ranking off)"
                # NOTE (requirement 2, v6.21.0): the previous top-30 cap for
                # 7-day replay has been removed. 7-day replay now uses the
                # SAME configured universe size as every other Replay
                # Backtest window (TOP_N_COINS / quality-ranked pool). The
                # job simply takes longer for 7 days of 1m data — existing
                # performance architecture (bounded concurrency, historical
                # candle caching, fetch-once/reuse, pagination, rate-limit/
                # backoff, progress updates, incremental persistence) is
                # unchanged and is what makes the larger universe feasible.
                bl = load_blacklist()
                symbols = [s for s in symbols if s not in bl]
                if not symbols:
                    await edit_msg(session, cid, mid, "❌ No symbols available (check blacklist/API).")
                    return

                # REPRODUCIBILITY FIX (root cause B/D): take every mutable-
                # state snapshot ONCE, right here, before the job starts —
                # not inside _run_replay_backtest_job — so the exact values
                # persisted to replay_backtests for diagnostics are
                # guaranteed identical to what the job actually uses (no
                # window for a second read to disagree with the first).
                replay_weights = get_all_strategy_weights()
                replay_cfg = load_bot_config()
                frozen_state = copy.deepcopy(state)

                nowiso = datetime.now(timezone.utc).isoformat()
                conn = get_db_connection()
                try:
                    conn.execute("""
                        INSERT INTO replay_backtests
                            (backtest_id, status, period_label, scope_label, symbols_json,
                             settings_snapshot_json, total_symbols, created_at, updated_at,
                             period_start_ms, period_end_ms, weights_snapshot_json, config_snapshot_json)
                        VALUES (?, 'RUNNING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (backtest_id, period_label, scope_label, json.dumps(symbols),
                          json.dumps(frozen_state.get("strategy_settings", {})), len(symbols), nowiso, nowiso,
                          period_start_ms, period_end_ms, json.dumps(replay_weights), json.dumps(replay_cfg)))
                    conn.commit()
                finally:
                    conn.close()

                progress_msg_id = await send_telegram(
                    session,
                    f"🔄 <b>Replay Backtest — <code>{backtest_id}</code></b>\n"
                    f"Progress: 0/{len(symbols)} symbols\nSignals: 0\nTrades resolved: 0\n"
                    f"Elapsed: 00:00\nStatus: Starting...",
                    cid,
                )
                asyncio.create_task(_run_replay_backtest_job(
                    session, backtest_id, symbols, period_start_ms, period_end_ms,
                    period_label, scope_label, frozen_state, cid, progress_msg_id,
                    replay_weights, replay_cfg,
                ))
                await edit_msg(session, cid, mid,
                    f"✅ Replay Backtest <b><code>{backtest_id}</code></b> started in the background "
                    f"({len(symbols)} symbols, {scope_label}, {period_label}).\n"
                    f"The bot stays fully responsive — progress updates will appear above, "
                    f"and the full report is sent automatically when it finishes.",
                    reply_markup=replay_menu_kb())
            except Exception as e:
                log.error(f"Replay Backtest start error: {e}")
                await edit_msg(session, cid, mid, f"❌ Error starting Replay Backtest: {e}")

        elif data.startswith("replay_bystrat:"):
            backtest_id = data.split(":", 1)[1]
            conn = get_db_connection()
            try:
                strat_rows = conn.execute(
                    "SELECT DISTINCT strategy FROM replay_trades WHERE backtest_id=?", (backtest_id,)
                ).fetchall()
            finally:
                conn.close()
            rows = [[{"text": r["strategy"], "callback_data": f"replay_list:{backtest_id}:strat:{r['strategy']}"}]
                    for r in strat_rows]
            rows.append([{"text": "◀️ Back", "callback_data": "main_menu"}])
            await edit_msg(session, cid, mid, f"📂 <b><code>{backtest_id}</code></b> — choose a strategy:",
                           reply_markup={"inline_keyboard": rows})

        elif data.startswith("replay_bysym:"):
            backtest_id = data.split(":", 1)[1]
            conn = get_db_connection()
            try:
                sym_rows = conn.execute(
                    "SELECT DISTINCT symbol FROM replay_trades WHERE backtest_id=? ORDER BY symbol", (backtest_id,)
                ).fetchall()
            finally:
                conn.close()
            rows = [[{"text": r["symbol"], "callback_data": f"replay_list:{backtest_id}:sym:{r['symbol']}"}]
                    for r in sym_rows]
            rows.append([{"text": "◀️ Back", "callback_data": "main_menu"}])
            await edit_msg(session, cid, mid, f"🔎 <b><code>{backtest_id}</code></b> — choose a symbol:",
                           reply_markup={"inline_keyboard": rows})

        elif data.startswith("replay_list:"):
            _, backtest_id, kind, value = data.split(":", 3)
            trades = _replay_query_trades(backtest_id, strategy=value if kind == "strat" else None,
                                           symbol=value if kind == "sym" else None)
            if not trades:
                await edit_msg(session, cid, mid, "No trades found.")
            else:
                header = f"<b><code>{backtest_id}</code></b> — {value} ({len(trades)} trades)\n"
                await edit_msg(session, cid, mid, header)
                for t in trades[:25]:  # cap a single response burst
                    kb = {"inline_keyboard": [[{"text": "📊 Show Chart", "callback_data": f"replay_chart:{t['id']}"}]]}
                    await send_telegram(session, _replay_format_trade(t), cid, reply_markup=kb)

        elif data.startswith("replay_chart:"):
            trade_id = int(data.split(":", 1)[1])
            await _send_replay_trade_chart(session, cid, trade_id)

        # ─── STRATEGY SETTINGS — per-strategy filter actions ───────────────────
        elif data.startswith("sf:") and has_permission(uid, "scan_toggle", state):
            _, idx_str, action = data.split(":", 2)
            idx = int(idx_str)
            name, _fn = STRATEGIES[idx]
            cfg = get_strategy_settings(state, name)

            if action == "adx_inc":
                cfg["adx_threshold"] = round(min(50.0, cfg.get("adx_threshold", ADX_THRESHOLD_DEFAULT) + 1.0), 1)
            elif action == "adx_dec":
                cfg["adx_threshold"] = round(max(10.0, cfg.get("adx_threshold", ADX_THRESHOLD_DEFAULT) - 1.0), 1)
            elif action == "adxthresh_toggle":
                cfg["adx_threshold_enabled"] = not cfg.get("adx_threshold_enabled", True)
            elif action == "atr_inc":
                cfg["atr_multiplier"] = round(min(5.0, cfg.get("atr_multiplier", ATR_MULTIPLIER_DEFAULT) + 0.1), 1)
            elif action == "atr_dec":
                cfg["atr_multiplier"] = round(max(1.0, cfg.get("atr_multiplier", ATR_MULTIPLIER_DEFAULT) - 0.1), 1)
            elif action.startswith("sess_"):
                sess_key = action[5:]  # london | ny | asian
                sf = cfg.get("session_filters", SESSION_FILTERS_DEFAULT.copy())
                sf[sess_key] = not sf.get(sess_key, True)
                cfg["session_filters"] = sf
            elif action == "evf_toggle":
                cfg["entry_volume_filter_enabled"] = not cfg.get("entry_volume_filter_enabled", True)
            elif action == "evf_ma_inc":
                cfg["entry_volume_ma_period"] = min(100, cfg.get("entry_volume_ma_period", 20) + 5)
            elif action == "evf_ma_dec":
                cfg["entry_volume_ma_period"] = max(5, cfg.get("entry_volume_ma_period", 20) - 5)
            elif action == "evf_mult_inc":
                cfg["entry_volume_multiplier"] = round(min(5.0, cfg.get("entry_volume_multiplier", 1.5) + 0.1), 2)
            elif action == "evf_mult_dec":
                cfg["entry_volume_multiplier"] = round(max(1.0, cfg.get("entry_volume_multiplier", 1.5) - 0.1), 2)
            elif action == "oi_toggle":
                cfg["oi_filter_enabled"] = not cfg.get("oi_filter_enabled", False)
            elif action == "oi_inc":
                cfg["oi_threshold"] = round(min(50.0, cfg.get("oi_threshold", 5.0) + 1.0), 1)
            elif action == "oi_dec":
                cfg["oi_threshold"] = round(max(0.5, cfg.get("oi_threshold", 5.0) - 1.0), 1)
            elif action == "fund_toggle":
                cfg["funding_filter_enabled"] = not cfg.get("funding_filter_enabled", False)
            elif action == "fund_inc":
                cfg["funding_rate_limit"] = round(min(1.0, cfg.get("funding_rate_limit", 0.05) + 0.01), 3)
            elif action == "fund_dec":
                cfg["funding_rate_limit"] = round(max(0.01, cfg.get("funding_rate_limit", 0.05) - 0.01), 3)
            elif action == "btc_toggle":
                cfg["btc_trend_filter_enabled"] = not cfg.get("btc_trend_filter_enabled", False)
            elif action == "btc_tf":
                cur = cfg.get("btc_trend_timeframe", "4h")
                options = BTC_TREND_TIMEFRAMES
                nxt = options[(options.index(cur) + 1) % len(options)] if cur in options else options[0]
                cfg["btc_trend_timeframe"] = nxt
            elif action == "news_toggle":
                cfg["news_filter_enabled"] = not cfg.get("news_filter_enabled", False)
            elif action == "news_before_inc":
                cfg["news_before_minutes"] = min(240, cfg.get("news_before_minutes", 30) + 5)
            elif action == "news_before_dec":
                cfg["news_before_minutes"] = max(5, cfg.get("news_before_minutes", 30) - 5)
            elif action == "news_after_inc":
                cfg["news_after_minutes"] = min(240, cfg.get("news_after_minutes", 30) + 5)
            elif action == "news_after_dec":
                cfg["news_after_minutes"] = max(5, cfg.get("news_after_minutes", 30) - 5)
            # ── EMA Filter (Task 2) ──
            elif action == "ema_toggle":
                cfg["ema_filter_enabled"] = not cfg.get("ema_filter_enabled", False)
            elif action == "ema_period_inc":
                cfg["ema_filter_period"] = min(400, cfg.get("ema_filter_period", 50) + 10)
            elif action == "ema_period_dec":
                cfg["ema_filter_period"] = max(5, cfg.get("ema_filter_period", 50) - 10)
            elif action == "ema_dir_toggle":
                cfg["ema_filter_direction"] = "below" if cfg.get("ema_filter_direction", "above") == "above" else "above"
            elif action == "ema_mode_toggle":
                cfg["ema_filter_mode"] = "auto" if cfg.get("ema_filter_mode", "manual") == "manual" else "manual"
            elif action == "ema_auto_atr_toggle":
                cfg["ema_filter_auto_atr_enabled"] = not cfg.get("ema_filter_auto_atr_enabled", True)
            elif action == "ema_auto_atr_inc":
                cfg["ema_filter_auto_atr_multiplier"] = round(min(5.0, cfg.get("ema_filter_auto_atr_multiplier", 1.0) + 0.1), 1)
            elif action == "ema_auto_atr_dec":
                cfg["ema_filter_auto_atr_multiplier"] = round(max(0.1, cfg.get("ema_filter_auto_atr_multiplier", 1.0) - 0.1), 1)
            # ── ADX Maximum (Task 2) ──
            elif action == "adxmax_inc":
                cfg["adx_max"] = round(min(80.0, (cfg.get("adx_max") or 30.0) + 1.0), 1)
            elif action == "adxmax_dec":
                cfg["adx_max"] = round(max(15.0, (cfg.get("adx_max") or 30.0) - 1.0), 1)
            elif action == "adxmax_off":
                cfg["adx_max"] = None
            # ── ATR Range Filter (Task 2, values as % of price) ──
            elif action == "atrrange_toggle":
                cfg["atr_range_filter_enabled"] = not cfg.get("atr_range_filter_enabled", False)
            elif action == "atrmin_inc":
                cfg["atr_min_pct"] = round((cfg.get("atr_min_pct") or 0.0) + 0.1, 2)
            elif action == "atrmin_dec":
                cfg["atr_min_pct"] = round(max(0.0, (cfg.get("atr_min_pct") or 0.1) - 0.1), 2)
            elif action == "atrmin_off":
                cfg["atr_min_pct"] = None
            elif action == "atrmax_inc":
                cfg["atr_max_pct"] = round((cfg.get("atr_max_pct") or 0.0) + 0.1, 2)
            elif action == "atrmax_dec":
                cfg["atr_max_pct"] = round(max(0.0, (cfg.get("atr_max_pct") or 0.1) - 0.1), 2)
            elif action == "atrmax_off":
                cfg["atr_max_pct"] = None
            # ── Volume Filter: SMA20/EMA20 option (Task 2) ──
            elif action == "volma_toggle":
                cfg["volume_ma_filter_enabled"] = not cfg.get("volume_ma_filter_enabled", False)
            elif action == "volma_type_toggle":
                cfg["volume_ma_type"] = "ema" if cfg.get("volume_ma_type", "sma") == "sma" else "sma"
            # ── AI Trade Intelligence (ATIE) — NEW toggle ──
            elif action == "atie_toggle":
                cfg["ai_trade_intelligence_enabled"] = not cfg.get("ai_trade_intelligence_enabled", False)
            # ── ATR Stop Buffer (SL: exact reference vs. reference +/- ATR) ──
            elif action == "atrsl_toggle":
                cfg["atr_stop_buffer_enabled"] = not cfg.get("atr_stop_buffer_enabled", True)
            elif action == "atrsl_inc":
                cfg["atr_stop_multiplier"] = round((cfg.get("atr_stop_multiplier") or 1.5) + 0.1, 2)
            elif action == "atrsl_dec":
                cfg["atr_stop_multiplier"] = round(max(0.1, (cfg.get("atr_stop_multiplier") or 1.5) - 0.1), 2)
            # ── Global Breakout Validation: min body % outside zone ──
            elif action == "bkmb_inc":
                cfg["breakout_min_body_ratio"] = round(min(1.0, (cfg.get("breakout_min_body_ratio") or 0.6) + 0.05), 2)
            elif action == "bkmb_dec":
                cfg["breakout_min_body_ratio"] = round(max(0.0, (cfg.get("breakout_min_body_ratio") or 0.6) - 0.05), 2)
            # ── HB-only score-component thresholds ──
            elif action == "hbzone_inc":
                cfg["hb_zone_tolerance"] = round(min(0.05, cfg.get("hb_zone_tolerance", 0.008) + 0.001), 4)
            elif action == "hbzone_dec":
                cfg["hb_zone_tolerance"] = round(max(0.001, cfg.get("hb_zone_tolerance", 0.008) - 0.001), 4)
            elif action == "hbspike_inc":
                cfg["hb_spike_dominance_multiplier"] = round(min(10.0, cfg.get("hb_spike_dominance_multiplier", 3.0) + 0.1), 2)
            elif action == "hbspike_dec":
                cfg["hb_spike_dominance_multiplier"] = round(max(1.0, cfg.get("hb_spike_dominance_multiplier", 3.0) - 0.1), 2)
            elif action == "hbtight_inc":
                cfg["hb_tight_level_touch_ratio"] = round(min(1.0, cfg.get("hb_tight_level_touch_ratio", 0.5) + 0.05), 2)
            elif action == "hbtight_dec":
                cfg["hb_tight_level_touch_ratio"] = round(max(0.05, cfg.get("hb_tight_level_touch_ratio", 0.5) - 0.05), 2)

            save_state(state)
            await edit_msg(session, cid, mid,
                f"🧩 <b>Strategy Settings: {name}</b>",
                reply_markup=strategy_settings_detail_kb(state, idx))

        elif data == "importcfg_yes":
            if not is_super_admin(uid, state):
                await send_msg(session, cid, "⛔️ Super Admin only.")
            else:
                await edit_msg(session, cid, mid, "⏳ Importing configuration...")
                await finalize_config_import(session, uid, cid, state)

        elif data == "importcfg_no":
            _PENDING_IMPORT_DATA.pop(uid, None)
            await edit_msg(session, cid, mid, "❎ Import cancelled. Current configuration unchanged.")

async def poll_updates(session, state):
    offset = 0
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    while True:
        try:
            params = {"timeout": 5, "offset": offset,
                      "allowed_updates": ["message", "channel_post", "callback_query", "my_chat_member"]}
            async with session.get(url, params=params, proxy=PROXY,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
            if data.get("ok"):
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    await handle_update(session, upd, state)
        except Exception as e:
            log.debug(f"Poll error: {e}")
        await asyncio.sleep(1)

async def scan_symbol(session, symbol, semaphore, state, open_keys):
    """
    🟢 LAYER A — LIVE SIGNAL ENGINE
    تمام فیلترها باید پاس شوند؛ هر شکستی سیگنال را کاملاً بلاک می‌کند.
    Patch #8/#13: فیلتر شده‌ها کولدوان ۱۰-۱۵ دقیقه می‌خورند، سپس دوباره اسکن می‌شوند.
    """
    async with semaphore:
        results = []
        disabled = state.get("disabled_strategies", [])
        regime_engine_on = state.get("regime_engine_enabled", True)

        # NOTE: Session filter دیگر یک Gate سراسری نیست — چون حالا مختص هر استراتژی است
        # (state["strategy_settings"][strategy]["session_filters"])، بررسی آن به داخل
        # run_live_filters (که strategy را می‌شناسد) منتقل شده است.

        # ── Market Regime Engine ──
        if regime_engine_on:
            try:
                regime = await get_market_regime(session, symbol)
                allowed_types = regime["allowed_types"]
            except Exception as e:
                log.debug(f"Regime detection failed for {symbol}: {e}")
                allowed_types = {"TREND", "REVERSAL", "HYBRID"}
                regime = None
        else:
            allowed_types = {"TREND", "REVERSAL", "HYBRID"}
            regime = None

        # FIX (Pass 4 audit, performance): c1h_tp2/c4h_tp2 don't depend on strat_name —
        # they were being re-fetched from Binance on every single strategy that produced
        # a candidate result for this symbol (up to 7x per symbol per cycle for the exact
        # same data). Fetch lazily once per symbol, on first use, and reuse for the rest
        # of this symbol's strategy loop.
        _tp2_candles_cache = None

        for strat_name, strat_fn in STRATEGIES:
            if strat_name in disabled:
                continue

            strat_type = STRATEGY_REGIME_TYPE.get(strat_name, "HYBRID")
            if strat_type not in allowed_types:
                continue

            try:
                result = await strat_fn(session, symbol, state)
                if not result:
                    continue

                direction = result.get("direction", "")

                # Patch #9: Duplicate guard on (symbol + strategy + direction)
                if not can_signal(symbol, strat_name, open_keys, direction):
                    continue

                # ── TP2 Potential Score (Patch #12: calculate BEFORE filter so filter can use it) ──
                try:
                    if _tp2_candles_cache is None:
                        c1h_tp2 = await get_candles(session, symbol, "1h", 100)
                        c4h_tp2 = await get_candles(session, symbol, "4h", 80)
                        _tp2_candles_cache = (c1h_tp2, c4h_tp2)
                    else:
                        c1h_tp2, c4h_tp2 = _tp2_candles_cache
                    tp2_pot = calc_tp2_potential_score(c1h_tp2, c4h_tp2, result["direction"])
                    result["tp2_potential"] = tp2_pot
                except Exception:
                    result["tp2_potential"] = 50

                # ── Market Regime label (for Dynamic Score in run_live_filters) ──
                if regime:
                    result["market_regime"] = regime["regime_label"]

                # ── Layer A: همه فیلترها باید پاس شوند ──
                passed, block_reasons = await run_live_filters(session, symbol, result, state)
                if not passed:
                    log.info(
                        f"Signal rejection | ref={generate_rejection_ref()} LIVE FILTERED: "
                        f"{symbol} | {strat_name} | {direction} | reasons={block_reasons}"
                    )
                    # Patch #8/#13: mark for rescan after cooldown
                    mark_filtered_for_rescan(symbol)
                    continue

                # ── Entry volume bonus on Score (per-strategy settings) ──
                try:
                    _strat_cfg = get_strategy_settings(state, strat_name)
                    ma_period = _strat_cfg.get("entry_volume_ma_period", 20)
                    vol_multiplier = _strat_cfg.get("entry_volume_multiplier", 1.5)
                    c5m_vol = await get_candles(session, symbol, "5m", ma_period + 5)
                    vol_ratio = entry_volume_ratio(c5m_vol, ma_period)
                    bonus = min(10, int((vol_ratio - vol_multiplier) * 4)) if vol_ratio > vol_multiplier else 0
                    result["score"] = min(100, result.get("score", 0) + max(0, bonus))
                except Exception:
                    pass

                # ── Adaptive weight ──
                try:
                    weight = get_strategy_weight(strat_name)
                    if weight != 1.0:
                        result["score"] = max(1, min(100, round(result.get("score", 0) * weight)))
                        result["strategy_weight"] = weight
                except Exception as e:
                    log.debug(f"Weight lookup failed for {strat_name}: {e}")

                # ── Final score gate: re-check after volume bonus + weight adjustments ──
                # تضمین می‌کند هیچ سیگنالی با score کمتر از min_score ارسال نشود،
                # حتی اگر بعد از run_live_filters تغییراتی روی score اعمال شده باشد.
                _final_score = result.get("score", 0)
                _min_required = get_strategy_min_score(state, strat_name)
                # FIX (lifecycle patch #7): math.ceil(score) >= threshold instead of raw float compare
                if not (math.ceil(_final_score) >= _min_required):
                    log.info(
                        f"Signal rejection | ref={generate_rejection_ref()} POST-FILTER SCORE DROP: "
                        f"{symbol} | {strat_name} | {direction} | score={_final_score} < min={_min_required} → blocked"
                    )
                    continue


                results.append(result)
            except Exception as e:
                log.debug(f"Error {strat_name} {symbol}: {e}")
        return results

async def scan_once(session, state):
    if not state.get("scanning", True):
        log.info("Scanning paused."); return
    blacklist = load_blacklist()
    log.info("Starting scan...")

    # بررسی معاملات بازِ قبلی برای TP/SL — مستقل از اسکن جدید، تا نتیجه‌ها سریع ثبت شوند
    # ROOT-CAUSE FIX (execution-flow audit): this call was previously gated behind
    # `state.get("backtest_enabled", True)`. That flag is exposed in the Telegram
    # UI as "🧪 اتو بک‌تست" (Auto Backtest) — an admin toggling it OFF reasonably
    # expects it to only affect the /backtest analytics feature. It does not:
    # the real /backtest pipeline (enqueue_backtest / backtest_worker_loop /
    # _run_backtest_job) reads straight from the DB and never checks this flag
    # at all. Instead, this flag was silently gating the LIVE monitoring pass
    # itself. With it off, update_trade_outcomes() never ran from the main scan
    # loop, so Entry/TP1/TP2/SL were never detected in real time for any open
    # trade — they'd sit stale (PENDING/ENTERED/TP1_HIT) until the once-daily
    # report loop happened to call update_trade_outcomes() on its own schedule.
    # This is the actual mechanism behind trades showing a missing or outdated
    # result in history/daily-report/backtest (e.g. TAOUSDT): the stored
    # monitoring result just hadn't been computed yet, for up to 24h at a time.
    # Monitoring is core bot behaviour, not a backtest feature — it must run
    # every cycle unconditionally, same as the already-unconditional calls to
    # update_trade_outcomes() from the manual-report and daily-report paths.
    try:
        trades_after_outcomes = await update_trade_outcomes(session)
    except Exception as e:
        log.error(f"update_trade_outcomes error: {e}")
        trades_after_outcomes = None

    if trades_after_outcomes is not None:
        try:
            await sync_daily_report(session, state, trades_after_outcomes)
        except Exception as e:
            log.error(f"sync_daily_report error (post-outcomes): {e}")

    try:
        if state.get("quality_ranking_enabled", True):
            _configured_pool_size = get_quality_pool_size()
            symbols = await get_quality_ranked_symbols(session, get_top_n_coins(), _configured_pool_size)
            log.info(f"Quality ranking selected {len(symbols)} symbols from pool of {_configured_pool_size}")
        else:
            symbols = await get_top_symbols(session, get_top_n_coins())
    except Exception as e:
        log.error(f"Failed to get symbols: {e}"); return

    # FIX (rate-limit audit): a Binance 429/418 backoff makes get_top_symbols()
    # and/or get_candles() return [] for every candidate, which previously
    # surfaced as "Quality ranking selected 0 symbols" and then silently
    # scanned nothing for this cycle. That's indistinguishable in the logs
    # from "no symbol was good enough" — but it's actually a transient data
    # outage, not a ranking outcome. Since scan_loop already re-calls
    # scan_once() every SCAN_INTERVAL, skipping here is a free retry instead
    # of wasting a cycle on zero symbols.
    if not symbols and _binance_backoff_active():
        remaining = max(0.0, _binance_backoff_until - time.time())
        log.warning(
            f"Skipping scan cycle — Binance rate-limit backoff active "
            f"({remaining:.0f}s remaining), no market data available. "
            f"Will retry next cycle."
        )
        return

    symbols = [s for s in symbols if s not in blacklist]

    # Patch #8/#13: Also include symbols whose filter cooldown has expired (ready for rescan)
    rescan_symbols = [s for s in list(_filter_cooldown.keys())
                      if not is_in_filter_cooldown(s) and s not in blacklist and s not in symbols]
    if rescan_symbols:
        log.info(f"Re-scanning {len(rescan_symbols)} previously-filtered symbols: {rescan_symbols[:5]}...")
        symbols = symbols + rescan_symbols

    log.info(f"Scanning {len(symbols)} symbols | workers={PARALLEL_WORKERS}")
    semaphore = asyncio.Semaphore(PARALLEL_WORKERS)

    # یک‌بار در ابتدای چرخه، معاملات بازِ فعلی را می‌خوانیم تا برای همان ارز+استراتژی
    # تا وقتی نتیجه قبلی مشخص نشده، سیگنال تکراری صادر نشود.
    # ROOT-CAUSE FIX (execution-flow audit): previously `get_open_trade_keys()`
    # was skipped (returning an empty set) whenever "اتو بک‌تست" was off, which
    # silently disabled duplicate-signal protection — can_signal()/open_keys
    # dedup is core signal-issuance logic, unrelated to backtesting, and must
    # always reflect the real set of currently-open trades.
    open_keys = get_open_trade_keys()

    start = time.time()
    all_results = await asyncio.gather(
        *[scan_symbol(session, s, semaphore, state, open_keys) for s in symbols],
        return_exceptions=True
    )
    log.info(f"Scan done in {time.time()-start:.1f}s")

    # ROOT-CAUSE FIX (execution-flow audit): log_trade() was previously only
    # called `if backtest_on` (state["backtest_enabled"]). log_trade() is not
    # a backtest-only helper — it's the ONLY place a signal's `signals`+`results`
    # rows get created. With the toggle off, a signal was still broadcast to
    # Telegram but never written to the DB at all: no signal_id, so it could
    # never be picked up by update_trade_outcomes() (monitoring), never appear
    # in /replay history, never count in the daily report, and never appear in
    # /backtest — a broadcast trade with literally no stored result, matching
    # the "missing history" symptom (e.g. TAOUSDT). Persisting every issued
    # signal is required for bugs #1/#3/#4/#5 (monitoring, daily report,
    # history, backtest must all share one DB-stored result) and must not be
    # conditional on an unrelated analytics toggle.
    signals_sent = 0
    for res in all_results:
        if isinstance(res, Exception) or not res: continue
        for result in res:
            log.info(f"SIGNAL: {result['symbol']} | {result['strategy']} | {result['direction']} | score={result['score']}")
            # FIX (audit #1): register the cooldown here — at the point the signal is
            # actually being sent — instead of at raw-candidate time in can_signal().
            mark_signal_sent(result["symbol"], result["strategy"], result.get("direction", ""))
            # BUG FIX (Problem 3, price mismatch): overwrite the strategy's stale
            # higher-timeframe "price" with a live fapi ticker read right before
            # this signal goes out, so the "Price:" line in the Telegram message
            # reflects the actual current market at send time instead of a candle
            # close that may be minutes/hours old. Falls back to the strategy's
            # original value only if the live read fails, so a signal is never
            # blocked or delayed by this. Does not touch entry/stop/tp1/tp2/score.
            live_price = await get_live_futures_price(session, result["symbol"])
            if live_price is not None:
                result["price"] = live_price
            signal_id = None
            signal_uid = None
            try:
                signal_id, signal_uid = log_trade(result)
            except Exception as e:
                log.error(f"log_trade error: {e}")
            signal_msg, signal_chart_url = build_message(
                symbol=result["symbol"], direction=result["direction"],
                strategy=result["strategy"], price=result["price"],
                entry=result["entry"], stop=result["stop"],
                tp1=result["tp1"], tp2=result["tp2"],
                timeframe=result["timeframe"], score=result["score"],
                rsi=result.get("rsi"), market_regime=result.get("market_regime"),
                tp2_potential=result.get("tp2_potential"),
                tp2_unlikely=result.get("tp2_unlikely", False),
                signal_uid=signal_uid,
            )
            # ── AI CHART ANALYSIS (additive, isolated, optional) ───────────
            # Attempt to build the entry-timeframe chart image for this
            # signal. Every step here is wrapped so that any failure
            # (missing visual data, a rendering bug, matplotlib absent from
            # this deployment, etc.) falls straight through to the exact
            # same text+URL-button message this bot sent before this
            # feature existed — chart rendering must never delay or block
            # signal delivery.
            chart_png = None
            if CHART_FEATURE_AVAILABLE and signal_id:
                try:
                    result["signal_uid"] = signal_uid
                    snapshot = build_and_save_chart_snapshot(result, signal_id)
                    if snapshot:
                        chart_png = render_signal_chart(snapshot)
                except Exception as e:
                    log.error(f"Chart generation failed for signal_id={signal_id}: {e}")
                    chart_png = None

            # RESTORED: the original Binance Futures (TradingView) chart
            # button — same URL, same position (always button #1), exactly
            # as it worked before the chart-image feature existed.
            binance_url_button = {"text": "📈 Open Binance Futures Chart", "url": signal_chart_url}
            # New Signal ID System: a dedicated button between the Binance
            # chart button and the Live Chart Update button. Uses Telegram's
            # native copy_text button (Bot API 7.x) so a tap copies the ID
            # directly to the clipboard — no chat message needed to copy it.
            signal_id_button = (
                {"text": f"🆔 {signal_uid}", "copy_text": {"text": signal_uid}}
                if signal_uid else None
            )

            tg_message_id = None
            if chart_png and signal_id:
                # UI FIX: single Telegram message — chart image with the
                # full signal text as its caption and all buttons attached
                # to that same photo message, instead of a separate text
                # message followed by a caption-less photo reply.
                live_chart_button = {"text": "📸 Live Chart Update",
                                      "callback_data": f"livechart:{signal_id}"}
                button_row = [binance_url_button]
                if signal_id_button:
                    button_row.append(signal_id_button)
                button_row.append(live_chart_button)
                photo_button_markup = {"inline_keyboard": [button_row]}
                try:
                    tg_message_id = await broadcast_signal_photo(
                        session, chart_png, signal_msg, state,
                        reply_markup=photo_button_markup,
                    )
                except Exception as e:
                    log.error(f"Chart photo send failed for signal_id={signal_id}: {e}")
                    tg_message_id = None
            if not tg_message_id:
                # FAILSAFE (per spec): chart image unavailable/failed to send —
                # restore the original pre-chart-feature message: a single
                # text message with the Binance Futures URL button (plus the
                # new Signal ID button) attached directly. The signal must
                # always go out regardless of chart status.
                fallback_row = [binance_url_button]
                if signal_id_button:
                    fallback_row.append(signal_id_button)
                fallback_markup = {"inline_keyboard": [fallback_row]}
                sent_message_ids = await broadcast_signal(session, signal_msg, state, reply_markup=fallback_markup)
                tg_message_id = sent_message_ids.get(str(TELEGRAM_CHAT_ID))
            if signal_id and tg_message_id:
                try:
                    save_signal_tg_message_id(signal_id, tg_message_id)
                except Exception as e:
                    log.error(f"save_signal_tg_message_id error: {e}")
            try:
                await sync_daily_report(session, state, load_trades())
            except Exception as e:
                log.error(f"sync_daily_report error (post-signal): {e}")
            signals_sent += 1
            await asyncio.sleep(5)  # جلوگیری از Rate Limit تلگرام: حداقل ۵ ثانیه فاصله بین هر سیگنال
    log.info(f"Signals sent: {signals_sent}")

# ═══════════════════════════════════════════════════════════════════════════
# AI TRADE INTELLIGENCE ENGINE (ATIE) — NEW, PURELY ADDITIVE FEATURE
# ═══════════════════════════════════════════════════════════════════════════
# Post-signal trade-management monitor. Everything in this section is new
# code, added on top of the existing bot without modifying it:
#   - Does NOT touch signal generation, any strategy function, Signal Score,
#     TP/SL calculation, risk management, existing filters, existing
#     strategies, Telegram commands/menus, existing reports, config format,
#     or existing settings.
#   - Runs as its own independent asyncio background task (see main()),
#     never inside scan_loop()/scan_once(), so it can never delay or block
#     signal generation.
#   - Reuses existing indicator/calculation helpers already defined above
#     (calc_adx, calc_atr, calc_atr_regime, calc_volatility_regime,
#     calc_volume_regime, calc_ema200_slope, calc_rsi, entry_volume_ratio,
#     find_sr_levels_scored, get_nearest_sr_scored, is_strong_candle,
#     has_sufficient_volume_sma20, is_volatility_abnormal, swing_high,
#     swing_low, get_candles, get_db_connection, get_strategy_settings,
#     get_signal_notification_row, send_telegram) instead of recomputing
#     anything from scratch.
#   - Per-strategy opt-in via "ai_trade_intelligence_enabled" (default
#     Disabled), stored the same way as every other per-strategy filter.

ATIE_CHECK_INTERVAL_DEFAULT = 300     # 5 minutes — configurable at runtime, same pattern as get_adx_threshold() etc.
ATIE_MAX_UPDATES_NORMAL = 2           # max Telegram updates per signal under normal conditions
ATIE_MAX_UPDATES_EXCEPTIONAL = 3      # max updates per signal when the trend is in the "Exceptional" tier

# ── Stricter anti-spam thresholds ────────────────────────────────────────────
# A Trend Update is only ever sent when at least one of these MAJOR conditions
# is met. Small, single-metric fluctuations must never reach Telegram, so
# there is intentionally no generic "any score moved a bit" fallback anymore.
ATIE_CONFIDENCE_SIGNIFICANT_DELTA = 15   # Confidence must move a lot on its own to qualify
ATIE_QUALITY_SIGNIFICANT_DELTA = 15      # Overall trend quality (avg of the 4 scores) big move
ATIE_ADX_FACTOR_DELTA = 4                # ADX move big enough to count as one of the "factors"
ATIE_RELVOL_FACTOR_DELTA = 0.5            # Relative Volume move big enough to count as a "factor"
                                          # (0.5x matches the existing scoring system's own
                                          # "big move" bucket for rel_vol delta — see the top
                                          # tier in _atie_acceleration_score's rv_delta check)
ATIE_ATR_FACTOR_PCT = 15                 # ATR % move big enough to count as a "factor"
ATIE_MULTI_FACTOR_MIN = 2                # How many factors must move together to qualify

def get_atie_check_interval() -> int:
    """Monitoring cadence in seconds. Stored/read via the existing
    filter_config table (same mechanism as get_adx_threshold()/get_atr_multiplier()),
    so it's runtime-configurable without touching config format or restarting."""
    try:
        return int(float(get_filter_config("atie_check_interval_seconds", ATIE_CHECK_INTERVAL_DEFAULT)))
    except Exception:
        return ATIE_CHECK_INTERVAL_DEFAULT

def init_atie_tables():
    """Creates ATIE's own dedicated table only. Never touches signals/results/
    any pre-existing table or column. Safe to call multiple times (IF NOT EXISTS),
    same idempotent pattern as init_database()."""
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS atie_state (
                signal_id       INTEGER PRIMARY KEY REFERENCES signals(id),
                continuation    REAL,
                acceleration    REAL,
                health          REAL,
                confidence      REAL,
                tier            TEXT,
                adx             REAL,
                rel_vol         REAL,
                atr             REAL,
                htf_structure   TEXT,
                update_count    INTEGER NOT NULL DEFAULT 0,
                last_checked_at TEXT
            )
        """)
        conn.commit()
        # Backward-compatible migration: older installs already have this table
        # without the htf_structure column (used only for the stricter anti-spam
        # "multiple factors changed together" check). Safe/idempotent — ignored
        # if the column already exists.
        try:
            conn.execute("ALTER TABLE atie_state ADD COLUMN htf_structure TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass
    finally:
        conn.close()

def _atie_get_state(signal_id: int) -> Optional[dict]:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM atie_state WHERE signal_id=?", (signal_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def _atie_save_state(signal_id: int, m: dict, update_count: int):
    now = datetime.now(timezone.utc).isoformat()
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT INTO atie_state (signal_id, continuation, acceleration, health, confidence, "
            "tier, adx, rel_vol, atr, htf_structure, update_count, last_checked_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(signal_id) DO UPDATE SET continuation=excluded.continuation, "
            "acceleration=excluded.acceleration, health=excluded.health, confidence=excluded.confidence, "
            "tier=excluded.tier, adx=excluded.adx, rel_vol=excluded.rel_vol, atr=excluded.atr, "
            "htf_structure=excluded.htf_structure, "
            "update_count=excluded.update_count, last_checked_at=excluded.last_checked_at",
            (signal_id, m["continuation"], m["acceleration"], m["health"], m["confidence"],
             m["tier"], m["adx"], m["rel_vol"], m["atr"], m.get("htf_structure"), update_count, now)
        )
        conn.commit()
    finally:
        conn.close()

# ─── Small, self-contained monitoring-only helpers (never used by any strategy) ──
def _atie_new_bos(candles: list[dict], direction: str, lookback: int = 20) -> bool:
    """Lightweight break-of-structure check for monitoring purposes only:
    has price closed beyond the prior swing high/low, in the trade's own
    direction, within the last few candles? Reuses swing_high()/swing_low()."""
    if len(candles) < lookback + 5:
        return False
    prior = candles[-(lookback + 5):-3]
    recent = candles[-3:]
    if direction == "BUY":
        prior_high = swing_high(prior, len(prior))
        return any(c["close"] > prior_high for c in recent)
    else:
        prior_low = swing_low(prior, len(prior))
        return any(c["close"] < prior_low for c in recent)

def _atie_adverse_divergence(candles: list[dict], direction: str) -> bool:
    """RSI/price divergence working against the trade's direction. Independent,
    freshly-computed reuse of calc_rsi() — mirrors the divergence concept used
    elsewhere in the bot but never reads or depends on any strategy's state."""
    if len(candles) < 25:
        return False
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    rsis = calc_rsi(closes, 14)
    valid = [r for r in rsis if r is not None]
    if len(valid) < 10:
        return False
    if direction == "BUY":
        recent_high, past_high = max(highs[-5:]), max(highs[-20:-5])
        return recent_high > past_high and valid[-1] < max(valid[-10:-3] or [0])
    else:
        recent_low, past_low = min(lows[-5:]), min(lows[-20:-5])
        return recent_low < past_low and valid[-1] > min(valid[-10:-3] or [100])

# ─── The four INDEPENDENT scores (0-100). Each is computed directly from raw,
# already-available indicators — never from one of the other three scores. ──
def _atie_continuation_score(candles_1h: list[dict], candles_4h: list[dict], direction: str) -> int:
    """Probability that price can continue beyond TP2."""
    if not candles_1h or len(candles_1h) < 30:
        return 50
    score = 0
    adx = calc_adx(candles_1h, 14)
    score += 25 if adx >= 35 else 18 if adx >= 28 else 10 if adx >= 22 else 0
    ema_slope = calc_ema200_slope([c["close"] for c in candles_1h])
    if (direction == "BUY" and ema_slope == "UP") or (direction == "SELL" and ema_slope == "DOWN"):
        score += 20
    elif ema_slope == "FLAT":
        score += 8
    if candles_4h and len(candles_4h) >= 20:
        levels = find_sr_levels_scored(candles_4h)
        price = candles_1h[-1]["close"]
        if levels:
            if direction == "BUY":
                above = [lv["price"] for lv in levels if lv["price"] > price]
                if above:
                    space_pct = (min(above) - price) / price * 100
                    score += 30 if space_pct >= 4 else 18 if space_pct >= 2.5 else 8 if space_pct >= 1.5 else 0
                else:
                    score += 25
            else:
                below = [lv["price"] for lv in levels if lv["price"] < price]
                if below:
                    space_pct = (price - max(below)) / price * 100
                    score += 30 if space_pct >= 4 else 18 if space_pct >= 2.5 else 8 if space_pct >= 1.5 else 0
                else:
                    score += 25
        else:
            score += 15
    else:
        score += 15
    vol_regime = calc_volume_regime(candles_1h)
    score += 15 if vol_regime == "HIGH" else 8 if vol_regime == "NORMAL" else 0
    atr_regime = calc_atr_regime(candles_1h)
    score += 10 if atr_regime in ("NORMAL", "HIGH") else 0
    return max(0, min(100, score))

def _atie_acceleration_score(candles_1h: list[dict]) -> int:
    """Whether the trend is becoming stronger over time (rate of change) —
    intentionally NOT the same measure as continuation."""
    if not candles_1h or len(candles_1h) < 40:
        return 50
    score = 0
    adx_now = calc_adx(candles_1h, 14)
    adx_prev = calc_adx(candles_1h[:-10], 14)
    adx_delta = adx_now - adx_prev
    score += 30 if adx_delta >= 8 else 20 if adx_delta >= 4 else 10 if adx_delta >= 1 else 0 if adx_delta <= -4 else 5
    atr_now = calc_atr(candles_1h[-20:], 14)
    atr_prev = calc_atr(candles_1h[-40:-20], 14)
    atr_change_pct = ((atr_now - atr_prev) / atr_prev * 100) if atr_prev > 0 else 0
    score += 25 if atr_change_pct >= 20 else 15 if atr_change_pct >= 8 else 8 if atr_change_pct >= 0 else 0
    closes = [c["close"] for c in candles_1h]
    rsis = [r for r in calc_rsi(closes, 14) if r is not None]
    if len(rsis) >= 6:
        rsi_delta = abs(rsis[-1] - rsis[-6])
        score += 25 if rsi_delta >= 10 else 15 if rsi_delta >= 5 else 5
    rv_now = entry_volume_ratio(candles_1h, 20)
    rv_prev = entry_volume_ratio(candles_1h[:-10], 20)
    rv_delta = rv_now - rv_prev
    score += 20 if rv_delta >= 0.5 else 10 if rv_delta >= 0.15 else 5 if rv_delta > 0 else 0
    return max(0, min(100, score))

def _atie_health_score(candles_1h: list[dict], candles_4h: list[dict], direction: str) -> int:
    """Quality/stability of the trend: HTF S/R, momentum quality, breakout
    quality, BOS quality, volume quality, divergence."""
    if not candles_1h or len(candles_1h) < 30:
        return 50
    score = 0
    closes = [c["close"] for c in candles_1h]
    price = closes[-1]
    levels = find_sr_levels_scored(candles_4h) if candles_4h and len(candles_4h) >= 20 else []
    nearest = get_nearest_sr_scored(price, levels, tol=0.02) if levels else None
    score += min(20, nearest.get("sr_score", 50) / 5) if nearest else 10
    rsis = calc_rsi(closes, 14)
    rsi_now = next((r for r in reversed(rsis) if r is not None), None)
    if rsi_now is not None:
        if direction == "BUY" and 50 <= rsi_now <= 70:
            score += 20
        elif direction == "SELL" and 30 <= rsi_now <= 50:
            score += 20
        elif direction == "BUY" and 40 <= rsi_now < 50:
            score += 10
        elif direction == "SELL" and 50 < rsi_now <= 60:
            score += 10
    if len(candles_1h) >= 6 and is_strong_candle(candles_1h[-1], candles_1h[-6:-1]):
        score += 15
    if _atie_new_bos(candles_1h, direction):
        score += 15
    if has_sufficient_volume_sma20(candles_1h, 20):
        score += 15
    if _atie_adverse_divergence(candles_1h, direction):
        score -= 15
    else:
        score += 10
    if not is_volatility_abnormal(candles_1h):
        score += 15
    return max(0, min(100, score))

def _atie_confidence_score(candles_1h: list[dict], candles_4h: list[dict]) -> int:
    """Confidence in the AI analysis itself (data sufficiency/reliability) —
    calculated independently, never derived from the other three scores."""
    score = 40
    score += 20 if candles_1h and len(candles_1h) >= 100 else 10 if candles_1h and len(candles_1h) >= 50 else 0
    score += 15 if candles_4h and len(candles_4h) >= 40 else 8 if candles_4h and len(candles_4h) >= 20 else 0
    if candles_1h:
        atr_regime = calc_atr_regime(candles_1h)
        score += 15 if atr_regime == "NORMAL" else 5 if atr_regime == "HIGH" else 0
        if calc_volatility_regime(candles_1h) != "HIGH":
            score += 10
    return max(0, min(100, score))

def _atie_tier(avg_score: float) -> str:
    if avg_score >= 80:
        return "Exceptional"
    elif avg_score >= 60:
        return "Strong"
    return "Normal"

def _atie_estimate_reward(continuation: int, health: int, candles_4h: list[dict],
                           direction: str, entry: float, stop: float) -> str:
    """Realistic reward range (e.g. '5R-7R'), dependent on current market
    conditions — reuses find_sr_levels_scored() for available room."""
    risk = abs(entry - stop)
    if risk <= 0:
        return "2R-3R"
    avg = (continuation + health) / 2
    lo, hi = (5, 7) if avg >= 80 else (4, 6) if avg >= 60 else (3, 4) if avg >= 40 else (2, 3)
    if candles_4h and len(candles_4h) >= 20:
        levels = find_sr_levels_scored(candles_4h)
        price = candles_4h[-1]["close"]
        if levels:
            if direction == "BUY":
                above = [lv["price"] for lv in levels if lv["price"] > price]
                if above:
                    space_r = (min(above) - entry) / risk
                    if space_r > 0:
                        hi = min(hi, max(lo + 1, round(space_r)))
            else:
                below = [lv["price"] for lv in levels if lv["price"] < price]
                if below:
                    space_r = (entry - max(below)) / risk
                    if space_r > 0:
                        hi = min(hi, max(lo + 1, round(space_r)))
    return f"{lo}R-{hi}R"

def _atie_build_reasons(prev: dict, curr: dict, bos_now: bool) -> list[str]:
    """Neutral, measurable reasons only — never an instruction to close or hold.
    Notification-format improvement: every changed indicator is still listed (ADX,
    Relative Volume, ATR — no reason is ever hidden), just rendered as one compact
    line each instead of a two-line block, to keep the overall message short."""
    reasons = []
    if abs(curr["adx"] - prev["adx"]) >= 1:
        d = curr["adx"] - prev["adx"]
        reasons.append(f"ADX: {prev['adx']:.0f} → {curr['adx']:.0f} ({'+' if d >= 0 else ''}{d:.0f})")
    if prev["rel_vol"] and abs(curr["rel_vol"] - prev["rel_vol"]) >= 0.1:
        reasons.append(f"Relative Volume: {prev['rel_vol']:.1f}x → {curr['rel_vol']:.1f}x")
    if prev["atr"] and prev["atr"] > 0:
        atr_change_pct = (curr["atr"] - prev["atr"]) / prev["atr"] * 100
        if abs(atr_change_pct) >= 5:
            reasons.append(f"ATR: {'+' if atr_change_pct >= 0 else ''}{atr_change_pct:.0f}%")
    if bos_now:
        reasons.append("New BOS confirmed")
    prev_htf, curr_htf = prev.get("htf_structure"), curr.get("htf_structure")
    if prev_htf and curr_htf and prev_htf != curr_htf:
        reasons.append(f"HTF Structure: {prev_htf} → {curr_htf}")
    if not reasons:
        reasons.append("Composite trend scores shifted")
    return reasons

def _atie_short_explanation(prev: dict, curr: dict, bos_now: bool = False) -> str:
    """AI TRADE INTELLIGENCE (feature — improved): a genuine multi-factor
    summary instead of picking whichever single score moved the most (the
    old approach could label an update 'Higher confidence' while momentum,
    volume, and structure were all actually deteriorating at the same time
    — misleading, not useful). This evaluates the same five dimensions the
    scores are already built from — momentum, trend strength/ADX, volume,
    market structure, and continuation strength — and reports only the ones
    that actually moved meaningfully, in that order, e.g.:
      'Trend strength increasing, momentum aligned, TP2 probability high'
      'Momentum loss, volume decreasing, stop risk increasing'
    Every phrase is grounded directly in a real indicator delta already
    computed by _atie_check_signal (ADX, acceleration score, relative
    volume, HTF structure, continuation score) — never a guess."""
    fragments = []

    # Trend strength / ADX — the clearest, most direct "is the trend gaining
    # or losing power" signal available.
    adx_delta = curr["adx"] - prev["adx"]
    if adx_delta >= ATIE_ADX_FACTOR_DELTA / 2:
        fragments.append("Trend strength increasing")
    elif adx_delta <= -ATIE_ADX_FACTOR_DELTA / 2:
        fragments.append("Trend strength weakening")

    # Momentum — the acceleration score is already a dedicated momentum
    # measure (rate-of-change of the trend), independent of the other three.
    accel_delta = curr["acceleration"] - prev["acceleration"]
    if accel_delta >= 8:
        fragments.append("momentum aligned")
    elif accel_delta <= -8:
        fragments.append("momentum loss")

    # Volume — relative volume vs. its own recent baseline.
    if prev.get("rel_vol"):
        rv_delta = curr["rel_vol"] - prev["rel_vol"]
        if rv_delta >= ATIE_RELVOL_FACTOR_DELTA / 2:
            fragments.append("volume increasing")
        elif rv_delta <= -ATIE_RELVOL_FACTOR_DELTA / 2:
            fragments.append("volume decreasing")

    # Market structure — HTF structure alignment/flip, or a fresh
    # break-of-structure in the trade's own direction.
    prev_struct, curr_struct = prev.get("htf_structure"), curr.get("htf_structure")
    if bos_now:
        fragments.append("structure confirming trend")
    elif curr_struct == "Aligned" and prev_struct != "Aligned":
        fragments.append("structure realigning with trend")
    elif curr_struct == "Not Aligned" and prev_struct == "Aligned":
        fragments.append("structure diverging from trend")

    # Continuation strength — read directly as a TP2-probability statement,
    # exactly the framing requested, rather than a bare score delta.
    cont_delta = curr["continuation"] - prev["continuation"]
    if curr["continuation"] >= 65 and cont_delta > 0:
        fragments.append("TP2 probability high")
    elif curr["continuation"] <= 35 and cont_delta < 0:
        fragments.append("TP2 probability low")

    # Stop risk — only surfaced when trend health is genuinely deteriorating
    # (not just one noisy metric), so it stays a meaningful warning rather
    # than a default pessimistic tack-on.
    health_delta = curr["health"] - prev["health"]
    if health_delta <= -10 and (adx_delta < 0 or accel_delta < 0):
        fragments.append("stop risk increasing")

    if not fragments:
        # Fallback: still say something concrete rather than a vague phrase,
        # using whichever of the four composite scores actually moved most.
        deltas = {
            "continuation": curr["continuation"] - prev["continuation"],
            "acceleration": curr["acceleration"] - prev["acceleration"],
            "health":       curr["health"]       - prev["health"],
            "confidence":   curr["confidence"]   - prev["confidence"],
        }
        key = max(deltas, key=lambda k: abs(deltas[k]))
        d = deltas[key]
        labels = {
            "continuation": ("Higher TP potential", "Lower TP potential"),
            "acceleration": ("Trend accelerating", "Trend losing strength"),
            "health":       ("Trend getting healthier", "Momentum weakening"),
            "confidence":   ("Higher confidence", "Lower confidence"),
        }
        fragments.append(labels[key][0] if d > 0 else labels[key][1])

    summary = ", ".join(fragments)
    prefix = "🟢 " if not any(w in summary for w in ("weakening", "loss", "decreasing", "diverging", "Lower", "risk increasing")) else "⚠️ "
    return prefix + (summary[0].upper() + summary[1:] if summary else summary)

def _atie_elapsed_since(opened_at: str) -> str:
    """Notification-format improvement: elapsed time since signal generation,
    e.g. '⏱️ 18 minutes after entry' / '⏱️ 2h 15m after entry'."""
    try:
        opened = datetime.fromisoformat(opened_at)
        minutes = max(0, int((datetime.now(timezone.utc) - opened).total_seconds() / 60))
    except Exception:
        return "⏱️ time after entry unavailable"
    if minutes < 60:
        return f"⏱️ {minutes} minutes after entry"
    hours, mins = divmod(minutes, 60)
    return f"⏱️ {hours}h {mins}m after entry"

def _atie_build_message(symbol: str, prev: dict, curr: dict, reasons: list[str], reward: str,
                         weakening: bool, elapsed_line: str, bos_now: bool = False) -> str:
    """Notification-format improvement (reduce Telegram spam / make messages compact):
    - Symbol is always the first line (🚀 strengthening / ⚠️ weakening).
    - Only metrics that actually changed are shown (no 31→31 style no-ops).
    - Beginner-friendly labels ("Continuation: 31 → 45") instead of bare numbers.
    - One short plain-language line instead of a long sentence.
    - No wasted blank lines — compact, single-line-per-fact layout.
    """
    header_emoji = "⚠️" if weakening else "🚀"
    lines = [f"{header_emoji} {symbol}"]

    metric_labels = [
        ("continuation", "📈", "Continuation"),
        ("acceleration", "🚀", "Acceleration"),
        ("health",       "🛡", "Trend Health"),
        ("confidence",   "⭐️", "Confidence"),
    ]
    for key, emoji, label in metric_labels:
        prev_v, curr_v = round(prev[key]), round(curr[key])
        if prev_v != curr_v:  # only display values that actually changed
            lines.append(f"{emoji} {label}: {prev_v} → {curr_v}")

    lines.extend(reasons)  # every changed reason (ADX/Rel.Vol/ATR/BOS) — never hidden
    lines.append(f"🎯 Estimated Reward: {reward.replace('-', '–')}")
    lines.append(elapsed_line)
    lines.append(_atie_short_explanation(prev, curr, bos_now))
    return "\n".join(lines)

async def _atie_get_candles_cached(session, cache: dict, symbol: str, interval: str, limit: int) -> list:
    """Per-cycle in-memory cache so multiple active signals on the same symbol
    never trigger duplicate candle fetches within one ATIE monitoring pass.
    Cache lives only for the duration of a single cycle (created fresh in
    atie_monitor_loop below) — no cross-cycle staleness risk."""
    key = (symbol, interval, limit)
    if key in cache:
        return cache[key]
    candles = await get_candles(session, symbol, interval, limit=limit)
    cache[key] = candles
    return candles

async def _atie_check_signal(session, state: dict, row: dict, candle_cache: dict):
    """Evaluate one active signal for ATIE and send a Telegram update only if
    warranted. Never touches signals/results — read-only against them, and
    only ever writes to ATIE's own atie_state table."""
    strategy = row["strategy"]
    cfg = get_strategy_settings(state, strategy)
    if not cfg.get("ai_trade_intelligence_enabled", False):
        return

    symbol, direction = row["symbol"], row["direction"]
    entry, stop = row["entry"], row["stop"]

    candles_1h = await _atie_get_candles_cached(session, candle_cache, symbol, "1h", 150)
    if not candles_1h or len(candles_1h) < 30:
        return
    candles_4h = await _atie_get_candles_cached(session, candle_cache, symbol, "4h", 60)

    continuation = _atie_continuation_score(candles_1h, candles_4h, direction)
    acceleration = _atie_acceleration_score(candles_1h)
    health = _atie_health_score(candles_1h, candles_4h, direction)
    confidence = _atie_confidence_score(candles_1h, candles_4h)
    avg_score = (continuation + acceleration + health + confidence) / 4
    tier = _atie_tier(avg_score)

    # HTF Structure: reuses the already-fetched candles_4h (no extra candle
    # fetch, no extra latency) and the existing _structure_trend_up() helper —
    # only used here to detect a change worth mentioning, never fed back into
    # scoring/TP/SL/signal generation.
    structure_up = _structure_trend_up(candles_4h) if candles_4h else None
    if structure_up is None:
        htf_structure = "Unclear"
    elif (direction == "BUY" and structure_up) or (direction == "SELL" and not structure_up):
        htf_structure = "Aligned"
    else:
        htf_structure = "Not Aligned"

    curr = {
        "continuation": continuation, "acceleration": acceleration,
        "health": health, "confidence": confidence, "tier": tier,
        "adx": round(calc_adx(candles_1h, 14), 1),
        "rel_vol": round(entry_volume_ratio(candles_1h, 20), 2),
        "atr": calc_atr(candles_1h, 14),
        "htf_structure": htf_structure,
    }

    prev = _atie_get_state(row["id"])
    if prev is None:
        # First observation for this signal establishes the baseline only —
        # nothing to compare against yet, so no message is sent (anti-spam).
        _atie_save_state(row["id"], curr, update_count=0)
        return

    prev_avg = (prev["continuation"] + prev["acceleration"] + prev["health"] + prev["confidence"]) / 4

    # ── Strict major-condition gate ──────────────────────────────────────────
    # A Trend Update is sent ONLY if at least one of these is true. Every
    # value reused here (adx/rel_vol/atr/htf_structure/tier/scores) was
    # already computed above or is cached from this same check — nothing is
    # recalculated, so this adds no extra latency.
    tier_changed = curr["tier"] != prev.get("tier")

    # 1) Estimated Reward moved to a different range (e.g. "4R-6R" -> "5R-7R").
    #    Reuses the same estimator already used for the outgoing message —
    #    just called once against prev's continuation/health against the
    #    current market snapshot, no new candle fetch.
    prev_reward = _atie_estimate_reward(prev["continuation"], prev["health"], candles_4h, direction, entry, stop)
    reward = _atie_estimate_reward(continuation, health, candles_4h, direction, entry, stop)
    reward_range_changed = reward != prev_reward

    # 2) Confidence changed significantly on its own.
    confidence_significant = abs(curr["confidence"] - prev["confidence"]) >= ATIE_CONFIDENCE_SIGNIFICANT_DELTA

    # 3) Overall trend quality changed significantly (tier flip, or a big move
    #    in the composite average of all 4 scores).
    quality_changed_significantly = tier_changed or abs(avg_score - prev_avg) >= ATIE_QUALITY_SIGNIFICANT_DELTA

    # 4) Multiple important factors moved together (ADX, Relative Volume, ATR,
    #    a fresh BOS, HTF Structure flip) — any single one of these alone is
    #    just noise, but several moving together is a real regime change.
    bos_now = _atie_new_bos(candles_1h, direction)
    factor_hits = 0
    factor_hits += 1 if abs(curr["adx"] - prev["adx"]) >= ATIE_ADX_FACTOR_DELTA else 0
    factor_hits += 1 if abs(curr["rel_vol"] - prev["rel_vol"]) >= ATIE_RELVOL_FACTOR_DELTA else 0
    if prev["atr"] and prev["atr"] > 0:
        atr_pct = abs((curr["atr"] - prev["atr"]) / prev["atr"] * 100)
        factor_hits += 1 if atr_pct >= ATIE_ATR_FACTOR_PCT else 0
    factor_hits += 1 if bos_now else 0
    factor_hits += 1 if (prev.get("htf_structure") and curr["htf_structure"] != prev["htf_structure"]) else 0
    multiple_factors_together = factor_hits >= ATIE_MULTI_FACTOR_MIN

    significant = (
        reward_range_changed
        or confidence_significant
        or quality_changed_significantly
        or multiple_factors_together
    )

    if not significant:
        # No meaningful change — silently refresh the stored snapshot (so the
        # next real comparison is against the freshest values) without
        # sending anything and without consuming an update slot.
        log.debug(
            f"Trend Update suppressed (no major condition met) | signal_id={row['id']} "
            f"signal_uid={row.get('signal_uid')} {row.get('symbol')} avg={avg_score:.0f} "
            f"factor_hits={factor_hits}"
        )
        _atie_save_state(row["id"], curr, update_count=prev.get("update_count", 0))
        return

    max_updates = ATIE_MAX_UPDATES_EXCEPTIONAL if curr["tier"] == "Exceptional" else ATIE_MAX_UPDATES_NORMAL
    update_count = prev.get("update_count", 0)
    if update_count >= max_updates:
        _atie_save_state(row["id"], curr, update_count=update_count)
        return

    notif = get_signal_notification_row(row["id"])
    tg_message_id = notif.get("tg_message_id") if notif else None
    if not tg_message_id:
        _atie_save_state(row["id"], curr, update_count=update_count)
        return

    weakening = avg_score < prev_avg
    reasons = _atie_build_reasons(prev, curr, bos_now)
    # reward already computed above during the major-condition gate check
    elapsed_line = _atie_elapsed_since(row.get("opened_at")) if row.get("opened_at") else "⏱️ time after entry unavailable"
    text = _atie_build_message(symbol, prev, curr, reasons, reward, weakening, elapsed_line, bos_now)
    signal_uid = row.get("signal_uid")
    # Append the signal's existing Signal ID at the very end, two blank
    # lines before it, so Trend Health/Confidence/Momentum updates are
    # always traceable to the exact signal they belong to.
    text += f"\n\n🆔 <code>{signal_uid if signal_uid else row['id']}</code>"

    try:
        await send_telegram(session, text, TELEGRAM_CHAT_ID, reply_to_message_id=tg_message_id)
        _atie_save_state(row["id"], curr, update_count=update_count + 1)
        log.info(
            f"Trend Update generation | signal_id={row['id']} signal_uid={signal_uid} {symbol} "
            f"tier={curr['tier']} avg={avg_score:.0f} weakening={weakening}"
        )
    except Exception as e:
        log.error(f"ATIE send_telegram failed for signal_id={row['id']} signal_uid={signal_uid}: {e}")
        _atie_save_state(row["id"], curr, update_count=update_count)

async def atie_monitor_loop(session, state):
    """Independent background task for the AI Trade Intelligence Engine.
    Runs on its own interval (default 5 min, configurable), completely
    separate from scan_loop() — it never delays or blocks signal generation.
    Only ever monitors signals already ENTERED/TP1_HIT (i.e. active, not yet
    at TP2 or SL) and only for strategies where ATIE has been explicitly
    enabled (per-strategy, default Disabled)."""
    while True:
        interval = get_atie_check_interval()
        try:
            conn = get_db_connection()
            try:
                rows = conn.execute("""
                    SELECT s.id, s.symbol, s.direction, s.strategy, s.entry, s.stop, s.tp1, s.tp2,
                           s.opened_at, s.signal_uid
                    FROM signals s JOIN results r ON r.signal_id = s.id
                    WHERE r.status IN ('ENTERED','TP1_HIT')
                """).fetchall()
                rows = [dict(r) for r in rows]
            finally:
                conn.close()

            if rows:
                candle_cache: dict = {}
                for row in rows:
                    try:
                        await _atie_check_signal(session, state, row, candle_cache)
                    except Exception as e:
                        log.debug(f"ATIE check error for signal_id={row.get('id')} {row.get('symbol')}: {e}")
        except Exception as e:
            log.error(f"ATIE monitor loop error: {e}")
        await asyncio.sleep(max(60, interval))

# ═══════════════════════════════════════════════════════════════════════════
# END AI TRADE INTELLIGENCE ENGINE (ATIE)
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    log.info("AFEE TRADER BOT starting up...")
    init_database()
    _migrate_json_trades_to_db()
    _init_backtest_queue()
    init_atie_tables()  # NEW: AI Trade Intelligence Engine — own table only, additive
    await _recover_interrupted_replay_jobs()  # mark any RUNNING replay left over from a crash as PARTIAL/INTERRUPTED
    state = load_state()
    if not os.path.exists(BOT_CONFIG_FILE):
        # Auto Create (feature): if bot_config.json is missing at startup,
        # create it now from the current default/loaded configuration so the
        # bot always has a config file to export/import against.
        try:
            save_state(state)
            log.info(f"{BOT_CONFIG_FILE} not found — created automatically with the current default configuration.")
        except Exception as e:
            log.error(f"Could not auto-create {BOT_CONFIG_FILE} at startup: {e}")
    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        from datetime import timedelta
        iran_tz = timezone(timedelta(hours=3, minutes=30))
        now_iran = datetime.now(iran_tz)
        try:
            import jdatetime
            jdt = jdatetime.datetime.fromgregorian(datetime=now_iran.replace(tzinfo=None))
            started_str = jdt.strftime("%Y/%-m/%-d, %H:%M:%S")
        except ImportError:
            started_str = now_iran.strftime("%Y-%m-%d, %H:%M:%S")

        await send_telegram(session,
            "📡 <b>Auto Trading Signal | AI Analysis 🤖</b>\n"
            "🐍 <b>AI AFEE TRADER</b> 🐍\n\n"
            "💎 <b>Symbol:</b> BOT\n"
            "🟢 <b>Signal:</b> START AFEE BOT\n"
            f"⏰ <b>Started:</b> {started_str}\n"
            f"🔖 <b>Version:</b> {BOT_VERSION}\n"
            "━━━━━━━━━━━━━━━\n"
            "<blockquote>🤖 This signal is automatically generated by the advanced AI trading robot "
            "AFEE TRADER based on real-time data analysis.</blockquote>\n\n"
            "@AFEETRADER\n\n"
            "To turn the robot on or off: enter /start or /stop. "
        )

        async def scan_loop():
            while True:
                try:
                    await scan_once(session, state)
                except Exception as e:
                    log.error(f"Scan error: {e}")
                log.info(f"Sleeping {SCAN_INTERVAL}s...")
                await asyncio.sleep(SCAN_INTERVAL)

        async def daily_report_loop():
            """Redesigned Daily Report (feature): the automatic report always
            covers one complete Asia/Tehran calendar day (00:00 -> 23:59:59.999
            Asia/Tehran) and is published exactly once, right at the 00:00
            Asia/Tehran rollover.
            The PRIMARY trigger for that is daily_report_midnight_scheduler()
            below, which sleeps precisely until 00:00:00 Asia/Tehran and
            fires independently of scan_loop/signals/user actions — so
            publication never waits on a new event. This loop (and the calls
            after every scan_once()/signal/close) is a secondary safety net
            only: it re-runs the same rollover check on a 5-minute cadence
            purely in case the midnight scheduler task itself ever dies, so
            the day's report still goes out (slightly late) rather than
            never at all. It never publishes anything the midnight scheduler
            wouldn't have."""
            while True:
                try:
                    trades = await update_trade_outcomes(session)
                    await sync_daily_report(session, state, trades)
                except Exception as e:
                    log.error(f"Daily report loop error: {e}")
                await asyncio.sleep(300)

        async def weekly_reweight_loop():
            """هر ۷ روز وزن همه استراتژی‌ها را بازمحاسبه می‌کند."""
            while True:
                try:
                    if state.get("adaptive_ranking_enabled", True):
                        last_str = state.get("last_reweight_date")
                        now_utc = datetime.now(timezone.utc)
                        should_run = False
                        if last_str is None:
                            should_run = True
                        else:
                            try:
                                last_dt = datetime.fromisoformat(last_str)
                                should_run = (now_utc - last_dt).total_seconds() >= 7 * 24 * 3600
                            except Exception:
                                should_run = True

                        if should_run:
                            log.info("Running weekly adaptive strategy reweighting...")
                            old_weights = get_all_strategy_weights()
                            new_weights = update_all_strategy_weights()
                            report = build_weight_update_report(old_weights, new_weights)
                            for aid in state.get("admins", {}):
                                await send_msg(session, aid, report)
                            state["last_reweight_date"] = now_utc.isoformat()
                            save_state(state)
                            log.info("Weekly reweighting done.")
                except Exception as e:
                    log.error(f"Weekly reweight error: {e}")
                await asyncio.sleep(3600)

        async def chart_snapshot_cleanup_loop():
            """AI CHART ANALYSIS — background retention cleanup (additive,
            isolated). Runs at most once a day; the actual DELETE happens in
            a worker thread (asyncio.to_thread) so a large chart_snapshots
            table can never block the event loop — meaning it can never
            delay scan_loop()/signal generation, Telegram polling, or
            anything else running concurrently. Any failure here is only
            logged; it can never affect trading logic, signals, or stats."""
            while True:
                try:
                    last_str = state.get("last_chart_snapshot_cleanup_date")
                    now_utc = datetime.now(timezone.utc)
                    should_run = True
                    if last_str:
                        try:
                            last_dt = datetime.fromisoformat(last_str)
                            should_run = (now_utc - last_dt).total_seconds() >= 24 * 3600
                        except Exception:
                            should_run = True
                    if should_run and CHART_FEATURE_AVAILABLE:
                        deleted = await asyncio.to_thread(
                            _analysis_snapshot.cleanup_old_snapshots,
                            CHART_SNAPSHOT_RETENTION_DAYS, DB_FILE,
                        )
                        if deleted is not None:
                            log.info(f"Chart snapshot cleanup: removed {deleted} snapshot(s) "
                                     f"older than {CHART_SNAPSHOT_RETENTION_DAYS} days.")
                        else:
                            log.warning("Chart snapshot cleanup failed (non-fatal); will retry later.")
                        state["last_chart_snapshot_cleanup_date"] = now_utc.isoformat()
                        save_state(state)
                except Exception as e:
                    log.error(f"Chart snapshot cleanup loop error: {e}")
                await asyncio.sleep(3600)

        await asyncio.gather(
            poll_updates(session, state),
            scan_loop(),
            daily_report_midnight_scheduler(session, state),  # PRIMARY trigger: fires exactly at 00:00 Asia/Tehran
            daily_report_loop(),                              # secondary safety net (5-min polling)
            weekly_reweight_loop(),
            weekly_report_midnight_scheduler(session, state),   # NEW: PRIMARY trigger — fires exactly at 00:00 Asia/Tehran every Saturday (covers full Sat->Fri week, see item 1 boundary fix)
            monthly_report_midnight_scheduler(session, state),  # NEW: PRIMARY trigger — fires exactly at 00:00 Asia/Tehran on the 1st of each month
            weekly_monthly_report_safety_loop(session, state),  # NEW: secondary safety net for both (5-min polling)
            backtest_worker_loop(session, state),   # جدید: background backtest worker
            atie_monitor_loop(session, state),      # NEW: AI Trade Intelligence Engine — independent trade-management monitor
            chart_snapshot_cleanup_loop(),           # NEW: AI Chart Analysis — background snapshot retention cleanup
        )

# ─── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "blacklist" and len(sys.argv) > 2:
            for sym in sys.argv[2:]:
                add_to_blacklist(sym.upper() + ("USDT" if not sym.upper().endswith("USDT") else ""))
            print("Blacklist:", load_blacklist())
        elif cmd == "show-blacklist":
            print("Blacklist:", load_blacklist())
        elif cmd == "remove-blacklist" and len(sys.argv) > 2:
            bl = load_blacklist()
            for sym in sys.argv[2:]:
                bl.discard(sym.upper() + ("USDT" if not sym.upper().endswith("USDT") else ""))
            save_blacklist(bl)
            print("Blacklist:", bl)
        else:
            print("Commands: blacklist <SYM> | show-blacklist | remove-blacklist <SYM>")
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("AFEE TRADER BOT shutting down (KeyboardInterrupt)...")
        except Exception as e:
            log.error(f"AFEE TRADER BOT crashed: {e}")
            raise
        finally:
            log.info("AFEE TRADER BOT shutdown complete.")

