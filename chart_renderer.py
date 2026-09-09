"""
chart_renderer.py
─────────────────────────────────────────────────────────────────────────────
Renders a professional, TradingView-style dark-theme PNG chart from a
completed Signal snapshot (see analysis_snapshot.py for the snapshot shape).

Architecture (per spec):

    Signal  ->  Chart Renderer  ->  PNG  ->  Telegram Sender

This module NEVER makes trading decisions and NEVER re-analyzes the market.
It only draws the objects handed to it in `snapshot["visual"]`, anchored to
the exact candle indices/prices recorded at signal-generation time. If a
strategy didn't produce a given object (e.g. no Fibonacci), it is simply not
drawn.

Failure mode: any exception here must be caught by the caller (see
analysis_snapshot.try_render / the integration in the main bot file) so that
a chart-rendering bug can never block signal delivery.
"""

from __future__ import annotations
import io
from datetime import datetime, timezone

import logging

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import drawing_tools as dt

log = logging.getLogger(__name__)

MAX_DISPLAY_CANDLES = 100  # more context per chart ("~80-120 candles" per
                           # updated spec). This also has a side benefit: most
                           # strategies' entry-tf candle lists (60-100 candles,
                           # see get_candles() call sites) now fit inside the
                           # display window without trimming at all, so the
                           # swing/zone/BOS indices recorded by the strategy
                           # almost never need to be clamped back onto screen.
RIGHT_PAD = 20             # empty candles' worth of space on the right for
                           # Entry/Stop/TP/Fib labels to stay legible

# Same S/R zone tolerance the strategies actually use to detect a price
# touch on a level (see is_valid_zone_breakout()/get_nearest_sr_scored()/the
# zone_tol default in _find_trigger_fib_setup(), all 0.008 = 0.8% in the main
# bot file). The renderer draws zones at this exact width so the box on the
# chart matches the zone the strategy actually reacted to — it is not a
# rendering-side guess.
ZONE_TOL_PCT = 0.008

# FIX (label clipping): the price/volume/RSI axes previously used width=0.90,
# leaving only ~3.5% of the figure's pixel width as a margin to the right of
# the plot area. Entry/Stop/TP1/TP2 labels are drawn with left-alignment
# starting at the rightmost data point, so their text extends further right
# in pixel-space than that — with only ~3.5% margin, longer labels (e.g.
# "Stop 105.234567") were getting cut off by the PNG's right edge. Reducing
# the axes width to 0.74 gives a ~23% margin, comfortably fitting every label
# at the font sizes drawing_tools.py uses, verified against the longest
# realistic label text (see the regression render check).
AXES_LEFT = 0.065
AXES_WIDTH = 0.74


def _trim_and_shift(candles: list[dict], visual: dict) -> tuple[list[dict], dict, int]:
    """Keep only the last MAX_DISPLAY_CANDLES candles and shift every *_idx
    field in `visual` so it still points at the same candle after trimming.
    Never adds/removes analysis — purely a display-window operation."""
    n = len(candles)
    k = max(0, n - MAX_DISPLAY_CANDLES)
    shown = candles[k:]
    shifted = dict(visual or {})
    for key, val in list(shifted.items()):
        if key.endswith("_idx") and isinstance(val, int):
            shifted[key] = max(0, val - k)
    return shown, shifted, k


def _price_at(candles: list[dict], idx: int, field: str = "close") -> float:
    idx = max(0, min(idx, len(candles) - 1))
    return candles[idx][field]


def _draw_strategy_overlay(ax, shown: list[dict], visual: dict, trigger_idx: int):
    vtype = (visual or {}).get("type")
    if not vtype:
        return

    if vtype == "zone_choch":
        zone_level = visual.get("zone_level")
        breakout_idx = visual.get("breakout_idx")
        swing_ref = visual.get("swing_ref")
        if zone_level is not None:
            # FIX (S/R zone width): draw the zone at ZONE_TOL_PCT, the exact
            # tolerance the strategy used to detect a price touch on this
            # level, instead of drawing_tools' generic 0.004 default — the
            # box on the chart now matches the zone the strategy actually
            # reacted to.
            dt.draw_zone(ax, 0, trigger_idx + RIGHT_PAD * 0.4, zone_level,
                         tol_pct=ZONE_TOL_PCT, label="Liquidity Zone")
        if breakout_idx is not None:
            y = shown[breakout_idx]["high"] if visual.get("direction_hint") != "BUY" else shown[breakout_idx]["low"]
            # This marks the liquidity sweep / false breakout beyond the
            # zone — the setup's first event, not the structure break
            # itself. Labeled "Sweep" so it isn't confused with the actual
            # BOS line below (which is now drawn at the real broken level).
            dt.draw_marker_label(ax, breakout_idx, y, "Sweep", dt.BOS_COLOR,
                                  above=(y == shown[breakout_idx]["high"]))
            # FIX (BOS placement): the strategy records `swing_ref` — the
            # exact minor-swing price level whose break confirms the CHOCH.
            # Previously this level was never drawn at all; the renderer
            # only labeled a candle wick. Now draw a real horizontal BOS
            # line at that exact level, from the swing/breakout candle
            # through to the candle that actually broke it (trigger_idx) —
            # no new analysis, just the value the strategy already computed.
            if swing_ref is not None:
                dt.draw_bos_line(ax, breakout_idx, trigger_idx, swing_ref, label="BOS")
        dt.draw_marker_label(ax, trigger_idx, shown[trigger_idx]["close"], "CHOCH",
                              dt.CHOCH_COLOR, above=True)

    elif vtype in ("fib", "fib_impulse", "fib_candle"):
        fib_high = visual.get("fib_high")
        fib_low = visual.get("fib_low")
        hi_idx = visual.get("fib_high_idx")
        lo_idx = visual.get("fib_low_idx")
        if fib_high is not None and fib_low is not None:
            # STRICT: anchor the ladder ONLY to swing index/indices the
            # strategy actually stored. No fallback of any kind — not
            # trigger_idx, not trigger_idx-5, not any other substitute
            # candle — is permitted as an anchor. If neither fib_high_idx
            # nor fib_low_idx is present, the exact swing candle is unknown,
            # so Fibonacci rendering is skipped entirely for this signal
            # rather than drawn from a guessed position.
            known_idxs = [i for i in (hi_idx, lo_idx) if i is not None]
            if known_idxs:
                anchor_idx = min(known_idxs)
                x_end = trigger_idx + RIGHT_PAD * 0.6
                # FIX (Fibonacci orientation, final v2): the underlying grid
                # lines are drawn at the same 7 fixed price positions either
                # way; only the printed labels flip for BUY (reverse=True)
                # so 1.0 prints at the swing low and 0.0 at the swing high,
                # matching the user's own TradingView convention for a
                # bullish retracement. Purely a label/rendering choice — it
                # does not read or affect entry/stop/tp1/tp2.
                dt.draw_fibonacci(ax, anchor_idx, x_end, fib_high, fib_low,
                                   reverse=(visual.get("direction_hint") == "BUY"))
                if hi_idx is not None and lo_idx is not None:
                    # Both swing endpoints are known exactly (their real
                    # candle indices) — connect them with the swing line.
                    dt.draw_trend_line(ax, hi_idx, fib_high, lo_idx, fib_low, label="Swing")
            else:
                log.warning(
                    "chart_renderer: skipping Fibonacci render — strategy "
                    "did not provide fib_high_idx/fib_low_idx (vtype=%r); "
                    "refusing to guess a swing anchor.", vtype
                )
        zone_level = visual.get("zone_level")
        if zone_level is not None:
            dt.draw_zone(ax, 0, trigger_idx + RIGHT_PAD * 0.3, zone_level,
                         tol_pct=ZONE_TOL_PCT, label="Zone")
        trend_wick = visual.get("trend_wick")
        t1_idx = visual.get("t1_idx")
        if trend_wick is not None and t1_idx is not None:
            # Use the same professional BOS-line styling as the zone_choch
            # case above, anchored at the exact trend_wick level / t1_idx
            # the Trigger-Fibonacci strategy already computed.
            dt.draw_bos_line(ax, t1_idx, trigger_idx, trend_wick, label="BOS")

    elif vtype == "exhaustion":
        zone_level = visual.get("zone_level")
        if zone_level is not None:
            dt.draw_zone(ax, 0, trigger_idx + RIGHT_PAD * 0.4, zone_level,
                         tol_pct=ZONE_TOL_PCT, label="Exhaustion Zone")
        dt.draw_marker_label(ax, trigger_idx, shown[trigger_idx]["close"],
                              "Exhaustion", dt.CHOCH_COLOR, above=True)

    elif vtype == "rsi_divergence":
        # Drawn on the RSI sub-panel by the caller (needs its own axes);
        # here we only add a small label on the price panel.
        if visual.get("divergence"):
            label = "Bearish Div." if visual.get("direction_hint") == "SELL" else "Bullish Div."
            dt.draw_marker_label(ax, trigger_idx, shown[trigger_idx]["close"], label,
                                  dt.FIB_COLOR, above=True)


def render_chart(snapshot: dict) -> bytes:
    """snapshot: see analysis_snapshot.build_snapshot(). Returns PNG bytes."""
    candles = snapshot["candles"]
    if not candles or len(candles) < 5:
        raise ValueError("Not enough candles to render a chart")

    visual = snapshot.get("visual") or {}
    shown, visual, _k = _trim_and_shift(candles, visual)
    trigger_idx = len(shown) - 1
    # Respect an explicit trigger_idx from the snapshot if it maps inside the window.
    raw_trigger = snapshot.get("trigger_idx")
    if isinstance(raw_trigger, int):
        shifted_trigger = raw_trigger - _k
        if 0 <= shifted_trigger < len(shown):
            trigger_idx = shifted_trigger

    direction = snapshot.get("direction", "BUY")
    entry = snapshot["entry"]
    stop = snapshot["stop"]
    tp1 = snapshot["tp1"]
    tp2 = snapshot["tp2"]

    has_rsi = visual.get("type") == "rsi_divergence" and visual.get("rsi_series")

    fig = dt.new_figure(1280, 900 if has_rsi else 800)
    if has_rsi:
        ax_price = fig.add_axes([AXES_LEFT, 0.30, AXES_WIDTH, 0.60])
        ax_vol = fig.add_axes([AXES_LEFT, 0.16, AXES_WIDTH, 0.12], sharex=ax_price)
        ax_rsi = fig.add_axes([AXES_LEFT, 0.03, AXES_WIDTH, 0.11], sharex=ax_price)
    else:
        ax_price = fig.add_axes([AXES_LEFT, 0.14, AXES_WIDTH, 0.80])
        ax_vol = fig.add_axes([AXES_LEFT, 0.03, AXES_WIDTH, 0.09], sharex=ax_price)
        ax_rsi = None

    dt.style_axes(ax_price)
    dt.plot_candles(ax_price, shown)
    dt.plot_volume(ax_vol, shown)

    x_end = trigger_idx + RIGHT_PAD
    dt.draw_position_tool(ax_price, trigger_idx, x_end, entry, stop, tp2, direction)
    dt.draw_tp1_ray(ax_price, trigger_idx, x_end, tp1)
    dt.draw_buy_sell_arrow(ax_price, trigger_idx, shown[trigger_idx]["close"], direction)

    visual_with_hint = dict(visual)
    visual_with_hint["direction_hint"] = direction
    _draw_strategy_overlay(ax_price, shown, visual_with_hint, trigger_idx)

    if ax_rsi is not None:
        dt.style_axes(ax_rsi)
        rsi_series = [v if v is not None else float("nan") for v in visual["rsi_series"]]
        rsi_shift = max(0, len(rsi_series) - len(candles))
        rsi_shown = rsi_series[rsi_shift + _k:] if len(rsi_series) >= len(candles) else rsi_series
        xs = list(range(len(rsi_shown)))
        ax_rsi.plot(xs, rsi_shown, color=dt.FIB_COLOR, linewidth=1.0)
        ax_rsi.axhline(70, color=dt.DOWN_COLOR, linewidth=0.6, linestyle="--", alpha=0.6)
        ax_rsi.axhline(30, color=dt.UP_COLOR, linewidth=0.6, linestyle="--", alpha=0.6)
        ax_rsi.set_ylim(0, 100)
        ax_rsi.text(0, 92, "RSI", color=dt.TEXT_COLOR, fontsize=6.5, va="top")

    price_pad = (max(c["high"] for c in shown) - min(c["low"] for c in shown)) * 0.06
    y_lo = min(min(c["low"] for c in shown), stop, tp1, tp2) - price_pad
    y_hi = max(max(c["high"] for c in shown), stop, tp1, tp2) + price_pad
    ax_price.set_ylim(y_lo, y_hi)
    ax_price.set_xlim(-1, x_end + 3)

    watermark_lines = [
        "AFEE AI Analysis",
        snapshot.get("symbol", ""),
        snapshot.get("strategy", ""),
        f"ID {snapshot.get('signal_uid')}" if snapshot.get("signal_uid") else "",
        f"Score {snapshot.get('score')}" if snapshot.get("score") is not None else "",
        snapshot.get("direction", ""),
    ]
    dt.add_watermark(ax_price, [ln for ln in watermark_lines if ln])

    ts = snapshot.get("signal_timestamp")
    try:
        ts_str = datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M UTC") if ts else ""
    except Exception:
        ts_str = ts or ""
    dt.add_title(fig, snapshot.get("symbol", ""), snapshot.get("strategy", ""),
                 direction, snapshot.get("score"), ts_str)

    for ax in (ax_price, ax_vol) + ((ax_rsi,) if ax_rsi is not None else ()):
        ax.tick_params(labelbottom=False)
        ax.set_xticks([])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=dt.BG_COLOR)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def render_live_chart(snapshot: dict, fresh_candles: list[dict]) -> bytes:
    """
    "📸 Live Chart Update" — renders CURRENT market candles (freshly fetched
    at button-press time, on the SAME entry timeframe as the original
    signal), overlaying the ORIGINAL Entry/Stop/TP1/TP2 levels exactly as
    stored in the snapshot.

    This function NEVER recalculates entry/stop/tp1/tp2/score — those are
    read verbatim from `snapshot`. It never re-runs strategy logic and never
    fetches anything itself (the caller passes in `fresh_candles`, already
    fetched via the bot's existing, unmodified get_candles()). This is a
    visualization-only refresh, per spec.
    """
    if not fresh_candles or len(fresh_candles) < 5:
        raise ValueError("Not enough fresh candles to render a live update chart")

    shown = fresh_candles[-MAX_DISPLAY_CANDLES:]
    direction = snapshot.get("direction", "BUY")
    entry = snapshot["entry"]
    stop = snapshot["stop"]
    tp1 = snapshot["tp1"]
    tp2 = snapshot["tp2"]

    fig = dt.new_figure(1280, 800)
    ax_price = fig.add_axes([AXES_LEFT, 0.14, AXES_WIDTH, 0.80])
    ax_vol = fig.add_axes([AXES_LEFT, 0.03, AXES_WIDTH, 0.09], sharex=ax_price)

    dt.style_axes(ax_price)
    dt.plot_candles(ax_price, shown)
    dt.plot_volume(ax_vol, shown)

    # Entry/Stop/TP1/TP2 are drawn as full-width horizontal levels (rather
    # than anchored to the original trigger candle's x-position, which may
    # have long since scrolled out of the fetch window) — this is exactly
    # what "current price vs. the original signal levels" should look like.
    x_start, x_end = 0, len(shown) - 1
    dt.draw_position_tool(ax_price, x_start, x_end, entry, stop, tp2, direction)
    dt.draw_tp1_ray(ax_price, x_start, x_end, tp1)

    # If the original trigger candle is still inside the freshly-fetched
    # window (matched by its exact open_time — no recalculation, just a
    # lookup), mark it so the viewer can see exactly where the signal fired.
    trig_ot = snapshot.get("trigger_open_time")
    if trig_ot is not None:
        idx = next((i for i, c in enumerate(shown) if c.get("open_time") == trig_ot), None)
        if idx is not None:
            dt.draw_buy_sell_arrow(ax_price, idx, shown[idx]["close"], direction)
            dt.draw_marker_label(ax_price, idx, shown[idx]["close"], "Signal",
                                  dt.ENTRY_COLOR, above=True)

    price_pad = (max(c["high"] for c in shown) - min(c["low"] for c in shown)) * 0.06
    y_lo = min(min(c["low"] for c in shown), stop, tp1, tp2) - price_pad
    y_hi = max(max(c["high"] for c in shown), stop, tp1, tp2) + price_pad
    ax_price.set_ylim(y_lo, y_hi)
    ax_price.set_xlim(-1, x_end + 3)

    watermark_lines = [
        "AFEE AI Analysis",
        snapshot.get("symbol", ""),
        snapshot.get("strategy", ""),
        f"ID {snapshot.get('signal_uid')}" if snapshot.get("signal_uid") else "",
        "LIVE UPDATE",
        direction,
    ]
    dt.add_watermark(ax_price, [ln for ln in watermark_lines if ln])

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    dt.add_title(fig, snapshot.get("symbol", ""), snapshot.get("strategy", ""),
                 direction, snapshot.get("score"), f"Live @ {now_str}")

    for ax in (ax_price, ax_vol):
        ax.tick_params(labelbottom=False)
        ax.set_xticks([])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=dt.BG_COLOR)
    plt.close(fig)
    buf.seek(0)
    return buf.read()
