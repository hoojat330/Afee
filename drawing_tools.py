"""
drawing_tools.py
─────────────────────────────────────────────────────────────────────────────
Pure, isolated drawing primitives used by chart_renderer.py.

IMPORTANT (per spec):
- This module NEVER analyzes the market. It only draws objects it is given
  (coordinates, prices, indices). It makes zero trading decisions.
- Every function here takes already-computed values and puts pixels on a
  matplotlib Axes. Nothing in this file talks to Binance, a strategy, or a
  database.
"""

from __future__ import annotations
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D

# ─── Dark, TradingView-like palette ───────────────────────────────────────────
BG_COLOR       = "#131722"
GRID_COLOR     = "#1e222d"
TEXT_COLOR     = "#d1d4dc"
UP_COLOR       = "#26a69a"
DOWN_COLOR     = "#ef5350"
VOL_UP_COLOR   = "#26a69a55"
VOL_DOWN_COLOR = "#ef535055"
ENTRY_COLOR    = "#2962ff"
STOP_COLOR     = "#ef535088"
TP_COLOR       = "#26a69a88"
TP1_COLOR      = "#ffb300"
FIB_COLOR      = "#7e57c2"
ZONE_COLOR     = "#42a5f5"
TREND_COLOR    = "#ffca28"
BOS_COLOR      = "#29b6f6"
CHOCH_COLOR    = "#ab47bc"
WATERMARK_COLOR = "#d1d4dc22"


def new_figure(width_px: int = 1280, height_px: int = 800, dpi: int = 130):
    fig = plt.figure(figsize=(width_px / dpi, height_px / dpi), dpi=dpi)
    fig.patch.set_facecolor(BG_COLOR)
    return fig


def style_axes(ax, grid: bool = True):
    ax.set_facecolor(BG_COLOR)
    for spine in ax.spines.values():
        spine.set_color(GRID_COLOR)
        spine.set_linewidth(0.6)
    ax.tick_params(colors=TEXT_COLOR, labelsize=7)
    if grid:
        # Slightly lighter/finer grid than before so overlays (zones, BOS,
        # fib ladder) read as the visual foreground instead of competing
        # with the background grid — closer to TradingView's subtle grid.
        ax.grid(True, color=GRID_COLOR, linewidth=0.5, alpha=0.55)
    else:
        ax.grid(False)


def plot_candles(ax, candles: list[dict], width: float = 0.6):
    """candles: list of {open, high, low, close}. x-axis is simple integer index."""
    for i, c in enumerate(candles):
        color = UP_COLOR if c["close"] >= c["open"] else DOWN_COLOR
        ax.plot([i, i], [c["low"], c["high"]], color=color, linewidth=1.0, zorder=2)
        lo, hi = sorted([c["open"], c["close"]])
        height = max(hi - lo, (c["high"] - c["low"]) * 0.01)
        ax.add_patch(Rectangle((i - width / 2, lo), width, height,
                                facecolor=color, edgecolor=color, linewidth=0.4, zorder=3))


def plot_volume(ax, candles: list[dict], width: float = 0.6):
    for i, c in enumerate(candles):
        color = VOL_UP_COLOR if c["close"] >= c["open"] else VOL_DOWN_COLOR
        ax.bar(i, c["volume"], width=width, color=color, zorder=2)
    style_axes(ax)
    ax.set_yticks([])


def draw_fibonacci(ax, x_start: float, x_end: float, price_high: float, price_low: float,
                    levels=(0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0), reverse: bool = False):
    """Anchors a fib retracement across [price_low, price_high] over [x_start, x_end].

    The 7 gridlines sit at the same fixed price positions either way (they're
    just the standard Fibonacci ratios evenly spaced between price_low and
    price_high) — `reverse` only changes which LABEL is printed next to each
    line, matching how the strategy actually drew that direction's ladder:

      reverse=False (SELL): 0% labeled at the swing LOW, 100% at the swing
        HIGH — confirmed against a live TradingView chart.
      reverse=True (BUY): 0% labeled at the swing HIGH, 100% at the swing
        LOW instead — per the user's own TradingView convention for a
        bullish retracement (drawn high-to-low). The "0.618" label then
        prints next to the price close to the low, matching where this
        strategy's BUY entry actually sits.

    This is a pure rendering function — it never computes or touches
    entry/stop/tp1/tp2 or any other trading calculation; the label mapping
    here must always be kept in sync with each strategy's own BUY/SELL fib
    price formula (see afee_trader_bot's Hammer Fib / Trigger Fibonacci).
    """
    diff = price_high - price_low
    for lv in levels:
        y = price_low + diff * lv
        label_lv = (1.0 - lv) if reverse else lv
        ax.plot([x_start, x_end], [y, y], color=FIB_COLOR, linewidth=0.8,
                linestyle="--", alpha=0.8, zorder=4)
        ax.text(x_end, y, f" {label_lv:.3f}", color=FIB_COLOR, fontsize=6.5,
                va="center", ha="left", zorder=5)


def draw_trend_line(ax, x1, y1, x2, y2, color=TREND_COLOR, label=None):
    ax.plot([x1, x2], [y1, y2], color=color, linewidth=1.3, zorder=4)
    if label:
        ax.text(x2, y2, f" {label}", color=color, fontsize=7, fontweight="bold",
                 va="bottom", ha="left", zorder=5)


def draw_zone(ax, x_start: float, x_end: float, level: float, tol_pct: float = 0.004,
              label: str = "Zone", color=ZONE_COLOR):
    """TradingView-style S/R zone box.

    `tol_pct` is supplied by the caller and should be the exact tolerance the
    strategy used to detect price touching this level (e.g. the same 0.008
    used by is_valid_zone_breakout()/get_nearest_sr_scored() in the bot) —
    this function only draws a box of the width it's given; it never invents
    or guesses that width itself.
    """
    y0 = level * (1 - tol_pct)
    y1 = level * (1 + tol_pct)
    # Soft fill + solid thin border (TradingView S/R box look), plus a faint
    # centerline at the exact detected level so the anchor price is legible
    # even when the box is thin.
    ax.add_patch(Rectangle((x_start, y0), x_end - x_start, y1 - y0,
                            facecolor=color, alpha=0.12, edgecolor=color,
                            linewidth=1.0, linestyle="-", zorder=1))
    ax.plot([x_start, x_end], [level, level], color=color, linewidth=0.7,
            linestyle=":", alpha=0.55, zorder=1.5)
    ax.text(x_start, y1, f" {label}", color=color, fontsize=7.2, fontweight="bold",
            va="bottom", ha="left", zorder=5,
            bbox=dict(boxstyle="round,pad=0.15", fc=BG_COLOR, ec=color, lw=0.6, alpha=0.8))


def draw_bos_line(ax, x_start: float, x_end: float, level: float,
                   label: str = "BOS", color=BOS_COLOR):
    """Structure-break (BOS/CHOCH) line, TradingView-style: a solid segment
    from the swing point (x_start) to the break point (x_end) at the exact
    structural level the strategy detected (e.g. `swing_ref`), with a short
    dashed continuation and a marker right at the break point.

    Pure rendering — level and x-positions are supplied by the caller and
    are drawn verbatim; this function never computes or infers a level.
    """
    ax.plot([x_start, x_end], [level, level], color=color, linewidth=1.5,
            linestyle="-", alpha=0.9, zorder=4)
    tail = x_end + max((x_end - x_start) * 0.06, 1)
    ax.plot([x_end, tail], [level, level], color=color, linewidth=1.5,
            linestyle="--", alpha=0.5, zorder=4)
    ax.plot([x_end], [level], marker="D", markersize=4.5, color=color, zorder=5,
            markeredgecolor="white", markeredgewidth=0.4)
    ax.text(x_end, level, f" {label}", color=color, fontsize=7.5, fontweight="bold",
            va="bottom", ha="left", zorder=6,
            bbox=dict(boxstyle="round,pad=0.15", fc=BG_COLOR, ec=color, lw=0.7, alpha=0.85))


def draw_marker_label(ax, x, y, text, color, above=True):
    va = "bottom" if above else "top"
    offset = 1 if above else -1
    ax.annotate(text, xy=(x, y), xytext=(x, y), color=color, fontsize=7.5,
                fontweight="bold", ha="center", va=va, zorder=6,
                bbox=dict(boxstyle="round,pad=0.2", fc=BG_COLOR, ec=color, lw=0.8, alpha=0.85))


def draw_buy_sell_arrow(ax, x, y, direction: str):
    color = UP_COLOR if direction == "BUY" else DOWN_COLOR
    marker = "^" if direction == "BUY" else "v"
    y_off = y * 0.996 if direction == "BUY" else y * 1.004
    ax.plot([x], [y_off], marker=marker, markersize=13, color=color, zorder=7,
            markeredgecolor="white", markeredgewidth=0.5)


def draw_position_tool(ax, x_start: float, x_end: float, entry: float, stop: float,
                        tp2: float, direction: str):
    """TradingView-style Long/Short Position tool: entry line + risk box (entry->stop, red)
    + reward box (entry->tp2, green). TP1 is intentionally NOT part of this box (drawn
    separately via draw_tp1_ray, per spec)."""
    risk_lo, risk_hi = sorted([entry, stop])
    reward_lo, reward_hi = sorted([entry, tp2])
    width = x_end - x_start

    ax.add_patch(Rectangle((x_start, risk_lo), width, risk_hi - risk_lo,
                            facecolor=STOP_COLOR, alpha=0.35, edgecolor=DOWN_COLOR,
                            linewidth=0.6, zorder=4))
    ax.add_patch(Rectangle((x_start, reward_lo), width, reward_hi - reward_lo,
                            facecolor=TP_COLOR, alpha=0.35, edgecolor=UP_COLOR,
                            linewidth=0.6, zorder=4))

    ax.plot([x_start, x_end], [entry, entry], color=ENTRY_COLOR, linewidth=1.4, zorder=5)
    ax.text(x_end, entry, f" Entry {entry:.6g}", color=ENTRY_COLOR, fontsize=7.5,
            fontweight="bold", va="center", ha="left", zorder=6)
    ax.plot([x_start, x_end], [stop, stop], color=DOWN_COLOR, linewidth=1.0,
            linestyle="-", zorder=5)
    ax.text(x_end, stop, f" Stop {stop:.6g}", color=DOWN_COLOR, fontsize=7.5,
            va="center", ha="left", zorder=6)
    ax.plot([x_start, x_end], [tp2, tp2], color=UP_COLOR, linewidth=1.0, zorder=5)
    ax.text(x_end, tp2, f" TP2 {tp2:.6g}", color=UP_COLOR, fontsize=7.5,
            va="center", ha="left", zorder=6)


def draw_tp1_ray(ax, x_start: float, x_end: float, tp1: float):
    """TP1 as a distinct Horizontal Ray, per spec (kept out of the Position Tool box)."""
    ax.plot([x_start, x_end], [tp1, tp1], color=TP1_COLOR, linewidth=1.1,
            linestyle="--", zorder=5)
    ax.text(x_end, tp1, f" TP1 {tp1:.6g}", color=TP1_COLOR, fontsize=7.5,
            fontweight="bold", va="center", ha="left", zorder=6)


def add_watermark(ax, lines: list[str]):
    text = "\n".join(lines)
    ax.text(0.015, 0.97, text, transform=ax.transAxes, color=WATERMARK_COLOR,
            fontsize=8, va="top", ha="left", zorder=1, linespacing=1.4)


def add_title(fig, symbol: str, strategy: str, direction: str, score, ts_str: str):
    color = UP_COLOR if direction == "BUY" else DOWN_COLOR
    fig.text(0.01, 0.985, f"{symbol}  ·  {strategy}  ·  {direction}", color=color,
              fontsize=11, fontweight="bold", va="top", ha="left")
    right = f"Score {score}/100" if score is not None else ""
    fig.text(0.99, 0.985, f"{right}   {ts_str}", color=TEXT_COLOR, fontsize=8,
              va="top", ha="right")
