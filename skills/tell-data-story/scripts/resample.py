#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""resample.py — TellDataStory ResampleHelper (per-skill duplicate; NO cross-skill import).

The story is a DISPLAY artifact, so it uses the VIZ driver: pick a bucket grain from the visible
(overlap) span against a point budget, floored at the series' native_resolution, and read a
min/max/mean envelope via SQL GROUP BY. Charts therefore never ship a 726k-point series to the
browser, and a peak is never hidden — the min/max band carries it. The store is never touched
(read-only); storage stays raw, native-resolution.

This is the SAME viz driver the dashboard uses (span + budget + envelope, automatic from the visible
window). `AnalyseData` owns the ANALYSIS driver (explicit grain + Nyquist floor) in its own helper
— the two are NEVER merged.

FAITHFULNESS: this helper only shapes the CHART. The finding values (r, lag, n, label, caveats) are
read verbatim from the `findings` row in tell.py and never pass through here — the envelope cannot
alter a stored statistic.

Standalone by design (ISA Decision 2026-06-09): each of the three pipeline skills carries its own
small bucket+envelope helper in its own scripts/, no shared module, no cross-skill import.

Self-test (ISC-23):  uv run --script resample.py --selftest
"""
from __future__ import annotations

# ── Single source of truth for the viz point budget ──────────────────────────
POINT_BUDGET = 2500

# Grain ladder, COARSEST -> FINEST. (name, approx bucket seconds, SQL bucket-key expr over the
# ISO-8601 TEXT `timestamp` column). The key only has to be UNIQUE per bucket; the plotted bucket
# time is MIN(timestamp). The 15-min grain floors the minute to 00/15/30/45. The weekly key uses
# integer julianday arithmetic (measured faster than strftime on big series, identical weeks).
GRAINS = [
    ("yearly",  365 * 86400, "substr(timestamp,1,4)"),
    ("monthly",  30 * 86400, "substr(timestamp,1,7)"),
    ("weekly",    7 * 86400, "CAST(julianday(timestamp)/7 AS INTEGER)"),
    ("daily",         86400, "substr(timestamp,1,10)"),
    ("hourly",         3600, "substr(timestamp,1,13)"),
    ("15min",           900, "substr(timestamp,1,14)||printf('%02d',"
                             "(cast(substr(timestamp,15,2) AS INTEGER)/15)*15)"),
]

_NATIVE_FLOOR_SECONDS = {
    "subhourly": 900,
    "subdaily":  3600,
    "daily":     86400,
    "daily-ish": 86400,
    "irregular": 86400,
}
DEFAULT_FLOOR_SECONDS = 86400


def native_floor_seconds(native_resolution: str | None) -> int:
    """meta.native_resolution label -> finest admissible bucket size in seconds (the floor)."""
    if not native_resolution:
        return DEFAULT_FLOOR_SECONDS
    return _NATIVE_FLOOR_SECONDS.get(str(native_resolution).strip().lower(), DEFAULT_FLOOR_SECONDS)


def choose_grain(span_days: float, point_budget: int, native_resolution: str | None):
    """Map (visible_span, point_budget, native_resolution) -> the FINEST grain that keeps the span
    within budget and is no finer than the series' native_resolution floor."""
    span_seconds = max(float(span_days), 0.0) * 86400.0
    floor_s = native_floor_seconds(native_resolution)
    need_s = (span_seconds / point_budget) if point_budget and point_budget > 0 else 0.0
    target_s = max(need_s, float(floor_s))
    for name, bsec, expr in reversed(GRAINS):
        if bsec >= target_s:
            return name, bsec, expr
    return GRAINS[0]


def _iso_days_between(lo_iso: str, hi_iso: str) -> float:
    from datetime import datetime
    try:
        return max((datetime.fromisoformat(hi_iso) - datetime.fromisoformat(lo_iso))
                   .total_seconds() / 86400.0, 0.0)
    except ValueError:
        return 0.0


def read_enveloped(conn, source, location_id, variable, native_resolution,
                   point_budget=POINT_BUDGET, window=None, span_days=None):
    """Read a series as a min/max/mean envelope, downsampled IN SQL.

    window: optional (lo_iso, hi_iso) inclusive timestamp bounds; None = full span.
    Returns (rows, grain) where rows is a list of dicts {t, mean, lo, hi}, t an ISO-8601 string.
    If the raw point count in the window already fits the budget, raw points are returned
    unaggregated (mean==lo==hi==value) so peaks are never hidden at full zoom. The store is read-only.
    """
    base = "source=? AND location_id=? AND variable=?"
    params: list = [source, location_id, variable]
    if window:
        base += " AND timestamp>=? AND timestamp<=?"
        params += [window[0], window[1]]

    cur = conn.cursor()
    row = cur.execute(
        f"SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM data WHERE {base}", params
    ).fetchone()
    mn, mx, raw_count = row if row else (None, None, 0)
    raw_count = int(raw_count or 0)

    if raw_count <= point_budget:
        rows = cur.execute(
            f"SELECT timestamp AS t, value AS v FROM data "
            f"WHERE {base} AND value IS NOT NULL ORDER BY timestamp",
            params,
        ).fetchall()
        return [{"t": t, "mean": v, "lo": v, "hi": v} for (t, v) in rows], "raw"

    if span_days is None:
        span_days = _iso_days_between(mn, mx) if (mn and mx) else 0.0
    name, _bsec, expr = choose_grain(span_days, point_budget, native_resolution)
    rows = cur.execute(
        f"SELECT MIN(timestamp) AS t, AVG(value) AS mean, MIN(value) AS lo, MAX(value) AS hi "
        f"FROM data WHERE {base} AND value IS NOT NULL GROUP BY {expr} ORDER BY t",
        params,
    ).fetchall()
    return [{"t": t, "mean": mean, "lo": lo, "hi": hi} for (t, mean, lo, hi) in rows], name


# ── self-test (ISC-23) ───────────────────────────────────────────────────────
def _selftest() -> int:
    name30, _, _ = choose_grain(30 * 365.25, POINT_BUDGET, "subhourly")
    name1, _, _ = choose_grain(1, POINT_BUDGET, "daily")
    name1sub, _, _ = choose_grain(1, POINT_BUDGET, "subhourly")
    ok = (name30 == "weekly") and (name1 == "daily") and (name1sub == "15min")
    print(f"choose_grain(30yr, {POINT_BUDGET}, subhourly) = {name30:<7} (expect weekly)")
    print(f"choose_grain(1day, {POINT_BUDGET}, daily)      = {name1:<7} (expect daily)")
    print(f"choose_grain(1day, {POINT_BUDGET}, subhourly)  = {name1sub:<7} (expect 15min)")
    print("SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        sys.exit(_selftest())
    print("resample.py — ResampleHelper (viz driver, story). Run with --selftest for the ISC-23 unit check.")
