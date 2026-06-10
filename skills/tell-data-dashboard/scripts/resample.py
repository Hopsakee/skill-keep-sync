#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""resample.py — TellDataDashboard ResampleHelper (per-skill duplicate; NO cross-skill import).

Read-time resolution for the VIZ driver: pick a bucket grain from the visible span against a
point budget, floored at the series' native_resolution, and read a min/max/mean envelope via
SQL GROUP BY. The store is never touched (read-only); storage stays raw, native-resolution.

Two-driver model (ISA 20260609 adaptive-resolution): this is the VIZ driver (span + budget +
envelope, automatic from zoom). `AnalyseData` owns the ANALYSIS driver (explicit grain +
Nyquist floor) in its own helper — the two are never merged.

Standalone by design (ISA Decision 2026-06-09): each of the three pipeline skills carries its
own small bucket+envelope helper in its own scripts/, no shared module, no cross-skill import.

Self-test (ISC-1):  uv run --script resample.py --selftest
"""
from __future__ import annotations

# ── Single source of truth for the viz point budget (ISC-2) ──────────────────
POINT_BUDGET = 2500

# Grain ladder, COARSEST -> FINEST. Each entry: (name, approx bucket seconds, SQL bucket-key
# expression over the ISO-8601 TEXT `timestamp` column). The key only has to be UNIQUE per bucket;
# the plotted bucket time is MIN(timestamp) (the real first timestamp in the group), independent of
# the key's type. The 15-min grain floors the minute field to 00/15/30/45. The weekly key uses
# integer julianday arithmetic rather than strftime('%Y-%W', …): measured ~1.8 s faster on a 726k-
# point series (strftime parses every row; the cast does too but the SQLite date-fn path is cheaper)
# while producing identical week buckets.
GRAINS = [
    ("yearly",  365 * 86400, "substr(timestamp,1,4)"),                       # 2024
    ("monthly",  30 * 86400, "substr(timestamp,1,7)"),                       # 2024-01
    ("weekly",    7 * 86400, "CAST(julianday(timestamp)/7 AS INTEGER)"),     # week bucket (no strftime)
    ("daily",         86400, "substr(timestamp,1,10)"),                      # 2024-01-01
    ("hourly",         3600, "substr(timestamp,1,13)"),                      # 2024-01-01T00
    ("15min",           900, "substr(timestamp,1,14)||printf('%02d',"        # 2024-01-01T00:15
                             "(cast(substr(timestamp,15,2) AS INTEGER)/15)*15)"),
]

# meta.native_resolution is a descriptive LABEL, not a number. Map each label to the finest grain
# (in seconds) we may legitimately bucket down to — the floor (ISC-5). A daily-native series is
# never bucketed sub-daily; a subhourly series may go as fine as 15-min.
_NATIVE_FLOOR_SECONDS = {
    "subhourly": 900,     # down to 15-min buckets
    "subdaily":  3600,    # down to hourly
    "daily":     86400,
    "daily-ish": 86400,
    "irregular": 86400,   # conservative: don't claim sub-daily on irregular timestamps
}
DEFAULT_FLOOR_SECONDS = 86400


def native_floor_seconds(native_resolution: str | None) -> int:
    """meta.native_resolution label -> finest admissible bucket size in seconds (the floor)."""
    if not native_resolution:
        return DEFAULT_FLOOR_SECONDS
    return _NATIVE_FLOOR_SECONDS.get(str(native_resolution).strip().lower(), DEFAULT_FLOOR_SECONDS)


def choose_grain(span_days: float, point_budget: int, native_resolution: str | None):
    """Map (visible_span, point_budget, native_resolution) -> a SQL bucket grain (ISC-1).

    Returns (name, bucket_seconds, sql_expr) for the FINEST grain that both (a) keeps the visible
    span within the point budget and (b) is no finer than the series' native_resolution floor
    (ISC-5). Floor (b) means a daily-native series, however far you zoom in, never returns
    sub-daily buckets.
    """
    span_seconds = max(float(span_days), 0.0) * 86400.0
    floor_s = native_floor_seconds(native_resolution)
    need_s = (span_seconds / point_budget) if point_budget and point_budget > 0 else 0.0
    target_s = max(need_s, float(floor_s))
    for name, bsec, expr in reversed(GRAINS):     # fine -> coarse: first >= target is the finest fit
        if bsec >= target_s:
            return name, bsec, expr
    return GRAINS[0]                              # span huge / budget tiny -> coarsest (yearly)


def read_enveloped(conn, source, location_id, variable, native_resolution,
                   point_budget=POINT_BUDGET, window=None, span_days=None):
    """Read a series as a min/max/mean envelope, downsampled IN SQL (ISC-3).

    window: optional (lo_iso, hi_iso) inclusive timestamp bounds for zoom; None = full span.
    span_days: optional visible span (the caller usually knows it from the zoom window) — passing it
    lets the grain be chosen without re-deriving it from the data.
    Returns (rows, grain) where rows is a list of dicts {t, mean, lo, hi} with t an ISO-8601 string.
    If the raw point count in the window already fits the budget, raw points are returned
    unaggregated (mean==lo==hi==value) so peaks are never hidden at full zoom (ISC-6 raw path).
    native_resolution is the floor input (ISC-29). The store is only read, never written.

    Cost shape: exactly ONE cheap metadata pass (MIN/MAX/COUNT, no value filter -> index-friendly)
    plus at most ONE heavy pass (the GROUP BY, or the raw select). The metadata pass deliberately
    omits `value IS NOT NULL` so it stays index-only; its COUNT is therefore a raw-row UPPER BOUND,
    which is exactly what the budget gate needs (if the upper bound fits, the filtered set fits too).
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

    # Full-zoom raw path: small enough to ship verbatim, no aggregation, every peak intact.
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


def _iso_days_between(lo_iso: str, hi_iso: str) -> float:
    from datetime import datetime
    try:
        return max((datetime.fromisoformat(hi_iso) - datetime.fromisoformat(lo_iso))
                   .total_seconds() / 86400.0, 0.0)
    except ValueError:
        return 0.0


# ── self-test (ISC-1) ─────────────────────────────────────────────────────────
def _selftest() -> int:
    name30, _, _ = choose_grain(30 * 365.25, POINT_BUDGET, "subhourly")  # pan out 30y -> weekly
    name1, _, _ = choose_grain(1, POINT_BUDGET, "daily")                 # zoom 1d daily -> daily
    name1sub, _, _ = choose_grain(1, POINT_BUDGET, "subhourly")          # 1d subhourly -> 15min
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
    print("resample.py — ResampleHelper (viz driver). Run with --selftest for the ISC-1 unit check.")
