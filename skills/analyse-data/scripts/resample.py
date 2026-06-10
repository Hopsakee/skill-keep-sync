#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""resample.py — AnalyseData ResampleHelper (per-skill duplicate; NO cross-skill import).

Read-time resolution for the ANALYSIS driver: the caller states an EXPLICIT grain
(--granularity), bounded below by a Nyquist floor derived from the lag-scan range, and every
series resampled coarser than its native_resolution earns an aliasing caveat. The store is never
touched (read-only); storage stays raw, native-resolution.

Two-driver model (ISA 20260609 adaptive-resolution): this is the ANALYSIS driver (explicit grain
+ Nyquist floor + aliasing caveat). `TellDataDashboard` owns the VIZ driver (span + point
budget + min/max/mean envelope, automatic from zoom) in its own helper — the two are NEVER merged.

Standalone by design (ISA Decision 2026-06-09): each of the three pipeline skills carries its own
small bucket helper in its own scripts/, no shared module, no cross-skill import.

Self-test (ISC-13/17):  uv run --script resample.py --selftest
"""
from __future__ import annotations

# ── Grain ladder, COARSEST -> FINEST ─────────────────────────────────────────
# Each entry: (name, approx bucket seconds, SQL bucket-key expression over the ISO-8601 TEXT
# `timestamp` column). The key only has to be UNIQUE per bucket; the same expr applied to every
# series guarantees identical bucket keys, so cross-series alignment for correlation is exact.
# The 15-min grain floors the minute field to 00/15/30/45. The weekly key uses integer julianday
# arithmetic rather than strftime('%Y-%W', …): measured ~1.8 s faster on a 726k-point series while
# producing identical week buckets (banked from the VIZ driver's perf work).
GRAINS = [
    ("monthly",  30 * 86400, "substr(timestamp,1,7)"),                       # 2024-01
    ("weekly",    7 * 86400, "CAST(julianday(timestamp)/7 AS INTEGER)"),     # week bucket (no strftime)
    ("daily",         86400, "substr(timestamp,1,10)"),                      # 2024-01-01
    ("hourly",         3600, "substr(timestamp,1,13)"),                      # 2024-01-01T00
    ("15min",           900, "substr(timestamp,1,14)||printf('%02d',"        # 2024-01-01T00:15
                             "(cast(substr(timestamp,15,2) AS INTEGER)/15)*15)"),
]
GRAIN_SQL = {name: expr for name, _sec, expr in GRAINS}
GRAIN_SECONDS = {name: sec for name, sec, _expr in GRAINS}
GRAIN_NAMES = [name for name, _sec, _expr in GRAINS]

# meta.native_resolution is a descriptive LABEL, not a number. Map each label to the finest grain
# (in seconds) the series may legitimately be bucketed down to — the floor. A daily-native series
# resampled to a sub-daily grain would be claiming resolution it does not have; a subhourly series
# resampled to monthly hides its sub-monthly dynamics (the aliasing caveat fires for the latter).
_NATIVE_FLOOR_SECONDS = {
    "subhourly": 900,     # down to 15-min buckets
    "subdaily":  3600,    # down to hourly
    "daily":     86400,
    "daily-ish": 86400,
    "irregular": 86400,   # conservative: don't claim sub-daily on irregular timestamps
}
DEFAULT_FLOOR_SECONDS = 86400

# Nyquist: a lead/lag relationship needs at least this many grain-steps in the scanned window to be
# resolvable. Fewer than 2 steps cannot separate "leads" from "contemporaneous" — the grain is then
# too coarse relative to the question and a faster process may be aliased into lag 0.
NYQUIST_MIN_SPAN = 2


def native_floor_seconds(native_resolution: str | None) -> int:
    """meta.native_resolution label -> finest admissible bucket size in seconds (the floor)."""
    if not native_resolution:
        return DEFAULT_FLOOR_SECONDS
    return _NATIVE_FLOOR_SECONDS.get(str(native_resolution).strip().lower(), DEFAULT_FLOOR_SECONDS)


def nyquist_floor_seconds(lag_min: int, lag_max: int) -> int:
    """Minimum admissible RESOLUTION (as a ceiling on bucket seconds) implied by the lag-scan range.

    Returns the COARSEST grain (largest bucket-seconds) that the lag window can support without
    under-sampling the lead/lag it claims to probe. A grain coarser than this is "below the floor"
    and should warn (ISC-18).

    The lag scan is expressed in grain-periods, so Nyquist in those units is grain-invariant: the
    only thing the lag range tells us is HOW MANY steps the window spans. A window of >= 2 steps can
    resolve lead/lag at any grain, so the lag scan imposes no ceiling (return the coarsest grain). A
    window of < 2 steps cannot resolve lead/lag at all — only the finest grain is defensible, so any
    coarser chosen grain warns (return the finest grain's seconds as the ceiling).
    """
    span = abs(int(lag_max) - int(lag_min))
    if span >= NYQUIST_MIN_SPAN:
        return GRAIN_SECONDS[GRAIN_NAMES[0]]    # coarsest (monthly) — lag scan imposes no ceiling
    return GRAIN_SECONDS[GRAIN_NAMES[-1]]        # finest (15min) — tight window needs full resolution


def nyquist_ok(grain_seconds: int, lag_min: int, lag_max: int) -> tuple[bool, str]:
    """(ok, message). ok=False when the chosen grain is coarser than the Nyquist floor (ISC-18)."""
    floor = nyquist_floor_seconds(lag_min, lag_max)
    if grain_seconds <= floor:
        return True, ""
    span = abs(int(lag_max) - int(lag_min))
    return False, (
        f"NYQUIST WARNING: lag scan spans only {span} grain-step(s) (< {NYQUIST_MIN_SPAN}); a grain of "
        f"{grain_seconds}s cannot resolve lead/lag this tight — a faster process may be aliased into "
        f"lag 0. Use a finer --granularity or widen --lag-min/--lag-max."
    )


def aliasing_caveats(keys, grain_name: str, grain_seconds: int, native_map: dict) -> list[str]:
    """Per-series aliasing caveats (ISC-19/34): one for each series whose native_resolution is finer
    than the chosen analysis grain. The store stays raw; this only flags the honest loss of
    resolution introduced by resampling coarser than what was measured."""
    out: list[str] = []
    for key in keys:
        native = (native_map or {}).get(key)
        floor = native_floor_seconds(native)
        if grain_seconds > floor:
            out.append(
                f"ALIASING: series {key} has native resolution '{native or 'unknown'}' (finer than the "
                f"'{grain_name}' analysis grain); sub-{grain_name} dynamics are aliased — lead/lag finer "
                f"than {grain_name} cannot be resolved from this finding."
            )
    return out


# ── self-test (ISC-13/17) ───────────────────────────────────────────────────
def _selftest() -> int:
    keys_ok = set(GRAIN_SQL) == {"15min", "hourly", "daily", "weekly", "monthly"}
    f_wide = nyquist_floor_seconds(0, 3)      # span 3 >= 2 -> coarsest (monthly) ceiling
    f_tight = nyquist_floor_seconds(0, 0)     # span 0 < 2  -> finest (15min) ceiling
    floor_ok = (f_wide == GRAIN_SECONDS["monthly"]) and (f_tight == GRAIN_SECONDS["15min"])
    ok_daily_wide, _ = nyquist_ok(GRAIN_SECONDS["daily"], 0, 3)     # fine
    ok_monthly_tight, _ = nyquist_ok(GRAIN_SECONDS["monthly"], 0, 0)  # should warn
    nyq_ok = ok_daily_wide and (not ok_monthly_tight)
    alias = aliasing_caveats(["rws:ommen.vecht:discharge"], "monthly", GRAIN_SECONDS["monthly"],
                             {"rws:ommen.vecht:discharge": "subhourly"})
    alias_ok = len(alias) == 1 and "ALIASING" in alias[0]
    no_alias = aliasing_caveats(["knmi:273:precipitation"], "daily", GRAIN_SECONDS["daily"],
                                {"knmi:273:precipitation": "daily"})
    no_alias_ok = no_alias == []
    ok = keys_ok and floor_ok and nyq_ok and alias_ok and no_alias_ok
    print(f"grain keys                = {sorted(GRAIN_SQL)} (expect 15min/hourly/daily/weekly/monthly)")
    print(f"nyquist_floor(0,3)        = {f_wide} (expect monthly {GRAIN_SECONDS['monthly']})")
    print(f"nyquist_floor(0,0)        = {f_tight} (expect 15min {GRAIN_SECONDS['15min']})")
    print(f"nyquist_ok(daily,0,3)     = {ok_daily_wide} (expect True)")
    print(f"nyquist_ok(monthly,0,0)   = {ok_monthly_tight} (expect False)")
    print(f"aliasing(subhourly,monthly) -> {len(alias)} caveat(s) (expect 1)")
    print(f"aliasing(daily,daily)       -> {len(no_alias)} caveat(s) (expect 0)")
    print("SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        sys.exit(_selftest())
    print("resample.py — ResampleHelper (analysis driver). Run with --selftest for the ISC-13/17 unit check.")
