#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""golden_viz_resample_crosscheck.py — pin the TWO VIZ-driver resample copies against drift.

Per the adaptive-resolution ISA (2026-06-09), `ResampleHelper` is a deliberate PER-SKILL DUPLICATE:
each pipeline skill carries its own bucket+envelope helper, no shared module (the user's locked call).
Two of those copies — `TellDataDashboard/scripts/resample.py` and
`TellDataStory/scripts/resample.py` — are the SAME driver (VIZ: span + point budget + min/max/mean
envelope). The Advisor flagged that "same code, two files, no test" is a silent-drift risk. This test
removes that risk by pinning their BEHAVIOR.

What it does NOT do (by design, see HANDOFF-NEXT-3 FIX 3a):
  - It does NOT byte-compare the two files — their docstrings/comments legitimately differ.
  - It does NOT compare against the `AnalyseData` resample copy — that is a DIFFERENT driver
    (explicit grain + Nyquist floor, NO envelope) and is MEANT to differ.

It asserts behavioral equality on the executable surface:
  1. choose_grain(span, budget, native) returns the same (name, seconds, expr) over a wide grid.
  2. native_floor_seconds(label) agrees over every label.
  3. POINT_BUDGET and the GRAINS ladder agree.
  4. read_enveloped(...) returns identical rows + grain on a fixture DB, in BOTH the raw-passthrough
     regime (raw_count <= budget) and the GROUP-BY envelope regime (raw_count > budget).

Run:  uv run --script golden_viz_resample_crosscheck.py        (exit 0 = pinned, 1 = drift)
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

SKILLS = Path(__file__).resolve().parents[2]  # .../skills
DASHBOARD_RESAMPLE = SKILLS / "TellDataDashboard" / "scripts" / "resample.py"
STORY_RESAMPLE = SKILLS / "TellDataStory" / "scripts" / "resample.py"


def _load(path: Path, alias: str):
    """Import a module from an absolute path under a unique alias (both files are named resample.py,
    so a plain import would collide — importlib keys on the alias instead)."""
    spec = importlib.util.spec_from_file_location(alias, path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


def _fixture_db() -> sqlite3.Connection:
    """Tiny in-memory `data` table (the columns read_enveloped touches) with one series spanning a
    few days at ~hourly cadence — enough rows to exercise BOTH regimes by varying the budget."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE data (source TEXT, location_id TEXT, variable TEXT, "
        "timestamp TEXT, value REAL)"
    )
    rows = []
    # 72 hourly points across 3 days, 2024-01-01..03, deterministic sawtooth incl a clear peak.
    for d in range(3):
        for h in range(24):
            ts = f"2024-01-0{d+1}T{h:02d}:00:00+00:00"
            val = float((h % 12) + d * 10) + (100.0 if (d == 1 and h == 12) else 0.0)  # peak marker
            rows.append(("test", "loc1", "var1", ts, val))
    conn.executemany("INSERT INTO data VALUES (?,?,?,?,?)", rows)
    conn.commit()
    return conn


def main() -> int:
    dash = _load(DASHBOARD_RESAMPLE, "viz_resample_dashboard")
    story = _load(STORY_RESAMPLE, "viz_resample_story")
    failures: list[str] = []

    # --- 3. constants agree -------------------------------------------------
    if dash.POINT_BUDGET != story.POINT_BUDGET:
        failures.append(f"POINT_BUDGET differs: {dash.POINT_BUDGET} vs {story.POINT_BUDGET}")
    if dash.GRAINS != story.GRAINS:
        failures.append("GRAINS ladder differs between the two VIZ copies")
    if dash._NATIVE_FLOOR_SECONDS != story._NATIVE_FLOOR_SECONDS:
        failures.append("_NATIVE_FLOOR_SECONDS differs between the two VIZ copies")

    # --- 2. native_floor_seconds parity ------------------------------------
    for label in ["subhourly", "subdaily", "daily", "daily-ish", "irregular", "weird", None]:
        if dash.native_floor_seconds(label) != story.native_floor_seconds(label):
            failures.append(f"native_floor_seconds({label!r}) differs")

    # --- 1. choose_grain parity over a wide grid ---------------------------
    spans = [1, 3, 7, 30, 90, 365, 365 * 5, 365 * 30]  # 1 day .. 30 years
    budgets = [dash.POINT_BUDGET, 100, 1, 0]
    natives = ["subhourly", "subdaily", "daily", "daily-ish", "irregular", None]
    grid = 0
    for span in spans:
        for budget in budgets:
            for native in natives:
                grid += 1
                a = dash.choose_grain(span, budget, native)
                b = story.choose_grain(span, budget, native)
                if a != b:
                    failures.append(f"choose_grain({span},{budget},{native!r}): {a} vs {b}")

    # --- 4. read_enveloped parity, BOTH regimes ----------------------------
    conn = _fixture_db()
    key = ("test", "loc1", "var1", "subhourly")
    # raw-passthrough regime: budget >> raw_count(72)  -> raw path (mean==lo==hi)
    raw_a = dash.read_enveloped(conn, *key, point_budget=10_000)
    raw_b = story.read_enveloped(conn, *key, point_budget=10_000)
    if raw_a != raw_b:
        failures.append("read_enveloped raw-passthrough rows/grain differ")
    if raw_a[1] != "raw":
        failures.append(f"raw-regime grain expected 'raw', got {raw_a[1]!r}")
    # GROUP-BY envelope regime: tiny budget(5) < raw_count(72) -> aggregated path
    env_a = dash.read_enveloped(conn, *key, point_budget=5)
    env_b = story.read_enveloped(conn, *key, point_budget=5)
    if env_a != env_b:
        failures.append("read_enveloped envelope rows/grain differ")
    if env_a[1] == "raw":
        failures.append("envelope-regime should have aggregated (grain != 'raw')")
    # the peak (100-marker) must survive in the envelope's hi band (faithfulness)
    if env_a[1] != "raw":
        max_hi = max(r["hi"] for r in env_a[0])
        if max_hi < 100.0:
            failures.append(f"envelope lost the peak: max hi={max_hi} (<100)")

    print(f"choose_grain grid points compared : {grid}")
    print(f"raw-passthrough rows              : {len(raw_a[0])} (grain={raw_a[1]})")
    print(f"envelope rows                     : {len(env_a[0])} (grain={env_a[1]})")
    if failures:
        print(f"\nGOLDEN CROSS-CHECK FAIL — {len(failures)} drift(s):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nGOLDEN CROSS-CHECK PASS — the two VIZ resample copies are behaviorally identical.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
