# /// script
# requires-python = ">=3.11"
# dependencies = ["fastlite", "pandas", "scipy", "numpy", "statsmodels"]
# ///
"""AnalyseData — stage 2 of the modular data pipeline (FetchData -> AnalyseData -> TellDataStory).

Rung 2: correlation + anomaly + trend + fact, with STL deseasonalization, an effective-sample-size
p-value, a two-sided lag scan, and stable-hypothesis-identity dedup. DOMAIN-AGNOSTIC. Reads a SQLite
package's `data` + `meta`, finds patterns with MANDATORY caveats, optionally grounds correlations
against a research-derived hypothesis baseline (--baseline), writes them idempotently to the
`findings` table via fastlite, and prints a short human-readable summary.

Hard rules:
  - NEVER mutates `data`. NEVER charts. NEVER touches FetchData/scripts/store.py.
  - fastlite for all SQLite access (no stdlib sqlite3).
  - No hardcoded hydrology: period/lag/variables are CLI params or come from the baseline/meta.
  - Research lookups happen in the AGENT layer (PKW_LIBRARIAN / research skills) and are handed in
    via --baseline FILE. This script is deterministic; it does not call the network.

Finding-types are SELECTED with --findings (comma list from {correlation,anomaly,trend,fact}). The
DEFAULT is `correlation` ONLY, so a no-new-flags run is byte-for-byte rung-1 behaviour (idempotency
preserved). The other types fire only when explicitly named. STL is OPT-IN (--deseasonalize stl);
`monthly` and `none` are unchanged. All finding-types build on the SQL-resampled wide frame from
load_wide() — never a full-raw pandas pivot.
"""
# ============================ RUNG-2 (IMPLEMENTED) ===========================
# All rung-2 roadmap items below are now implemented; kept as a map of WHERE each lives.
#  DONE(rung2-findingtypes): anomaly / trend / fact alongside correlation — run_anomaly / run_trend /
#       run_fact; each one-job + caveated; gated behind --findings (default correlation only).
#  DONE(rung2-stl): STL deseasonalization via statsmodels (deseasonalize(..., 'stl', grain=...)):
#       regular-frequency reindex + interior interpolation, STL(period=grain-period, robust=True),
#       residual reindexed back to the OBSERVED index; short-series fallback = series - mean + caveat.
#  DONE(rung2-effN): effective-sample-size p (Dawdy-Matalas / Bartlett lag-1 correction) recomputed
#       from the t-stat with df = n_eff - 2, then Bonferroni-over-lags ON TOP. Default on; --no-eff-n
#       disables. n_eff/r1x/r1y recorded in evidence; the independence caveat names the adjustment.
#  DONE(rung2-twosided): --two-sided forces a symmetric +/- scan (lag_min = -lag_max) so reverse-lead
#       pairs surface without a hand-set negative --lag-min. best_lag_corr already handles negative.
#  DONE(rung2-dedup): baseline findings carry a STABLE hypothesis identity embedded in `period` as a
#       `[hyp:{a_tok}~{b_tok}]` tag on BOTH absent and resolved strings; write_findings deletes by the
#       tag so an absent->resolved transition is superseded. Non-baseline keeps (type,columns,period).
# =============================================================================
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from fastlite import Database

# Per-skill ResampleHelper (ANALYSIS driver: explicit grain + Nyquist floor + aliasing caveat).
# Same-skill sibling import only — NO cross-skill import (ISA Decision 2026-06-09: each pipeline
# skill carries its own bucket helper). `uv run --script analyse.py` puts this dir on sys.path[0].
from resample import (
    GRAIN_SQL,
    GRAIN_SECONDS,
    GRAIN_NAMES,
    aliasing_caveats,
    nyquist_ok,
)

SKILL_VERSION = "0.2.0"
# grain name -> SQL bucket-key expression over the ISO-8601 TEXT `timestamp` column (ISC-13).
# Resolution is a read-time choice; the store stays raw (locked contract 2026-06-06).
_GRAIN_PERIOD = dict(GRAIN_SQL)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- IO (fastlite)

def open_db(path: str) -> Database:
    db = Database(str(Path(path).expanduser()))
    # 64 MB page cache (ISC-20): the default -2000 (2 MB) thrashes on a 552 MB package; bumping it
    # keeps the resample GROUP BY scan in memory. Negative => KiB (so -65536 = 64 MiB).
    db.conn.execute("PRAGMA cache_size=-65536")
    return db


def load_wide(db: Database, granularity: str) -> pd.DataFrame:
    """Read `data` -> wide frame indexed by period, one column per series, resampled IN SQL (ISC-15).

    Series key = "source:location_id:variable". Resampling to the target granularity is ANALYSIS,
    not fetch — FetchData stores raw; aligning two native resolutions for correlation happens here.

    The bucket aggregation is a single `GROUP BY {bucket_expr}` in SQLite (NOT a pandas pivot over
    every raw row, ISC-16): each (series, bucket) collapses to one AVG(value) row before it ever
    reaches pandas, so a 2.6M-row package loads as a few-thousand-row aggregate. The same bucket-key
    expression is applied to every series, so the keys align exactly across series for correlation;
    the bucket's representative period is MIN(timestamp) within the bucket (1:1 with the key).
    """
    expr = _GRAIN_PERIOD[granularity]
    rows = list(db.q(
        f"SELECT {expr} AS bkey, "
        f"source || ':' || location_id || ':' || variable AS skey, "
        f"MIN(timestamp) AS t, AVG(value) AS v "
        f"FROM data WHERE value IS NOT NULL "
        f"GROUP BY skey, {expr}"
    ))
    if not rows:
        raise SystemExit("No data rows in package — nothing to analyse.")
    df = pd.DataFrame(rows)
    # One representative timestamp per bucket key (global across series) -> identical period index.
    period_for_bucket = df.groupby("bkey")["t"].min()
    df["period"] = pd.to_datetime(df["bkey"].map(period_for_bucket), utc=True).dt.tz_localize(None)
    wide = df.pivot_table(index="period", columns="skey", values="v", aggfunc="mean").sort_index()
    return wide


def load_meta(db: Database) -> pd.DataFrame:
    rows = list(db.q("SELECT * FROM meta"))
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ---------------------------------------------------------------- stats core

# STL seasonal period per analysis grain. STL works on grain-PERIODS, so the period is the number of
# grain-steps in one seasonal cycle (a year for sub-yearly grains; a day for hourly/15-min). These are
# calendar facts about the grain, identical for any domain — NOT hydrology constants.
_STL_PERIOD = {"daily": 365, "weekly": 52, "monthly": 12, "hourly": 24, "15min": 96}
# pandas resample/date_range rule per grain — used to put the series on the REGULAR spacing STL needs.
_STL_FREQ = {"daily": "D", "weekly": "W", "monthly": "MS", "hourly": "h", "15min": "15min"}


def _stl_residual(s: pd.Series, grain: str) -> tuple[pd.Series, str | None]:
    """STL deseasonalize one series on the given analysis grain. Returns (residual_on_observed_index,
    note_or_None). STL needs regular spacing + a seasonal period, so: reindex to a regular freq,
    linearly interpolate INTERIOR gaps (leading/trailing NaN dropped — never extrapolated), run STL
    with the grain's period, take the residual, then reindex the residual BACK to the original
    OBSERVED index so only real observed points survive into the correlation. Too short
    (< 2*period observed points) => no crash: fall back to (series - mean) + a caveat recording the
    skip — so STL stays honest on short series rather than throwing."""
    from statsmodels.tsa.seasonal import STL

    period = _STL_PERIOD.get(grain, 365)
    freq = _STL_FREQ.get(grain, "D")
    observed = s.dropna()
    if len(observed) < 2 * period:
        return (s - s.mean(),
                f"STL skipped for a series: only {len(observed)} observed point(s) < 2*period "
                f"({2 * period}) at the '{grain}' grain; fell back to mean-removal (no seasonal model).")
    # Regular grid spanning the observed range; interpolate INTERIOR gaps only (limit_area='inside').
    grid = pd.date_range(observed.index.min(), observed.index.max(), freq=freq)
    regular = (observed.reindex(observed.index.union(grid))
                       .interpolate(method="time", limit_area="inside")
                       .reindex(grid).dropna())
    if len(regular) < 2 * period:
        return (s - s.mean(),
                f"STL skipped for a series: regular-grid length {len(regular)} < 2*period "
                f"({2 * period}) at '{grain}'; fell back to mean-removal.")
    res = STL(regular, period=period, robust=True).fit().resid
    # Reindex residual back to the ORIGINAL observed index — only measured points feed correlation.
    return (res.reindex(s.index), None)


def deseasonalize(s: pd.Series, method: str, grain: str = "daily") -> pd.Series:
    """Remove seasonality before correlating. Parameterized ON PURPOSE — monthly is a hydro-board
    choice, NOT the skill's only method. STL (opt-in) is the correct general default: auto
    trend/seasonal/residual with the period inferred from the analysis grain, no need to know the
    period up front. `none` and `monthly` are unchanged from rung 1. Returns just the series — for
    the STL-skipped advisory caveat use deseasonalize_noted (which threads the note out)."""
    return deseasonalize_noted(s, method, grain)[0]


def deseasonalize_noted(s: pd.Series, method: str, grain: str = "daily") -> tuple[pd.Series, str | None]:
    """deseasonalize + an optional advisory note (currently only the STL-skipped-for-short-series
    note). Pure: no shared mutable state. Callers that build caveats use this; everything else uses
    the thin deseasonalize wrapper."""
    if method == "none":
        return s, None
    if method == "monthly":
        # Subtract the per-calendar-month climatology (same-month mean across years): "Jan vs Jan".
        clim = s.groupby(s.index.month).transform("mean")
        return s - clim, None
    if method == "stl":
        return _stl_residual(s, grain)
    raise ValueError(f"unknown deseasonalize method: {method!r}")


def best_lag_corr(a: pd.Series, b: pd.Series, lag_min: int, lag_max: int):
    """Scan lag in [lag_min, lag_max] where a LEADS b by `lag` periods (a[t] vs b[t+lag]).
    Returns (lag, r, raw_p, n, k_tested, xa, xb) at the largest |r|, or None if no lag had >=4 points.
    `xa`, `xb` are the ALIGNED series at the best lag (a[t], b[t+lag]) — needed for the eff-N p.

    `k_tested` = how many lags actually had >=4 aligned points. The caller MUST correct the
    reported p for this selection (we keep the max-|r| lag, so the raw single-test p understates
    the false-positive rate). Bonferroni-over-lags is applied downstream. Negative lags are handled
    (b leads a) — `--two-sided` sets lag_min = -lag_max so reverse-lead pairs surface."""
    best = None
    tested = 0
    for lag in range(lag_min, lag_max + 1):
        pair = pd.concat([a, b.shift(-lag)], axis=1).dropna()
        if len(pair) < 4:
            continue
        xa, xb = pair.iloc[:, 0], pair.iloc[:, 1]
        r, p = stats.pearsonr(xa, xb)
        if np.isnan(r):
            continue
        tested += 1
        if best is None or abs(r) > abs(best[1]):
            best = (lag, float(r), float(p), int(len(pair)),
                    xa.reset_index(drop=True), xb.reset_index(drop=True))
    if best is None:
        return None
    lag, r, p, n, xa, xb = best
    return (lag, r, p, n, max(1, tested), xa, xb)


def corrected_p(raw_p: float, k_lags: int) -> float:
    """Bonferroni-over-lags: we searched k lags and kept the best, so penalize the reported p."""
    return min(1.0, raw_p * k_lags)


def _lag1_autocorr(x: pd.Series) -> float:
    """Lag-1 autocorrelation of an aligned series (for the effective-N adjustment). NaN/short -> 0.0
    (no autocorrelation assumed => n_eff == n, the conservative no-adjustment fallback)."""
    x = pd.Series(np.asarray(x, float)).dropna()
    if len(x) < 3:
        return 0.0
    r1 = x.autocorr(lag=1)
    return 0.0 if (r1 is None or np.isnan(r1)) else float(r1)


def eff_n_p(xa: pd.Series, xb: pd.Series, r: float, n: int) -> tuple[float, float, float, float]:
    """Effective-sample-size two-sided p (Dawdy-Matalas / Bartlett lag-1 correction).

    Pearson's p assumes INDEPENDENT samples; serially autocorrelated series (hydrology especially)
    make it optimistic. Deflate the sample size by the two series' lag-1 autocorrelations:
        n_eff = n * (1 - r1x*r1y) / (1 + r1x*r1y),   clamped to [3, n]
    then recompute the two-sided p from the t-statistic with df = n_eff - 2:
        t = |r|*sqrt(df/(1-r^2)),   p = 2*stats.t.sf(t, df).
    Returns (p_effN, n_eff, r1x, r1y). r==+/-1 (perfect, 1-r^2==0) -> p=0.0; df<=0 -> p=1.0."""
    r1x, r1y = _lag1_autocorr(xa), _lag1_autocorr(xb)
    denom = 1.0 + r1x * r1y
    factor = (1.0 - r1x * r1y) / denom if denom != 0 else 1.0
    n_eff = float(np.clip(n * factor, 3.0, float(n)))
    df = n_eff - 2.0
    if df <= 0:
        return 1.0, n_eff, r1x, r1y
    one_minus = 1.0 - r * r
    if one_minus <= 0:
        return 0.0, n_eff, r1x, r1y
    t = abs(r) * np.sqrt(df / one_minus)
    p = float(2.0 * stats.t.sf(t, df))
    return min(1.0, p), n_eff, r1x, r1y


def bh_significant(pvals: list[float], alpha: float = 0.05) -> np.ndarray:
    """Benjamini-Hochberg FDR control. Returns a boolean mask of significant p-values."""
    p = np.asarray(pvals, float)
    n = p.size
    if n == 0:
        return np.zeros(0, bool)
    order = np.argsort(p)
    ranked = p[order]
    thresh = (np.arange(1, n + 1) / n) * alpha
    passed = ranked <= thresh
    if not passed.any():
        return np.zeros(n, bool)
    cutoff = ranked[: np.where(passed)[0].max() + 1].max()
    return p <= cutoff


# ---------------------------------------------------------------- caveats (load-bearing)

def base_caveats(method, lag, lag_min, lag_max, k_lags, keys, rules,
                 grain_name=None, grain_seconds=None, native_map=None, eff_n=True) -> list[str]:
    """Mandatory caveats. Hydrology-specific caveats (e.g. potential-vs-actual ET) are NOT baked
    in — they arrive as data via `rules` (a domain-supplied --caveat-rules file: {match, caveat}).

    When grain info is supplied, a per-series ALIASING caveat (ISC-19/34) is appended for every
    series resampled coarser than its native_resolution. `method` may be 'stl' (residual after auto
    trend/seasonal decomposition). `eff_n` toggles which independence caveat is recorded."""
    cav: list[str] = []
    if method == "none":
        cav.append("NOT deseasonalized — seasonality can inflate Pearson r; treat with caution.")
    elif method == "stl":
        cav.append("Deseasonalized via STL (robust auto trend/seasonal/residual; period inferred from "
                   "the analysis grain) before correlating — correlated on the residual.")
    else:
        cav.append(f"Deseasonalized via {method} climatology before correlating.")
    reverse = "reverse-lead (negative lag) included" if lag_min < 0 else "reverse-lead NOT tested (one-sided scan)"
    cav.append(f"Lag scanned over [{lag_min},{lag_max}]; r found at lag={lag} period(s) — location-dependent, "
               f"not universal; {reverse}.")
    if eff_n:
        cav.append(f"p-value Bonferroni-corrected for K={k_lags} scanned lag(s) ON TOP of an "
                   f"effective-sample-size adjustment (Dawdy-Matalas/Bartlett lag-1 autocorrelation "
                   f"correction): n was deflated to n_eff before the t-test, so the reported p already "
                   f"accounts for serial autocorrelation (see evidence n_eff/r1x/r1y).")
    else:
        cav.append(f"p-value Bonferroni-corrected for K={k_lags} scanned lag(s); Pearson p also assumes "
                   f"independent samples, so serial autocorrelation makes even the corrected p optimistic "
                   f"(effective-N adjustment DISABLED via --no-eff-n).")
    if grain_name is not None and grain_seconds is not None:
        cav.extend(aliasing_caveats(keys, grain_name, grain_seconds, native_map or {}))
    for rule in rules:
        m = str(rule.get("match", "")).lower()
        if m and any(m in str(key).lower() for key in keys):
            cav.append(str(rule.get("caveat", "")))
    return cav


# ---------------------------------------------------------------- finding records + idempotent write

def _columns_json(keys: list[str]) -> str:
    return json.dumps(sorted(keys))


def make_finding(keys, period, r, p, caveats, evidence, created_by) -> dict:
    return dict(
        type="correlation",
        columns=_columns_json(keys),
        period=period,
        statistic=round(float(r), 6),          # Pearson r
        confidence=round(float(max(0.0, 1.0 - p)), 6),  # 1 - lag-corrected p (exact r, p_raw, p_corrected in evidence)
        caveats=json.dumps(caveats),
        evidence=json.dumps(evidence),
        created_at=_now(),
        created_by=created_by,
    )


def make_typed_finding(ftype, keys, period, statistic, confidence, caveats, evidence, created_by) -> dict:
    """Generic finding record for the rung-2 finding-types (anomaly/trend/fact). The `findings` schema
    is fixed (type/columns/period/statistic/confidence/caveats/evidence/created_*); these types reuse
    it without any schema change — `statistic`/`confidence` meaning is per-type, spelled out in
    evidence + caveats. `confidence` is left None where a type has no natural [0,1] confidence."""
    return dict(
        type=ftype,
        columns=_columns_json(keys),
        period=period,
        statistic=None if statistic is None else round(float(statistic), 6),
        confidence=None if confidence is None else round(float(confidence), 6),
        caveats=json.dumps(caveats),
        evidence=json.dumps(evidence),
        created_at=_now(),
        created_by=created_by,
    )


def _hyp_tag(period: str) -> str | None:
    """Extract the stable `[hyp:a~b]` tag a baseline finding embeds in its period string, if present."""
    i = period.find("[hyp:")
    if i < 0:
        return None
    j = period.find("]", i)
    return period[i:j + 1] if j > i else None


def write_findings(db: Database, findings: list[dict]) -> int:
    """Idempotent write. Two dedup keys (DONE rung2-dedup):
      - Baseline findings carry a STABLE hypothesis identity as a `[hyp:a_tok~b_tok]` tag inside the
        `period` string. When present, dedup deletes EVERY prior row whose period contains that tag —
        so an absent->resolved transition is superseded (the absent row, period `baseline-absent (...)
        [hyp:a~b]`, and the resolved row, period `baseline lagX-Y (...) [hyp:a~b]`, share the tag even
        though (type,columns,period) differ across the transition).
      - Non-baseline findings (autonomous/anomaly/trend/fact) have NO tag and keep the rung-1
        (type,columns,period) dedup."""
    tbl = db.t.findings
    written = 0
    for f in findings:
        tag = _hyp_tag(f["period"])
        if tag is not None:
            tbl.delete_where("period LIKE ?", ["%" + tag + "%"])
        else:
            tbl.delete_where(
                "type=? AND columns=? AND period=?", [f["type"], f["columns"], f["period"]]
            )
        tbl.insert(f)
        written += 1
    return written


# ---------------------------------------------------------------- baseline (agent-supplied hypotheses)

def resolve_key(wide_cols: list[str], token: str) -> str | None:
    """Match a baseline token (e.g. 'precipitation' or a full 'src:loc:var' key) to a real column."""
    if token in wide_cols:
        return token
    cands = [c for c in wide_cols if c.split(":")[-1] == token or c.endswith(":" + token)]
    if not cands:
        cands = [c for c in wide_cols if token.lower() in c.lower()]
        if len(cands) > 1:
            print(f"WARNING: baseline token {token!r} fuzzily matches {cands} — using {cands[0]!r}; "
                  f"disambiguate with a full 'source:location_id:variable' key.", file=sys.stderr)
    return cands[0] if cands else None


def label_agreement(expected_sign: str, r: float, p: float, alpha: float) -> str:
    sig = p <= alpha
    same_sign = (r >= 0 and expected_sign == "+") or (r < 0 and expected_sign == "-")
    if sig and same_sign:
        return "consistent"
    if sig and not same_sign:
        return "surprising"
    if (not sig) and same_sign and abs(r) > 0.1:
        return "weaker"
    return "absent"


def run_baseline(db, wide, hyps, method, alpha, created_by, rules,
                 grain_name=None, grain_seconds=None, native_map=None, eff_n=True, two_sided=False):
    findings = []
    summary_rows = []
    for h in hyps:
        a_tok, b_tok = h["a"], h["b"]
        a_key, b_key = resolve_key(list(wide.columns), a_tok), resolve_key(list(wide.columns), b_tok)
        src = h.get("source", "uncited")
        lag_min, lag_max = int(h.get("lag_min", 0)), int(h.get("lag_max", 0))
        if two_sided:
            lag_min = -lag_max          # symmetric +/- scan so reverse-lead pairs surface
        exp = h.get("expected_sign", "+")
        # STABLE hypothesis identity: keyed on the RAW tokens (not resolved keys) so the tag survives
        # the absent->resolved transition. Embedded in the period of BOTH the absent and resolved rows.
        hyp_tag = f"[hyp:{a_tok}~{b_tok}]"
        if a_key is None or b_key is None:
            missing = [t for t, k in ((a_tok, a_key), (b_tok, b_key)) if k is None]
            cav = [
                "Expected by literature baseline but a required series is ABSENT from this package — "
                "no correlation computed (honest gap, not a fabricated finding).",
                f"baseline source: {src}",
            ]
            ev = dict(baseline_label="absent", absent_series=missing, expected_sign=exp,
                      expected_lag=[lag_min, lag_max], baseline_source=src)
            findings.append(dict(
                type="correlation", columns=_columns_json([a_tok, b_tok]),
                period=f"baseline-absent ({a_tok}~{b_tok}) {hyp_tag}", statistic=None, confidence=0.0,
                caveats=json.dumps(cav), evidence=json.dumps(ev),
                created_at=_now(), created_by=created_by))
            summary_rows.append(f"  - {a_tok} ~ {b_tok}: ABSENT — series {missing} not in package (baseline: {src})")
            continue
        a, na = deseasonalize_noted(wide[a_key], method, grain_name or "daily")
        b, nb = deseasonalize_noted(wide[b_key], method, grain_name or "daily")
        res = best_lag_corr(a, b, lag_min, lag_max)
        if res is None:
            summary_rows.append(f"  - {a_key} ~ {b_key}: too few overlapping points to test")
            continue
        lag, r, raw_p, n, k, xa, xb = res
        # Effective-N p (default) then Bonferroni-over-lags ON TOP; --no-eff-n uses the raw Pearson p.
        if eff_n:
            p_eff, n_eff, r1x, r1y = eff_n_p(xa, xb, r, n)
        else:
            p_eff, n_eff, r1x, r1y = raw_p, n, None, None
        cp = corrected_p(p_eff, k)
        lab = label_agreement(exp, r, cp, alpha)
        cav = base_caveats(method, lag, lag_min, lag_max, k, [a_key, b_key], rules,
                           grain_name, grain_seconds, native_map, eff_n=eff_n)
        cav += [n for n in (na, nb) if n]
        cav.append(f"Tested against literature baseline (expected sign {exp}, lag {lag_min}-{lag_max}); "
                   f"agreement: {lab}.")
        cav.append(f"baseline source: {src}")
        ev = dict(r=round(r, 4), p_raw=round(raw_p, 8), p_effn=round(p_eff, 8), p_corrected=round(cp, 8),
                  k_lags=k, lag=lag, n=n, n_eff=(None if n_eff is None else round(n_eff, 2)),
                  r1x=(None if r1x is None else round(r1x, 4)), r1y=(None if r1y is None else round(r1y, 4)),
                  eff_n=eff_n, deseasonalize=method, baseline_label=lab, expected_sign=exp, baseline_source=src)
        findings.append(make_finding([a_key, b_key],
                                     f"baseline lag{lag_min}-{lag_max} ({a_key}~{b_key}) {hyp_tag}",
                                     r, cp, cav, ev, created_by))
        summary_rows.append(
            f"  - {a_key} ~ {b_key}: r={r:+.2f} (p={cp:.3g} corr, lag={lag}, n={n}) -> {lab.upper()} (baseline: {src})")
    return findings, summary_rows


# ---------------------------------------------------------------- autonomous pass

def run_autonomous(wide, method, lag_min, lag_max, alpha, granularity, created_by, rules, max_pairs=200,
                   grain_name=None, grain_seconds=None, native_map=None, eff_n=True, two_sided=False):
    if two_sided:
        lag_min = -lag_max              # symmetric +/- scan so reverse-lead pairs surface
    cols = list(wide.columns)
    pairs = list(combinations(cols, 2))[:max_pairs]
    raw = []
    for a_key, b_key in pairs:
        a, na = deseasonalize_noted(wide[a_key], method, grain_name or "daily")
        b, nb = deseasonalize_noted(wide[b_key], method, grain_name or "daily")
        res = best_lag_corr(a, b, lag_min, lag_max)
        if res is None:
            continue
        lag, r, raw_p, n, k, xa, xb = res
        if eff_n:
            p_eff, n_eff, r1x, r1y = eff_n_p(xa, xb, r, n)
        else:
            p_eff, n_eff, r1x, r1y = raw_p, n, None, None
        cp = corrected_p(p_eff, k)
        notes = [t for t in (na, nb) if t]
        raw.append((a_key, b_key, lag, r, cp, raw_p, p_eff, n, k, n_eff, r1x, r1y, notes))
    if not raw:
        return [], []
    # BH-FDR is fed the lag-scan-CORRECTED p-values (index 4), not the raw ones — else the FDR
    # guarantee is broken at the input by the lag-selection bias. The corrected p now sits on top of
    # the effective-N p (when enabled), so BH-FDR inherits the autocorrelation widening too.
    sig_mask = bh_significant([x[4] for x in raw], alpha)
    findings, summary_rows = [], []
    for (a_key, b_key, lag, r, cp, raw_p, p_eff, n, k, n_eff, r1x, r1y, notes), sig in zip(raw, sig_mask):
        cav = base_caveats(method, lag, lag_min, lag_max, k, [a_key, b_key], rules,
                           grain_name, grain_seconds, native_map, eff_n=eff_n)
        cav += notes
        cav.append("EXPLORATORY: autonomous discovery with no literature baseline — multiple-"
                   "comparisons risk; Benjamini-Hochberg FDR applied at alpha=%.2f on lag-corrected p." % alpha)
        cav.append("BH-FDR significant." if sig else "NOT significant after BH-FDR correction.")
        ev = dict(r=round(r, 4), p_raw=round(raw_p, 8), p_effn=round(p_eff, 8), p_corrected=round(cp, 8),
                  k_lags=k, lag=lag, n=n, n_eff=(None if n_eff is None else round(n_eff, 2)),
                  r1x=(None if r1x is None else round(r1x, 4)), r1y=(None if r1y is None else round(r1y, 4)),
                  eff_n=eff_n, granularity=granularity, deseasonalize=method, baseline_label=None,
                  bh_significant=bool(sig))
        findings.append(make_finding([a_key, b_key],
                                     f"autonomous {granularity} lag{lag_min}-{lag_max} ({a_key}~{b_key})",
                                     r, cp, cav, ev, created_by))
        flag = "*" if sig else " "
        summary_rows.append(f" {flag}{a_key} ~ {b_key}: r={r:+.2f} (p={cp:.3g} corr, lag={lag}, n={n})")
    return findings, summary_rows


# ---------------------------------------------------------------- per-series finding-types (rung 2)

def _period_str(idx) -> str:
    """ISO date/time of a pandas index value (for fact first/last-period evidence)."""
    return pd.Timestamp(idx).isoformat()


def run_anomaly(wide, method, granularity, created_by, rules, z_thresh,
                grain_name=None, grain_seconds=None, native_map=None):
    """Per-series robust outlier detection on the (optionally deseasonalized) series. Modified z-score
    from the median + MAD: mz = 0.6745*(x - median)/MAD (Iglewicz-Hoaglin). Points with |mz| > z_thresh
    are flagged. statistic = max |mz| (the most extreme point); evidence carries the count + top points.
    MAD is robust to the very outliers being detected, unlike std. caveats: deseasonalize state,
    threshold dependence, serial-autocorrelation of consecutive anomalies, and aliasing where coarser
    than native."""
    findings, summary_rows = [], []
    for key in wide.columns:
        s, note = deseasonalize_noted(wide[key], method, grain_name or "daily")
        s = s.dropna()
        if len(s) < 3:
            continue
        med = float(s.median())
        mad = float((s - med).abs().median())
        if mad == 0:
            # Degenerate spread — MAD-based z is undefined; skip honestly rather than divide by zero.
            continue
        mz = 0.6745 * (s - med) / mad
        flagged = mz[mz.abs() > z_thresh]
        max_mz = float(mz.abs().max())
        top = flagged.reindex(flagged.abs().sort_values(ascending=False).index)[:5]
        ev = dict(count=int(len(flagged)), threshold=z_thresh, max_abs_modified_z=round(max_mz, 4),
                  median=round(med, 6), mad=round(mad, 6), n=int(len(s)), deseasonalize=method,
                  top=[{"period": _period_str(i), "value": round(float(s.loc[i]), 6),
                        "modified_z": round(float(mz.loc[i]), 4)} for i in top.index])
        cav = [
            f"Robust outlier detection: modified z = 0.6745*(x-median)/MAD; |z| > {z_thresh} flagged "
            f"({len(flagged)} of {len(s)} points). statistic = max |modified z|.",
            ("Deseasonalized via STL residual before flagging." if method == "stl"
             else "NOT deseasonalized — seasonal extremes can read as anomalies." if method == "none"
             else f"Deseasonalized via {method} climatology before flagging."),
            f"Threshold-dependent: a different --anomaly-z changes which points are flagged (no "
            f"universal cutoff; {z_thresh} is the Iglewicz-Hoaglin default).",
            "Consecutive anomalies are NOT independent (serial autocorrelation) — a multi-period "
            "excursion is one event, not many; counts overstate independent anomalies.",
        ]
        if note:
            cav.append(note)
        if grain_name is not None and grain_seconds is not None:
            cav += aliasing_caveats([key], grain_name, grain_seconds, native_map or {})
        for rule in rules:
            m = str(rule.get("match", "")).lower()
            if m and m in str(key).lower():
                cav.append(str(rule.get("caveat", "")))
        findings.append(make_typed_finding("anomaly", [key], f"anomaly {granularity} ({key})",
                                           max_mz, None, cav, ev, created_by))
        summary_rows.append(f"  - {key}: {len(flagged)} anomaly point(s), max |mz|={max_mz:.2f}")
    return findings, summary_rows


def run_trend(wide, method, granularity, created_by, rules,
              grain_name=None, grain_seconds=None, native_map=None,
              alpha=0.05, window=None, min_n=4, plausible_max=None):
    """Per-series monotonic trend. Theil-Sen slope (robust to outliers) + Mann-Kendall via Kendall's
    tau (tau + p). statistic = Theil-Sen slope in units per grain-period. evidence: slope, intercept,
    tau, p, n, grain, plus a first-class `significant` flag (p<alpha) and a `suspect` flag.

    nl-groundwater-trends learnings folded in:
      - `significant` (MK p < alpha) is recorded explicitly + as `confidence`=1-p, and stated in a
        caveat — a large slope is routinely NOT significant; downstream (the map viz) hatches the
        non-significant ones instead of drawing them solid.
      - `window=(start,end)` fits every series on a COMMON time window so cross-series slopes are
        comparable (differing record end-dates otherwise become fake between-series differences).
      - `plausible_max`: |slope| beyond a domain-plausible bound is flagged `suspect` (likely a datum
        shift / sensor splice) — surfaced, excluded by the viz from the colour scale, never dropped.
      - `min_n`: require >= this many points for a slope (a near-empty series yields no fabricated trend).
    """
    findings, summary_rows = [], []
    for key in wide.columns:
        s, note = deseasonalize_noted(wide[key], method, grain_name or "daily")
        s = s.dropna()
        if window is not None:                     # common analysis window for cross-series comparability
            s = s.loc[window[0]:window[1]]
            if len(s) == 0:
                print(f"[trend] {key}: 0 points in window {window[0]}..{window[1]} — skipped.",
                      file=sys.stderr)
                continue
        if len(s) < max(4, int(min_n)):
            continue
        x = np.arange(len(s), dtype=float)        # integer grain-period index (0,1,2,...)
        y = s.to_numpy(dtype=float)
        slope, intercept, lo, hi = stats.theilslopes(y, x)
        tau, p = stats.kendalltau(x, y)
        if tau is None or np.isnan(tau):
            continue
        significant = bool(p < alpha)
        suspect = bool(plausible_max is not None and abs(float(slope)) > float(plausible_max))
        ev = dict(slope=round(float(slope), 8), intercept=round(float(intercept), 6),
                  slope_lo=round(float(lo), 8), slope_hi=round(float(hi), 8),
                  tau=round(float(tau), 6), p=round(float(p), 8), n=int(len(s)), grain=granularity,
                  deseasonalize=method, significant=significant, alpha=alpha, suspect=suspect,
                  plausible_max=(None if plausible_max is None else float(plausible_max)),
                  window=(None if window is None else [str(window[0]), str(window[1])]))
        cav = [
            f"Theil-Sen slope (robust median-of-pairwise-slopes); Mann-Kendall trend test via "
            f"Kendall's tau={tau:+.3f}, p={p:.3g} -> {'SIGNIFICANT' if significant else 'NOT significant'} "
            f"at alpha={alpha} (magnitude without significance over-claims; report both).",
            f"statistic = Theil-Sen slope in value-units per ONE {granularity} period (slope over the "
            f"integer grain-period index).",
            "Mann-Kendall p assumes independent samples; serial autocorrelation inflates significance "
            "(a real autocorrelated series shows a smaller effective sample than n).",
            ("Computed on the STL residual — a residual trend is the trend NET of season." if method == "stl"
             else "NOT deseasonalized — a seasonal cycle can masquerade as trend over a partial-year window."
             if method == "none" else f"Deseasonalized via {method} climatology before the trend test."),
        ]
        if window is not None:
            cav.append(f"Fitted on the common window [{window[0]}, {window[1]}] so slopes are comparable "
                       f"across series (not biased by differing record end-dates).")
        if suspect:
            cav.append(f"SUSPECT: |slope| {abs(slope):.4g} exceeds the plausibility bound {plausible_max} "
                       f"per {granularity} — almost always a datum shift / sensor splice / wrong sub-series, "
                       f"not real signal. Surface it flagged; exclude from any comparison/colour scale.")
        if note:
            cav.append(note)
        if grain_name is not None and grain_seconds is not None:
            cav += aliasing_caveats([key], grain_name, grain_seconds, native_map or {})
        for rule in rules:
            m = str(rule.get("match", "")).lower()
            if m and m in str(key).lower():
                cav.append(str(rule.get("caveat", "")))
        # confidence=None on purpose: a `1-p` here would be the RAW Mann-Kendall p, NOT the
        # effective-N/Bonferroni-corrected p that correlation findings put in `confidence` — mixing
        # them in one column misleads any cross-type ranking (Engineer review H1, 2026-06-20).
        # Significance is first-class in evidence.significant (raw MK p<alpha, caveated); the map reads that.
        findings.append(make_typed_finding("trend", [key], f"trend {granularity} ({key})",
                                           slope, None, cav, ev, created_by))
        flag = "*" if significant else ("!" if suspect else " ")
        summary_rows.append(f" {flag}{key}: slope={slope:+.4g}/{granularity}, tau={tau:+.2f} "
                            f"(p={p:.3g}{', SUSPECT' if suspect else ''})")
    return findings, summary_rows


def run_fact(wide, meta, granularity, created_by, rules):
    """Per-series descriptive summary. statistic = mean. evidence: n, mean, std, min, max,
    first/last period, coverage (observed fraction over the period span), unit (from meta if present).
    No deseasonalize — a fact describes the series as measured/resampled. caveats: resampled-grain note,
    gap/coverage note."""
    # unit lookup from meta (domain-supplied, not baked in): meta key = source:location_id:variable.
    unit_map: dict[str, str] = {}
    if meta is not None and not meta.empty and "unit" in meta.columns:
        for _, m in meta.iterrows():
            unit_map[f"{m['source']}:{m['location_id']}:{m['variable']}"] = m.get("unit")
    findings, summary_rows = [], []
    for key in wide.columns:
        s = wide[key].dropna()
        if len(s) == 0:
            continue
        span = len(wide[key])                       # total periods in the resampled frame
        coverage = round(len(s) / span, 4) if span else 0.0
        unit = unit_map.get(key)
        mean = float(s.mean())
        ev = dict(n=int(len(s)), mean=round(mean, 6), std=round(float(s.std()), 6),
                  min=round(float(s.min()), 6), max=round(float(s.max()), 6),
                  first_period=_period_str(s.index.min()), last_period=_period_str(s.index.max()),
                  coverage=coverage, unit=unit, grain=granularity)
        cav = [
            f"Descriptive summary on the {granularity}-resampled series (mean of bucket-AVG values); "
            f"statistic = mean{f' in {unit}' if unit else ''}.",
            f"Coverage {coverage:.0%}: {len(s)} of {span} {granularity} periods observed — gaps are "
            f"NOT interpolated for this fact; mean/std weight only observed buckets.",
        ]
        for rule in rules:
            m = str(rule.get("match", "")).lower()
            if m and m in str(key).lower():
                cav.append(str(rule.get("caveat", "")))
        findings.append(make_typed_finding("fact", [key], f"fact {granularity} ({key})",
                                           mean, None, cav, ev, created_by))
        summary_rows.append(f"  - {key}: mean={mean:.4g}{f' {unit}' if unit else ''}, n={len(s)}, "
                            f"coverage={coverage:.0%}")
    return findings, summary_rows


# ---------------------------------------------------------------- main

_FINDING_TYPES = ["correlation", "anomaly", "trend", "fact"]


def main(argv=None):
    ap = argparse.ArgumentParser(description="AnalyseData rung 2 — correlation + anomaly + trend + fact")
    ap.add_argument("--db", help="SQLite package path (asks if omitted on a TTY)")
    ap.add_argument("--granularity", choices=GRAIN_NAMES, default="daily",
                    help="Analysis grain (explicit). 15min/hourly/daily/weekly/monthly. Bounded below "
                         "by a Nyquist floor from the lag range; coarser-than-native series get an aliasing caveat.")
    ap.add_argument("--deseasonalize", choices=["none", "monthly", "stl"], default="none",
                    help="Default 'none' (caveat auto-added). 'monthly' for multi-year seasonal; 'stl' = "
                         "robust auto trend/seasonal/residual (period from the grain), opt-in.")
    ap.add_argument("--findings", default="correlation",
                    help="Comma list from {correlation,anomaly,trend,fact}. DEFAULT 'correlation' keeps "
                         "rung-1 behaviour (idempotency); other types fire only when named.")
    ap.add_argument("--lag-min", type=int, default=0)
    ap.add_argument("--lag-max", type=int, default=3)
    ap.add_argument("--two-sided", dest="two_sided", action="store_true",
                    help="Symmetric +/- lag scan (overrides lag_min = -lag_max) so reverse-lead pairs "
                         "(b leads a) surface without a hand-set negative --lag-min.")
    ap.add_argument("--no-eff-n", dest="eff_n", action="store_false",
                    help="Disable the effective-sample-size p adjustment (default ON). With it OFF the "
                         "p assumes independent samples — useful to demonstrate the autocorrelation widening.")
    ap.add_argument("--anomaly-z", dest="anomaly_z", type=float, default=3.5,
                    help="Modified-z threshold for anomaly flagging (Iglewicz-Hoaglin default 3.5).")
    ap.add_argument("--trend-window", dest="trend_window", default=None,
                    help="START,END (ISO dates) — fit every trend on this COMMON window so cross-series "
                         "slopes are comparable (differing record end-dates otherwise bias the comparison).")
    ap.add_argument("--trend-min-n", dest="trend_min_n", type=int, default=4,
                    help="Minimum points for a trend slope (default 4). Raise for an honest long-record fit; "
                         "a series below it yields no trend row (never a fabricated slope).")
    ap.add_argument("--trend-plausible-max", dest="trend_plausible_max", type=float, default=None,
                    help="|slope|-per-grain bound; a trend beyond it is flagged `suspect` (likely a datum "
                         "shift / sensor splice), surfaced flagged and excluded by the viz from the scale.")
    ap.add_argument("--alpha", type=float, default=0.05)
    ap.add_argument("--baseline", help="Path to a research-derived hypotheses JSON (agent-supplied)")
    ap.add_argument("--caveat-rules", dest="caveat_rules",
                    help="Path to a domain caveat-rules JSON: [{\"match\": substring, \"caveat\": text}]. "
                         "Domain knowledge (e.g. the hydrology potential-vs-actual-ET caveat) lives HERE, "
                         "not in the skill — keeps the engine domain-agnostic.")
    ap.add_argument("--created-by", default=f"AnalyseData v{SKILL_VERSION}")
    args = ap.parse_args(argv)

    selected = [t.strip() for t in args.findings.split(",") if t.strip()]
    bad = [t for t in selected if t not in _FINDING_TYPES]
    if bad:
        ap.error(f"unknown --findings type(s) {bad}; choose from {_FINDING_TYPES}")

    rules = []
    if args.caveat_rules:
        rules = json.loads(Path(args.caveat_rules).expanduser().read_text())

    db_path = args.db
    if not db_path:
        if sys.stdin.isatty():
            db_path = input("SQLite package path (~/data/sqlite/...): ").strip()
        else:
            ap.error("--db is required in non-interactive use")

    db = open_db(db_path)

    # Nyquist floor (ISC-17/18): warn if the chosen grain is too coarse for the lag-scan window.
    grain_name = args.granularity
    grain_seconds = GRAIN_SECONDS[grain_name]
    ok, msg = nyquist_ok(grain_seconds, args.lag_min, args.lag_max)
    if not ok:
        print(msg, file=sys.stderr)

    # native_resolution per series, for the aliasing caveat (ISC-19/34).
    meta = load_meta(db)
    native_map: dict[str, str] = {}
    if not meta.empty and "native_resolution" in meta.columns:
        for _, m in meta.iterrows():
            skey = f"{m['source']}:{m['location_id']}:{m['variable']}"
            native_map[skey] = m["native_resolution"]

    wide = load_wide(db, args.granularity)
    print(f"# AnalyseData rung 2 — {len(wide.columns)} series, {len(wide)} {args.granularity} periods")
    print(f"# package: {db_path} | findings={','.join(selected)} | deseasonalize={args.deseasonalize} "
          f"| lag={args.lag_min}-{args.lag_max} | eff_n={args.eff_n} | two_sided={args.two_sided}")

    all_findings, lines = [], []

    # --- correlation (baseline or autonomous) — only when 'correlation' is selected (rung-1 default) ---
    if "correlation" in selected:
        if args.baseline:
            hyps = json.loads(Path(args.baseline).expanduser().read_text())
            bf, brows = run_baseline(db, wide, hyps, args.deseasonalize, args.alpha, args.created_by, rules,
                                     grain_name, grain_seconds, native_map,
                                     eff_n=args.eff_n, two_sided=args.two_sided)
            all_findings += bf
            lines.append("\n## Baseline-grounded (tested against literature)")
            lines += brows
        else:
            af, arows = run_autonomous(wide, args.deseasonalize, args.lag_min, args.lag_max,
                                       args.alpha, args.granularity, args.created_by, rules,
                                       grain_name=grain_name, grain_seconds=grain_seconds,
                                       native_map=native_map, eff_n=args.eff_n, two_sided=args.two_sided)
            all_findings += af
            lines.append("\n## Autonomous (exploratory, BH-FDR; '*' = significant)")
            lines += arows

    # --- per-series rung-2 finding-types — each fires only when named ---
    if "anomaly" in selected:
        anf, anrows = run_anomaly(wide, args.deseasonalize, args.granularity, args.created_by, rules,
                                  args.anomaly_z, grain_name, grain_seconds, native_map)
        all_findings += anf
        lines.append("\n## Anomalies (per-series robust outliers, modified z)")
        lines += anrows
    if "trend" in selected:
        twin = None
        if args.trend_window:
            a, b = [p.strip() for p in args.trend_window.split(",", 1)]
            twin = (pd.Timestamp(a), pd.Timestamp(b))
            if twin[0] > twin[1]:
                ap.error(f"--trend-window start ({a}) is after end ({b})")
        tnf, tnrows = run_trend(wide, args.deseasonalize, args.granularity, args.created_by, rules,
                                grain_name, grain_seconds, native_map, alpha=args.alpha,
                                window=twin, min_n=args.trend_min_n,
                                plausible_max=args.trend_plausible_max)
        all_findings += tnf
        lines.append("\n## Trends (per-series Theil-Sen + Mann-Kendall; '*'=significant, '!'=suspect)")
        lines += tnrows
    if "fact" in selected:
        fnf, fnrows = run_fact(wide, meta, args.granularity, args.created_by, rules)
        all_findings += fnf
        lines.append("\n## Facts (per-series descriptive summary)")
        lines += fnrows

    n = write_findings(db, all_findings)
    print("\n".join(lines))
    print(f"\n## Summary\nWrote {n} finding(s) [{','.join(selected)}] to the `findings` table of {db_path}.")
    print("Every finding carries caveats[]; deseasonalization, lag-dependence, threshold-dependence and "
          "the effective-N / serial-autocorrelation adjustment are always flagged; domain caveats arrive "
          "via --caveat-rules; autonomous findings are marked exploratory. Charts are out of scope "
          "(that is TellDataStory).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
