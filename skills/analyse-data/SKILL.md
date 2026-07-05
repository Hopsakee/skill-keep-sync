---
name: AnalyseData
model: sonnet
description: >
  Find honest patterns in a SQLite dataset package and write them to its `findings` table — the
  SECOND stage of the modular data pipeline (FetchData -> AnalyseData -> TellDataStory).
  Reads the `data` + `meta` tables a `FetchData` package ships, finds patterns by itself, and
  fills the empty `findings` table `FetchData` hands over. DOMAIN-AGNOSTIC by design: it runs on
  hydrology, energy, housing, "or whatever" — domain specifics (period, lag, variable meaning) are
  INPUTS, never baked in. Finding-types (`--findings`, default `correlation` for byte-compatible
  rung-1 behaviour): CORRELATION-with-caveats (deseasonalized, lag-scanned Pearson r + effective-N /
  Bonferroni-corrected p), plus ANOMALY (robust modified-z), TREND, and FACT. The TREND type is
  Theil–Sen slope + Mann–Kendall significance with a first-class `significant` flag, an optional
  common-window fit (`--trend-window`) for cross-series comparability, a plausibility/`suspect` flag
  (`--trend-plausible-max`), and a min-points guard — so a large slope is never reported as real
  without its significance (and the geo-map in TellDataStory hatches the non-significant ones).
  Every finding carries a MANDATORY caveats[] array. Optionally grounds correlation against a
  research-derived hypothesis baseline (`--baseline FILE`), labelling each consistent / weaker /
  absent / surprising. Dual output: durable `findings` rows (via fastlite) + a short human-readable
  summary. USE WHEN analyse data, analyse dataset package, find correlations, find patterns,
  fill findings table, correlation with caveats, analyse_data, stage 2, research baseline test,
  deseasonalize correlation, lag correlation, trend, trend significance, Theil-Sen, Mann-Kendall,
  significant trend, anomaly detection, descriptive facts. NOT FOR pulling/fetching raw data
  (use FetchData), charts/dashboards/stories/maps (use TellDataStory / TellDataDashboard), or ingesting documents into the Library (use _TO_LIBRARY).
---

# AnalyseData

Stage 2 of the modular data pipeline. Its one job: **a dataset package → patterns found by the skill
itself → rows in the `findings` table (with confidence + caveats) → a short human summary.**
It never charts (that is `TellDataStory`) and never mutates `data`.

## The load-bearing distinction

1. **The skill is domain-agnostic.** It looks for patterns by itself on ANY `FetchData` package.
   No hardcoded hydrology. Period, lag, and variable meanings are CLI params, baseline inputs, or
   read from `meta` — never skill constants.
2. **Hydrology is the current test board**, exactly as the 2-BRO-well slice proved `FetchData`.
   The hydrology parameters (monthly Jan-vs-Jan seasonal, 1-3 day rainfall→discharge lag, the ET
   caveat) are test-board inputs you PASS, not behavior baked into the skill.

## Rung 1 scope (correlation-with-caveats only)

Anomaly, trend, and fact finding-types are later rungs. Rung 1 gets correlation honest first:
deseasonalize, lag, the research-baseline loop, the caveats discipline, the human summary.

## Run it

```bash
# Autonomous exploratory pass (all pairs, Benjamini-Hochberg FDR, every finding flagged exploratory):
uv run --script ~/.claude/skills/AnalyseData/scripts/analyse.py \
  --db ~/data/sqlite/wdodelta-hydro.db --granularity daily --lag-max 3

# Seasonal monthly pass on multi-year series (Jan-vs-Jan deseasonalization):
uv run --script .../analyse.py --db ~/data/sqlite/wdodelta-hydro.db \
  --granularity monthly --deseasonalize monthly --lag-max 2

# Research-grounded pass (agent supplies the cited hypotheses JSON — see below):
uv run --script .../analyse.py --db ~/data/sqlite/wdodelta-hydro.db \
  --granularity daily --deseasonalize monthly --baseline /tmp/baseline.json
```

`--db` is optional and TTY-guarded (asks when omitted, never headless), mirroring `FetchData`.
Always invoke with `uv run --script` (PEP-723 inline deps: fastlite, pandas, scipy, numpy) — bare
`python3` lacks fastlite.

## The research-baseline loop (the user's central design idea)

Pattern-finding must be **honest and grounded**, not blind all-pairs fishing. The grounding lives in
the AGENT layer, fed into the deterministic script:

1. The agent identifies the package's variables/domain (from `meta` + the data).
2. The agent consults literature for KNOWN correlations — **`Skill("_PKW_LIBRARIAN")` first**, then a
   general research-paper search (`Skill("Research")` / `deep-research`). (The pluggable per-domain
   research adapter is POSTPONED — revisit only if the general path has poor recall.)
3. The agent writes a hypotheses JSON and passes it via `--baseline`. The script tests each
   hypothesis and labels it consistent / weaker / absent / surprising — and when a required series is
   **absent from the package**, it says so plainly instead of inventing a correlation.

Baseline JSON shape (one object per hypothesis):

```json
[
  {"a": "precipitation", "b": "discharge", "expected_sign": "+", "lag_min": 0, "lag_max": 3,
   "source": "Rainfall-runoff: precip leads discharge at short lag (STOWA/Deltares)"}
]
```

`a`/`b` match either a full `source:location_id:variable` key or a bare variable name. Sources are
cited in the finding's caveats — "experts say" is not an argument.

## Output contract

1. **The `findings` table** (durable). One row per finding: `type` (correlation), `columns` (JSON
   series keys), `period`, `statistic` (Pearson r), `confidence` (1 − p), `caveats` (JSON array —
   load-bearing), `evidence` (JSON: exact r, p, lag, n, deseasonalize, baseline label/source),
   `created_at`, `created_by`. Idempotent: re-running dedupes on (type, columns, period).
2. **A short human-readable summary** to stdout — findings, the baseline tested, and the caveats.
   The rich visual story is still `TellDataStory`'s job.

## Caveats discipline (why this skill exists)

Every correlation finding carries `caveats[]`. Always: a deseasonalization note (or a loud "NOT
deseasonalized" warning when `--deseasonalize none`) and a lag-is-location-dependent note. Auto-added:
the `precipitation − Makkink = potential, not actual` ET caveat whenever an evaporation series is in
the pair; an EXPLORATORY / multiple-comparisons note (with Benjamini-Hochberg FDR) on autonomous
findings. `caveats[]` travels WITH the finding so `TellDataStory` cannot over-claim downstream.

## Gotchas

- **Deseasonalize is parameterized on purpose — `monthly` is a test-board choice, not the skill's
  method.** Default is `none` (with a loud caveat). `monthly` subtracts the same-calendar-month mean
  across years (needs multi-year data — degenerate on a single short year like the RWS Jan-Apr slice).
  **`stl` deliberately raises `NotImplementedError`** — STL is the correct general default (auto
  trend/seasonal/residual without knowing the period up front) and is the rung-2 TODO.
- **fastlite only — never stdlib `sqlite3`** for new code (`FetchData/scripts/store.py` is the one
  locked stdlib exception; do NOT touch it).
- **Resampling happens here, not in fetch.** `FetchData` stores raw native resolution; aligning two
  series (e.g. sub-daily RWS discharge with daily KNMI precip) by mean-resampling to a common
  granularity is analysis and belongs in this skill.
- **Absent series is a feature, not a crash.** A baseline hypothesis whose series is missing produces
  an honest "expected by literature but absent from package" finding.
- **Idempotency gap (rung-2 TODO): an absent→resolved transition is NOT superseded.** Dedup keys on
  (type, columns, period). When a hypothesis was ABSENT (period `baseline-absent (a~b)`, bare-token
  columns) and a later run resolves it (period `baseline lagX-Y (key_a~key_b)`, resolved columns),
  the keys differ, so the stale ABSENT row survives alongside the new real finding. Until rung 2 keys
  dedup on a stable hypothesis identity, delete stale `baseline-absent%` rows after backfilling the
  missing series. (Hit 2026-06-07 when BRO groundwater was merged into the hydro package.)
- **Lag semantics:** `a` LEADS `b` by `lag` periods (a[t] vs b[t+lag]); the reported lag is the one
  with the largest |r| in the scanned range and is never presented as universal.
- **The lag scan is itself a multiple comparison — correct the p BEFORE using it.** Keeping the
  max-|r| lag and reporting that lag's single-test Pearson p is selection bias (~29% false positives
  on pure-noise pairs across 8 lags vs the honest ~5%). The skill applies Bonferroni-over-lags
  (`p_corrected = min(1, p_raw · k_lags)`) BEFORE labelling against a baseline, before BH-FDR, and
  before the `confidence` column. Evidence stores `p_raw`, `p_corrected`, `k_lags`. Never label or
  FDR on `p_raw`. (Caught by Engineer + Advisor on the rung-1 build, 2026-06-07.) Pearson's p also
  assumes independent samples — serially autocorrelated series make even the corrected p optimistic;
  that caveat is always attached. Rung-2: a proper effective-sample-size / block-bootstrap p.
- **Domain knowledge is DATA, not code — `--caveat-rules`.** Domain-specific caveats (the hydrology
  potential-vs-actual-ET note) are supplied via a `--caveat-rules` JSON (`[{match, caveat}]`), never
  hardcoded as `if "makkink" in key`. A hardcoded substring branch is a domain leak that fails the
  agnosticism test silently on hydro-only data. The synthetic generic-column run (no rules) must
  produce zero domain caveats and no crash — that is the real agnosticism probe.

### Learnings from the nl-groundwater-trends map (2026-06)

- **`groupby(<DatetimeIndex>)` SILENTLY EMPTIES the result — group by `.to_numpy()`.** Passing a
  pandas `DatetimeIndex` (or any pandas Index) as the `by=` grouper makes pandas LABEL-ALIGN it to the
  Series' own index instead of grouping positionally; mismatched labels → an empty result, no error.
  A half-month reducer returned 0 rows this way and every downstream stat became NaN/object-dtype.
  Always `s.groupby(keys.to_numpy())` (or `keys.values`) for a positional grouper.
- **Extremes-based aggregates are sampling-density-sensitive — normalize cadence + require a min
  count.** A per-period min/max (or a GxG HG3/LG3 = mean of the 3 highest/lowest) drops/rises simply
  because a denser-sampled period resolves an extreme a sparse period missed — a measurement artifact
  masquerading as signal. Reduce to a fixed cadence (e.g. biweekly median) BEFORE taking the extreme,
  and emit NaN (never a fabricated 0/ffill) for a period below a min reading-count.
- **Sampling INHOMOGENEITY is its own caveat.** A series whose cadence/method changes mid-record
  (manual biweekly → automatic diver, ~24 → ~2000 obs/yr) biases extremes and even the slope: the
  early sparse years anchor a steeper apparent trend. `meta.native_resolution` is whole-series, so
  check per-period obs-density for a step-change and attach an inhomogeneity caveat when found.
- **Cross-entity trend comparison needs a COMMON window.** Comparing slopes across many series whose
  records end on different dates is biased — a drought year at one record's end becomes a fake
  between-series difference. Theil–Sen is robust to outliers, NOT to different time windows; fit every
  compared slope on the shared `[start,end]` window.
- **Physically-implausible effect sizes are FLAGGED, not silently shown or dropped.** A computed
  effect beyond a domain-plausible bound (a groundwater trend of +67 cm/yr) is almost always a datum
  shift / sensor splice / wrong sub-series — mark it `suspect` (bound supplied as DATA, like
  `--caveat-rules`), exclude it from any comparison/colour scale, and surface it as a flagged finding.
  Showing it as real over-claims; dropping it hides a data problem. Both are dishonest.

## Pipeline pointers

- Stage 1 (fetch + the empty `findings` table this skill owns): `~/.claude/skills/FetchData/`.
- findings schema (locked): `~/.claude/skills/FetchData/scripts/store.py` (`_SCHEMA`).
