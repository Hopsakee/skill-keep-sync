---
name: TellDataDashboard
model: deterministic
description: >
  Serve a LIVE, clear, comprehensible NiceGUI dashboard to BROWSE a finished SQLite PACKAGE — the
  live-app sibling of TellDataStory in the modular data pipeline
  (FetchData -> AnalyseData -> {TellDataStory | TellDataDashboard}). Opens a dark dashboard
  at localhost:8080 with ONE PAGE PER TABLE: (1) DATA — pick a SINGLE or MULTIPLE measurement points
  (locations) AND a single or multiple parameters (variables), and read the data as faceted
  small-multiple line charts (one chart per parameter, the selected locations overlaid in a
  consistent colour, units on the axis) plus a browsable value table; period range, time-grid, and
  level/change/rebase controls; (2) REEKSEN — the full `meta` provenance table (every series:
  source, location, variable, unit, resolution, row count, coordinates); (3) BEVINDINGEN — the
  `findings` rows rendered VERBATIM (r, lag, n, caveats exactly as AnalyseData recorded them; .6g;
  computes NO statistic). The package stores RAW heterogeneous-resolution data, so each series is
  DISPLAY-resampled to a chosen grid by MEAN — surfaced in a visible control + a Data & Methods
  panel, empty buckets left missing (connectgaps=False; no fill/interpolation), storage never
  touched. Opens on a smart default (the widest-coverage parameter + its locations) so it never
  shows an empty chart. DOMAIN-AGNOSTIC: metric=variable, entity=location, labels/units from `meta`.
  Python+uv (nicegui/plotly/fastlite/pandas), `uv run --script`. USE WHEN dashboard from sqlite,
  dashboard from a database, browse the data, look through the tables, explore a data package,
  interactive dashboard from the pipeline, nicegui dashboard from db, tell_data_dashboard, page per
  table, select measurement points, select parameters, stage 3 dashboard. NOT FOR a dashboard from a
  loose CSV/Excel file (run FetchData to package it first), a static shareable HTML narrative (use
  TellDataStory), finding patterns / computing correlations (use AnalyseData), or pulling raw
  data (use FetchData).
---

# TellDataDashboard

Stage 3 of the modular data pipeline, the **live** sibling of `TellDataStory`. Its one job:
**a finished SQLite package → a reactive NiceGUI dashboard at `localhost:8080` for browsing all the
data in the package**, one page per table. `TellDataStory` tells a static story; this lets you
*look through* the same package — pick the measurement points and parameters you care about and read
the series.

## The three pages (one per table)

1. **📈 Data** — the browser. A parameter selector (**max 2**) and a measurement-point
   multi-select (one or many). **One parameter → single left y-axis. Two parameters → parameter 1
   on the LEFT y-axis (solid lines), parameter 2 on the RIGHT y-axis (dashed)** — so two different
   units (groundwater level in m NAP vs discharge in m³/s) compare on one chart. Locations are one
   **consistent colour** each. The **Meetpunten list is filtered** to points that actually measure
   the selected parameter(s), and each point is **prefixed with a short code** for the parameters it
   carries (e.g. `[GL] Dalfsen`, `[D·GL·W] Genemuiden`), with a legend mapping codes → names. Below,
   a **browsable value table** (Periode · Meetpunt · Parameter · Waarde). Controls: a period range
   slider whose handles show the **start/end dates** on hover/drag (the from–to line sits above it),
   time-grid (Day/Week/Month), level / YoY% / index=100.
2. **🗂️ Reeksen (meta)** — the full provenance table: every `(source · location · variable)` with
   unit, native resolution, row count, and coordinates. Browse what's in the package.
3. **💡 Bevindingen** — the `findings` rows rendered **faithfully**: r, lag, n and every caveat
   exactly as `AnalyseData` wrote them, at `.6g` stored precision. This page computes **no**
   statistic — honesty is established once by `AnalyseData` and never re-laundered here.

## Run it

```bash
# Serve the dashboard for a finished package (default :8080):
uv run --script ~/.claude/skills/TellDataDashboard/scripts/dashboard.py --db ~/data/sqlite/wdodelta-hydro.db

# Inspect the detected shape first (cheap — no server, no full-table read):
uv run --script .../dashboard.py --db <pkg.db> --inspect

# Headless sanity build (CI / verification — builds UI + figures, exits 0):
uv run --script .../dashboard.py --db <pkg.db> --check

# Force the display grid, or a custom port:
uv run --script .../dashboard.py --db <pkg.db> --period W --port 8099
```

`--db` is TTY-guarded (asks when omitted on a terminal, errors in non-interactive use), mirroring the
sibling stages. Always `uv run --script` (PEP-723 deps: nicegui, plotly, fastlite, pandas).

## The adapter (package → shared display grid)

The package stores RAW data on heterogeneous time grids (daily KNMI, sub-daily RWS, irregular BRO). A
shared grid is a **mathematical precondition** for overlaying series, so each series is
display-resampled by **MEAN** to a chosen period (auto-picked from the span, overridable: Day / Week
/ Month). Mapping is **meta-driven** — `metric = variable`, `entity = location`, labels/units straight
from `meta` — so the schema can never disagree with the store (no `infer_schema` guessing).

## Smart first-open default

The dashboard opens on the **parameter with the widest location coverage** and the locations that
actually have it — so it never opens on an empty chart. (Sparse cells are real: discharge lives only
at RWS points, groundwater only at BRO wells; a naive "first parameter + first locations" default can
miss entirely.)

## Chart rules

Charts obey `References/ChartDesign.md` (reused from the family) and are checked by
`scripts/chart_lint.py`: direct end-labels not legends, conditional markers, faint grid,
unit-labelled axes, a title, one annotation. Run `chart_lint.py dashboard.py` → exit 0.

## Gotchas

- **`uv run --script`, never bare `python3`.** Deps live in the PEP-723 header.
- **Smart default exists because of sparse cells.** Not every location has every variable. The
  open-view picks the widest-coverage parameter + its locations; without it the dashboard can open on
  a parameter/location pair that don't intersect and show "Geen data …". If you change the default,
  keep it coverage-aware.
- **`--inspect` is cheap; the server's first paint is not.** `--inspect` reads only `meta` + a
  `MIN/MAX(timestamp)`; the server caches every raw series ONCE at startup (a few seconds on the
  214 MB `wdodelta-hydro.db`), then resamples from cache on every control change — never re-reads.
- **`connectgaps=False` is load-bearing.** Empty resample buckets are real missing periods; bridging
  them fabricates continuity. Resampling is MEAN-only and never forward-fills/interpolates. Caveat
  the reader: a bin with 1 observation and a bin with 200 render as one point — the chart says
  nothing about per-bin density (stated in Data & Methods).
- **MEAN-only resample — no sum control.** Summing a stock variable (groundwater level) is physically
  false; flow totals (rainfall/discharge) are shown as bin-means, not totals. Don't add a blanket
  "sum" without tagging intensive vs extensive off `meta`.
- **`times` are ISO strings, not Timestamps.** NiceGUI serializes element state to JSON; a pandas
  `Timestamp` in a Plotly figure throws `TypeError: Type is not JSON serializable: Timestamp` on the
  live page (but NOT under `--check`, which never serializes). `build_panel` emits ISO-string `times`
  and keeps a Timestamp-keyed index internally. `resample_one` uses `utc=True` so a mixed tz-aware/
  naive package can't crash the sort.
- **Findings page never recomputes.** r/lag/n are read from the row at `.6g` — rounding to `+0.47`
  would fabricate a number absent from the row.
- **Material-Icon names can render as literal text in a fresh automation browser** (the icon webfont
  isn't cached). Tabs use **emoji labels** (system font) to stay legible regardless; a normal browser
  renders the rest fine. If you add icons, prefer emoji or accept the font dependency.
- **HEAD returns 405, GET returns 200.** Smoke-test with a GET (`curl -s -o /dev/null -w '%{http_code}'`),
  not `curl -I`.
- **`ui.run()` blocks.** Verify with `--check` or launch in the background + poll with
  `curl --retry --retry-connrefused`.
- **Headless Brave can't screenshot the Plotly charts** (it tears down the NiceGUI websocket). Use
  Interceptor against a live a real browser (keeps the WS alive); `--check` confirms figures build.
- **Empty `findings` is graceful** — the Bevindingen page shows "draai eerst AnalyseData", no crash.
- **`scripts/` not `Tools/`** — mirrors the family layout. Family consistency over the generic
  CreateSkill `Tools/` default.

### Learnings from the nl-groundwater-trends map (2026-06)

- **Encode significance/uncertainty visually — never render a non-significant or suspect finding
  identically to a solid one.** When the Bevindingen page (or any map view) shows a finding that
  carries a `significant`/`suspect` flag, distinguish it (hatch/grey/fade + a label), or the dashboard
  silently asserts a real effect the stats don't support. Match grondwatertools.nl's "geen
  significante trend" behaviour rather than colouring an insignificant slope as if it were fact.

## What gets written

Nothing to the package — it serves a live app and is read-only on `data`/`meta`/`findings`. The only
persistent write is the execution-log line below.

## Execution Log

```bash
echo '{"ts":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","skill":"TellDataDashboard","workflow":"build-dashboard","input":"8_WORD_SUMMARY","status":"ok|error","duration_s":SECONDS}' >> ~/.claude/PAI/MEMORY/SKILLS/execution.jsonl
```

## Pipeline pointers

- Stage 1 (fetch + empty `findings`): `~/.claude/skills/FetchData/`.
- Stage 2 (fills `findings`): `~/.claude/skills/AnalyseData/`.
- Stage 3 static story sibling: `~/.claude/skills/TellDataStory/`.
- Chart rules (reused): `References/ChartDesign.md` + `scripts/chart_lint.py`.
