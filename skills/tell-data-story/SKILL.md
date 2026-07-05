---
name: TellDataStory
model: deterministic
description: >
  Turn a finished dataset package's findings into a short, self-contained human STORY — the THIRD and
  final stage of the modular data pipeline (FetchData -> AnalyseData -> TellDataStory). Reads
  the `findings` rows + `caveats[]` that `AnalyseData` wrote (plus `meta`/`data` for labels and the
  series) and renders a plain-language narrative + one high-signal Plotly chart per finding into two
  files next to the DB: `<stem>_story.html` (narrative + caveats + interactive figures) and
  `<stem>_story.md` (narrative + caveats, readable cold). FAITHFUL RENDERER, NOT A SECOND ANALYST:
  every number (r, lag, n, literature label) is quoted VERBATIM from the finding row — this stage
  computes NO statistic — and every `caveats[]` string is rendered VERBATIM so the story literally
  cannot over-claim. DOMAIN-AGNOSTIC: labels/units/meaning come from `meta` + the finding fields, never
  baked in. The chart obeys the high-signal rules in References/ChartDesign.md and is checked by
  scripts/chart_lint.py; verify the rendered HTML with a Claude-as-judge screenshot. ALSO has a
  GEOGRAPHIC POINT-MAP mode (`scripts/map.py`): a self-contained folium/Leaflet HTML that plots
  per-location findings (default `type='trend'`) at their `meta.lat/lon`, coloured by the finding
  statistic on a diverging scale, with non-significant findings HATCHED and `suspect` ones greyed —
  same faithful-renderer rule (reads the finding row, computes nothing; no surface between points).
  USE WHEN tell a
  data story, visualise findings, render findings, data story, story from findings, narrate findings,
  chart the findings, close the pipeline, tell_data_story, stage 3, story layer, findings to narrative,
  map of findings, geographic map, point map, plot wells/stations on a map, spatial map of trends,
  folium map, leaflet map, map the findings, where are the trends.
  NOT FOR finding patterns / computing correlations (use AnalyseData), pulling raw data (use
  FetchData), an interactive dashboard (use TellDataDashboard), or wisdom extraction
  from prose (use ExtractWisdom).
---

# TellDataStory

## Geographic point-map mode (`scripts/map.py`)

For per-location findings (a `trend`/`fact`/`anomaly` per well/station), render a spatial map instead
of (or alongside) the narrative:

```bash
uv run scripts/map.py --db ~/data/sqlite/pkg.db --finding-type trend   # -> <stem>_map.html
```

It joins each single-series finding to its `meta.lat/lon`, colours the marker by the finding
`statistic` (diverging, red=low/falling … blue=high/rising), **hatches non-significant** findings
(evidence `significant=false`) and **greys `suspect`** ones — so the map can't imply a real trend the
stats don't support. Faithful renderer: every value is read from the finding row; no statistic is
computed and no surface is drawn between points. Self-contained HTML, mobile-collapsible methods panel.
Pairs with `AnalyseData --findings trend` (which writes the `significant`/`suspect` flags).

---

Stage 3 of the modular data pipeline. Its one job: **a package's `findings` + `caveats[]` → a short
self-contained human narrative + one simple high-signal chart per finding → an `.html` report and a
`.md` twin next to the DB.** It never finds patterns (that is `AnalyseData`) and never mutates any table.

## The load-bearing distinction

1. **Faithful renderer, not a second analyst.** Every statistic in the prose and on the chart is read
   from the `findings` row. This stage computes nothing — that is the whole reason analyse and tell are
   separate skills: honesty is established once by `AnalyseData` and never re-laundered here.
2. **Caveats travel verbatim.** Each string in `caveats[]` is rendered character-for-character. A
   paraphrase is an over-claim; truncation is a lie. The story is hedged exactly as the analysis hedged it.
3. **Domain-agnostic.** Human labels and units come from `meta.location_label` / `meta.unit`; the
   relationship facts come from the finding. No hydrology (or any domain) is baked into control flow.

## Run it

```bash
# Render the story for a finished package (charts + narrative, two files next to the DB):
uv run --script ~/.claude/skills/TellDataStory/scripts/tell.py --db ~/data/sqlite/wdodelta-hydro.db

# Custom output directory:
uv run --script .../tell.py --db ~/data/sqlite/wdodelta-hydro.db --out-dir /tmp/story
```

`--db` is optional and TTY-guarded (asks when omitted, errors in non-interactive use), mirroring
`FetchData` / `AnalyseData`. Always invoke with `uv run --script` (PEP-723 inline deps:
fastlite, pandas, plotly) — bare `python3` lacks them.

## Output contract

1. **`<stem>_story.html`** — a headline, one section per finding (plain-language narrative + the
   verbatim caveats + an interactive Plotly figure), and a "Data & method" disclosure.
2. **`<stem>_story.md`** — the same narrative + verbatim caveats, readable cold, pointing at the HTML
   for the figure. Self-contained: every series is named by its human label, no undefined abbreviation.

Both are written next to the DB (or `--out-dir`). Output is **idempotent**: the "generated" marker is
derived from the newest finding's `created_at`, never wall-clock, so re-running on an unchanged package
produces byte-identical files.

## The chart (high-signal, Plotly)

One figure per renderable finding, obeying `References/ChartDesign.md`: a title stating the finding
(with its r and lag, both quoted from the row), axis titles carrying the series units from `meta`,
direct end-labels instead of a legend, faint gridlines, conditional markers (only when sparse), and one
story annotation marking the peak in view. `scripts/chart_lint.py scripts/tell.py` statically verifies
these rules; a Claude-as-judge `Interceptor` screenshot is the experiential gate.

## Gotchas

- **Never recompute a statistic.** r, lag, n, and the literature label are read from the `findings`
  row. If you find yourself importing `scipy.stats` or calling `.corr()` here, you are rebuilding
  `AnalyseData` — stop. The split exists so the story can't quietly disagree with the analysis.
- **Caveats are HTML-escaped, and that IS verbatim.** Escaping makes the *displayed* glyphs equal the
  stored string even when a caveat contains `<`, `>`, or `&` (without it the browser eats `<…>` as a
  tag and silently drops the hedge — an over-claim). The verbatim probe therefore compares the
  **escaped** form in the HTML and the raw form in the MD. Don't "simplify" by removing the escape:
  that reintroduces the silent-mangle hole the Engineer review caught on the rung-1 build.
- **Never recompute means never round, either.** The coefficient is rendered with `.6g` (full stored
  precision, e.g. `+0.471501`), not `+0.47`. Rounding produces a number absent from the `findings`
  row and falsifies the "verbatim" claim — the Advisor flagged this as a boundary violation.
- **Narrative converts only `**bold**`, never `_italic_`.** A greedy underscore→`<em>` pass mangles
  DB-derived labels/sources containing `_` (generic column names routinely do). Underscore-italic was
  removed for exactly this reason; keep it removed.
- **Only `type='correlation'` is narrated as "Pearson r".** A forward-guard renders any other
  `findings.type` (future anomaly/trend/fact rungs) as a generic line so this stage can't over-claim a
  non-correlation finding. A malformed (unparseable-JSON) row is shown but not narrated.
- **Plotly `to_html` uses a random div id by default → breaks idempotency.** This skill passes a
  deterministic `div_id=fig{finding_id}`. If you add figures, give each a stable id or two runs will
  differ byte-for-byte.
- **Charts use the overlapping time window of the two series.** A finding whose series barely overlap
  (e.g. a 30-year daily series vs a 3-month sub-daily one) is clipped to the intersection so the chart
  is readable.
- **Charts downsample via the viz envelope (`scripts/resample.py`), and that's a CHART change only.**
  `read_enveloped` buckets each series to a min/max/mean envelope in SQL so a 726k-point series never
  reaches the browser (the story HTML stays ~KB, not multi-MB). This shapes the *picture*; it never
  touches a finding value — r/lag/n are still read verbatim from the row (keep it that way). The
  envelope is the SAME viz driver the dashboard uses, carried as a per-skill duplicate (no cross-skill
  import) per the adaptive-resolution ISA decision.
- **An honest envelope needs BOTH peaks and gaps, not just peaks.** The min/max band preserves a peak
  the mean would hide — but a `GROUP BY` only emits rows for buckets that HAVE data, so a connected
  line will *bridge* empty buckets and paint continuity across a real coverage gap (fabrication-by-
  rendering). Every envelope trace sets `connectgaps=False` so holes render as breaks. Preserving
  extremes without preserving gaps still lies, just in the other direction — the Advisor caught this
  on the StoryInherit build.
- **Absent / non-overlapping series is a feature, not a crash.** A finding whose series aren't both in
  `data`, or don't overlap in time, is narrated honestly (its caveats explain why) with no chart.
- **Zero findings → run `AnalyseData` first.** An empty `findings` table prints a friendly message
  and exits 0 — it does not write empty files or crash.
- **`scripts/` not `Tools/` on purpose.** This skill mirrors the proven sibling layout
  (`FetchData/scripts/`, `AnalyseData/scripts/`): `uv run --script` files live in `scripts/`. Family
  consistency over the generic CreateSkill `Tools/` default.

### Learnings from the a prior groundwater-trends tool map (2026-06)

- **Encode significance/uncertainty visually — never render a non-significant or suspect finding
  identically to a solid one.** If a finding carries a `significant` or `suspect` flag, the picture
  MUST show it (hatch the marker, grey it, or fade it) — otherwise the reader infers a real effect the
  stats don't support, the exact over-claim this skill polices in text. A point-estimate slope drawn
  solid next to p≥0.05 reads as fact; hatching it is the honest fix. (Cross-checked against
  grondwatertools.nl, whose "geen significante trend" is the behaviour to match.)
- **Self-contained HTML gets read on phones — make overlays collapsible + responsive.** A fixed
  info/methods panel that's fine on desktop covers a third of the view on mobile. Put it in a
  `<details>` that auto-collapses below ~600px (`window.matchMedia` removes the `open` attr) and shrink
  the title with a media query. A bottom-corner legend + title that overlap each other on a phone are
  the giveaway.

## Pipeline pointers

- Stage 1 (fetch + the empty `findings` table): `~/.claude/skills/FetchData/`.
- Stage 2 (fills `findings` with caveated correlations): `~/.claude/skills/AnalyseData/`.
- findings schema (locked): `~/.claude/skills/FetchData/scripts/store.py` (`_SCHEMA`).
- Chart rules (reused, the project): `References/ChartDesign.md` + `scripts/chart_lint.py`.
