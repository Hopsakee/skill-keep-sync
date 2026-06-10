---
name: FetchData
description: >
  Pull raw data from a source into a standard single-file SQLite package the caller names — the
  first stage of the modular data pipeline (FetchData -> AnalyseData -> TellDataStory).
  One skill, ONE function per source (not one skill per source): a shared stdlib-sqlite3 packaging
  core writes three tables — `data` (tidy/long observations, one row per obs), `meta` (per-series
  provenance), `findings` (created empty, owned by AnalyseData) — and a thin per-source fetcher
  only hits the API and hands back tidy series. Stores RAW, native-resolution data (no resampling,
  no merging, no interpolation — those are downstream); idempotent re-fetch via composite primary
  key + INSERT OR REPLACE. CSV is export-only (`--export-csv`), never the store. Adding a new source
  = adding one fetcher function + one registry line. Currently implemented: BRO groundwater
  (generalised from a prior hydrology-fetch tool), KNMI meteo (daily aggregates), and RWS surface water (raw
  sub-daily via ddlpy). USE WHEN fetch data, get raw
  data, pull from BRO, groundwater data, fill a dataset, build a dataset package, fetch into sqlite,
  fetch_data, hydrology fetch, add a data source. NOT FOR resampling/correlation/anomaly finding
  (use AnalyseData), charts/dashboards/stories (use TellDataStory), one-shot CSV profiling
  (use DataAnalysis), or ingesting documents into the Library (use _TO_LIBRARY).
---

# FetchData

Stage 1 of the modular data pipeline. Its one job: **raw data from a source → a standard SQLite store.**

## The contract (locked 2026-06-06 —)

- **Store = one SQLite file**, three tables: `data` (tidy obs), `meta` (per-series provenance),
  `findings` (empty, populated later by `AnalyseData`). Full schema: `References/Schema.md`.
- **Tidy/long always.** One row per observation. Adding a source adds rows, never columns.
- **Raw only.** No resampling, no merging, no interpolation. If — and only if — you explicitly ask
  the API for a native aggregate (e.g. weekly mean) and it provides it, that is stored with
  `aggregation` set accordingly; otherwise `aggregation='raw'`.
- **One DB, caller-chosen — the skill ASKS when `--db` is omitted.** `--db` is optional: name it for
  scripted use, or omit it and the skill prompts for which DB to create/use (it's on-demand, never
  headless). Convention: one file under `~/data/sqlite/` — all hydrology (BRO + KNMI + RWS) can share
  `wdodelta-hydro.db` because the `source` column discriminates the series.
- **Idempotent.** Re-running a fetch replaces overlapping rows (composite PK + `INSERT OR REPLACE`),
  never duplicates.
- **CSV is export-only** (`--export-csv`), never the store.

## Run

All hydrology lands in ONE db by convention (`~/data/sqlite/wdodelta-hydro.db`); the `source` column
separates BRO/KNMI/RWS. Omit `--db` and the skill asks where to create/use it.

```bash
# omit --db and the skill ASKS where to create/use the DB (on-demand only; never headless)
uv run scripts/fetch.py --source knmi --knmi-vars RH --xy 203000,503000 --from 2024-01-01 --to 2024-03-31
#   -> "Which database should I create or use? [default: ~/data/sqlite/wdodelta-hydro.db]"

# small-BRO slice (proven first rung) — names the one shared db explicitly
uv run scripts/fetch.py --db ~/data/sqlite/wdodelta-hydro.db --source bro \
  --wells "Zwolle:GMW000000040772:1,Kampen:GMW000000042761:1"

# all 11 WDODelta wells (full station set) — via a referenced well-list FILE (explicit path), not 11 pasted triples.
# Any path works (the mechanism is generic — no privileged lookup into the skill's own folder).
uv run scripts/fetch.py --db ~/data/sqlite/wdodelta-hydro.db --source bro \
  --wells-file References/wells-wdodelta.json --from 1995-01-01 --to 2026-06-06 --min-obs 50
# Genemuiden's tube is 5 (not 1); location_id stays "{gmw}-{tube}" so it never collides with a tube-1 series.

# KNMI meteo — the full daily var set into the same db
uv run scripts/fetch.py --db ~/data/sqlite/wdodelta-hydro.db --source knmi \
  --knmi-vars TG,SQ,RH,EV24,FG --xy 203000,503000 --from 1995-01-01 --to 2026-06-06
# vars: TG temp_mean, SQ sun_hours, RH precipitation, EV24 evaporation_makkink, FG wind_speed

# RWS surface water — the full WDODelta station list via a referenced station-file (explicit path).
# Mirrors --wells-file: the 4 stations are reference DATA the caller names; --rws-box stays the CLI filter.
uv run scripts/fetch.py --db ~/data/sqlite/wdodelta-hydro.db --source rws \
  --stations-file References/stations-wdodelta.json --rws-box "52.40,52.85,5.80,6.75" \
  --from 1995-01-01 --to 2026-06-06
# Grootheid codes: WATHTE waterlevel (cm), Q discharge (m3/s). --rws-stations (inline ';'-separated, a Naam
# may contain a comma) and --stations-file are mutually exclusive — pass exactly one.

# full BRO options (--wells / --wells-file mutually exclusive; --export-csv exports the tidy table)
uv run scripts/fetch.py --db PATH --source bro --wells "label:gmw:tube,..." \
  --from 1995-01-01 --to 2026-06-06 --min-obs 50 --export-csv out.csv
```

## Architecture — one core, one function per source

- `scripts/store.py` — the shared packaging core (pure stdlib `sqlite3`). Creates the schema,
  `write_series()`/`write_payloads()` (NaN→NULL, ISO timestamps, idempotent upsert), `export_csv()`.
  **This file does not change when you add a source.**
- `scripts/sources/<source>.py` — a fetcher whose only job is to hit the API and return a list of
  `store.SeriesPayload`. Currently: `bro.py` (groundwater), `knmi.py` (meteo, daily), `rws.py` (surface water, raw sub-daily).
- `scripts/fetch.py` — CLI + a `--source` registry that dispatches to the right fetcher.

**To add a source:** write `scripts/sources/<name>.py` with a `fetch_<name>(...) -> list[SeriesPayload]`,
add it to the `--source` choices + dispatch in `fetch.py`. Do not touch `store.py`.

## Gotchas

- **SQLite has no native datetime.** Timestamps are ISO-8601 *text*; downstream must `parse_dates`.
  `meta.native_resolution` records the real cadence ('daily'/'subdaily'/'irregular') so a reader knows
  the gap structure without guessing. This is the exact failure mode CSV had — handled on purpose.
- **Timestamps are stored UTC with explicit offset (`+00:00`).** Contract decision (2026-06-06): every
  fetcher normalises to UTC-aware ISO so cross-source joins (BRO vs KNMI vs RWS) cannot silently
  misalign by an offset/DST hour. BRO `Tijdstip` is epoch-ms UTC → kept tz-aware UTC. `meta.request_params`
  records `timezone`.
- **`location_id` is the monitoring POINT, not the site.** A BRO well (GMW) has multiple tubes/filters at
  different depths, each its own series — so BRO encodes `location_id` as `"{gmw}-{tube}"`. Two tubes of one
  well must never collide on the `data` PK. Each source owns its own `location_id` encoding (KNMI/RWS:
  station id). This keeps the generic 4-column PK while making identity source-correct.
- **Per-well failure isolation.** A network failure on one well skips that well and continues; the store
  commits per-series, so a mid-run crash leaves already-fetched series persisted (not all-or-nothing).
- **Known follow-up (slice-1):** BRO value coalescing blends quality tiers (Beoordeelde→…→Controle) into one
  column and leaves `quality` NULL. Acceptable for raw levels now; a later rung should emit a per-row
  `quality` tag recording which tier supplied each value.
- **`hydropandas` is heavy** and only used for the BRO GMW→GLD id lookup; the seriesAsCsv parse is
  plain `requests`. First `uv run` resolves the dep tree (can take a minute).
- **`store.py` is pure stdlib** by design — keep pandas/hydropandas out of it so the storage contract
  never drifts with a library version.
- **KNMI precip (RH) + evap (EV24) come from `hydropandas` in METRES** (it normalises the raw KNMI 0.1mm
  integers to SI), so the fetcher multiplies ×1000 and stores unit `'mm'`. Sanity check: Dutch daily totals
  should reach double-digit mm (e.g. 20 mm), never sub-0.1 "metres" labelled mm. `hydropandas` already
  resolves the KNMI `-1` trace marker — raw values are ≥0, so no negative-rainfall sentinel leaks through.
- **KNMI daily timestamps are keyed to a CALENDAR DATE, stored at UTC midnight** — NOT by localizing the
  raw stamp. `read_knmi` returns a naive index stamped `01:00` (KNMI day convention). Localizing that to
  Europe/Amsterdam then UTC gives `00:00Z` in winter but `23:00Z of the PREVIOUS day` in summer (DST +02:00)
  — a silent one-day shift of the daily total. `knmi.py` instead takes each observation's calendar date and
  anchors it to UTC midnight (season-invariant). Verify any KNMI change across BOTH a winter and a summer span.
- **KNMI `location_id` is the station NUMBER only** (`obs.station`, e.g. `278`). `obs.name`
  (`RH_HEINO_278`) is deliberately NOT a fallback — a different key would split one station into two series
  and break the idempotent PK (the same identity trap as BRO's multi-tube `location_id`).
- **RWS goes through `ddlpy` directly, never `hydropandas.read_waterinfo`** (its `extent=` path is broken in
  0.18.1). `ddlpy`'s measurement index is tz-AWARE → `tz_convert("UTC")`; NEVER `tz_convert(None)` (naive
  timestamps silently misalign cross-source joins). RWS encodes missing as `999999999`; mask `|value| >= 1e8`
  to NULL (keep the row — a sentinel timestamp is real signal) and record `sentinel_masked` in `meta`.
- **`--rws-stations` uses `;` as the station separator**, because an RWS `Naam` can itself contain a comma
  (e.g. `Dalfsen, Vechterweerd`). Format is `Naam:Grootheid;Naam:Grootheid`, Grootheid = the last `:` field.
- **`--wells-file` is a generic mechanism; `References/wells-wdodelta.json` is shipped reference DATA, not
  hardcoded curation.** The skill principle is "the caller names everything; the skill invents no curation in
  code." A baked-in WDODelta well-set constant would violate that — so the 11 wells live as a JSON data file
  the caller names by **explicit path** (`--wells-file References/wells-wdodelta.json` or any absolute/relative
  path). There is **no privileged bare-name lookup** into the skill's own folder — every path is treated
  identically, so the code never privileges the skill's own presets (that would be curation-in-code). The
  shipped JSON is just one example well-set; to add your own, drop another `*.json` (a bare list of
  `{label,gmw,tube}` or an object with a `wells:` array) anywhere and pass its path.
- **`--stations-file` mirrors `--wells-file` exactly; `References/stations-wdodelta.json` is caller-referenced
  reference DATA, not code curation.** Same doctrine: explicit path only, NO privileged bare-name lookup; the
  4 WDODelta RWS stations live as a JSON data file (`{naam,grootheid}` objects, or an object with a `stations:`
  array). A JSON object per station sidesteps the `;`-separator the inline `--rws-stations` needs (two Naams
  contain commas). The `--rws-box` filter stays a run-level CLI arg — the file holds only the list, mirroring
  wells (no top-level `box` key). `--rws-stations` XOR `--stations-file` — pass exactly one.
- **`--db` is optional; the skill ASKS when it's omitted.** `FetchData` is on-demand only (NOT scheduled), so
  an interactive prompt is safe: with no `--db` on a TTY it offers `~/data/sqlite/wdodelta-hydro.db` (and lists
  existing `~/data/sqlite/*.db`); piped/headless with no `--db` it exits cleanly telling you to pass `--db`
  (never hangs). A shared `~/data/sqlite/` folder is safe for many DBs because SQLite names its
  `-wal`/`-shm`/`-journal` sidecars per DB *filename* — differently-named DBs in one dir cannot collide, so no
  per-DB subfolder is needed.
- **New SQLite-touching code uses AnswerDotAI `fastlite`**, not stdlib `sqlite3` (a deliberate convention). `store.py`
  is the one deliberate exception — it stays pure stdlib so the storage contract never drifts with a library
  version; the locked write-path was not rewritten. Verification probes and any future read/query helpers use
  fastlite (`Database(path).q("SELECT …")`).
