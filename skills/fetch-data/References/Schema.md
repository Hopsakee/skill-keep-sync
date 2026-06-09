# SQLite package schema (the contract every stage depends on)

One `.db` file = one dataset package. Three tables. Locked 2026-06-06 .

## `data` — tidy observations (one row per obs)

| column | type | notes |
|--------|------|-------|
| source | TEXT | 'bro' \| 'knmi' \| 'rws' \| ... |
| location_id | TEXT | stable source id (BRO: GMW id) |
| variable | TEXT | 'groundwater_level' \| 'precipitation' \| 'discharge' \| ... |
| timestamp | TEXT | ISO-8601 (SQLite has no datetime — `parse_dates` downstream) |
| value | REAL | NULL = missing (NaN→NULL) |
| unit | TEXT | denormalised for query self-sufficiency ('m NAP', 'mm', 'm3/s') |
| quality | TEXT | per-row flag, nullable |
| aggregation | TEXT | 'raw' \| 'weekly_mean' \| ... (what the API returned) |

`PRIMARY KEY (source, location_id, variable, timestamp)` → idempotent re-fetch via `INSERT OR REPLACE`.

## `meta` — per-series provenance (one row per series)

`PRIMARY KEY (source, location_id, variable)`. Columns: `unit, aggregation, native_resolution,
fetched_at, endpoint, request_params (JSON), row_count, sentinel_masked, skill_version,
location_label, lat, lon`. Makes a dataset reproducible and self-describing.

## `findings` — created EMPTY here, owned by `AnalyseData`

`finding_id, type (correlation|anomaly|trend|fact), columns (JSON), period, statistic, confidence,
caveats (JSON — load-bearing over-claim guard), evidence, created_at, created_by`. Fetch writes
nothing here; an empty table means `TellDataStory` can run on data-only without erroring.
