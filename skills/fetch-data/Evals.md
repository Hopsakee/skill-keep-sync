# FetchData — Evals

Binary pass/fail checks graded in a fresh context. Two surfaces: the core fetch contract and the
BRO discovery mode.

## BRO discovery (`scripts/discover_bro.py`) — area → wells-file (a prior groundwater-trends tool, 2026-06)

Binary checks (run against the cached BRO data or a small live area):

- [ ] `discover_bro.py --box LA0,LA1,LO0,LO1 --from .. --to .. --n N --out F` writes `F` = `{"wells":[{label,gmw,tube,...}]}` with up to N wells.
- [ ] The output wells-file is consumable verbatim by the normal fetch path: `fetch._load_wells_file(F)` parses it into (label,gmw,tube) triples with no error.
- [ ] Cells are visited in dispersed (farthest-point) order: with a small `--n`, the kept wells span the box extent (not clustered along one edge).
- [ ] A cheap `observationsSummary` span pre-filter runs before any full `seriesAsCsv` download; per-key caches (boxes/gldids/summary/series) are written and reused (a re-run hits cache, no new network for cached keys).
- [ ] `--exclude` (an existing wells-file) and an existing `--out` both dedup by `gmw`: already-known wells are never re-added.
- [ ] Only wells meeting the completeness rule (≥ `--min-years` covered hydrological years, record spanning near `--from`..`--to`) are written; a network failure on one well is isolated, never aborting the run (the wells-file is checkpointed per well).
