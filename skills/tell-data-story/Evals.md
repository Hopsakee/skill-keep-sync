# TellDataStory — Evals (durable binary pass/fail contract)

> CreateSkill Evals track. Each check is binary, graded in a fresh context against the stage-3 spec.
> The skill passes only when ALL checks pass. Grade in a window that did NOT write the skill.

## Setup

```bash
SK=~/.claude/skills/TellDataStory/scripts/tell.py
LINT=~/.claude/skills/TellDataStory/scripts/chart_lint.py
DB=~/data/sqlite/wdodelta-hydro.db
uv run --script $SK --db $DB        # produces <stem>_story.html + <stem>_story.md
HTML=~/data/sqlite/wdodelta-hydro_story.html
MD=~/data/sqlite/wdodelta-hydro_story.md
```

## Checks

| # | Check | Probe | Pass condition |
|---|-------|-------|----------------|
| E1 | Faithful renderer: no stat computed | `grep -nE "pearsonr|corrcoef|np\.corr|\.corr\(|scipy\.stats" $SK` | No match (0 lines) |
| E2 | fastlite only | `grep -n "import sqlite3" $SK` | No match (0 lines) |
| E3 | Domain-agnostic: no hydro literal in control flow | `grep -nE "precipitation|discharge|makkink|knmi|rws|groundwater" $SK` | Matches appear ONLY in docstring/comments, never in `if`/branch logic |
| E4 | Both output files written | `ls $HTML $MD` | both exist |
| E5 | Chart-lint clean | `uv run --script $LINT $SK` | exit 0 (prints "chart-lint OK") |
| E6 | Verbatim r in story | run, then check the precip→discharge r | `grep -F "+0.47" $HTML` AND in `$MD` → present |
| E7 | Verbatim ET caveat | `grep -F "potential" $HTML && grep -F "not actual" $HTML` | both present, exactly as stored |
| E8 | Caveat count preserved | for each finding, count `<li>` in its caveats block vs `len(caveats[])` from DB | equal |
| E9 | Self-contained narrative | open `$MD` | each series named by human label; no undefined abbreviation stands alone |
| E10 | Idempotent | `uv run --script $SK --db $DB; sha256sum $HTML $MD` twice; compare | identical hashes |
| E11 | TTY guard | `echo "" | uv run --script $SK` (no --db, non-interactive) | clear error "--db is required" |
| E12 | Never mutates tables | `SELECT COUNT(*) FROM data; SELECT COUNT(*) FROM findings` before/after | unchanged |
| E13 | Upstream scripts untouched | `git -C ~/.claude status --porcelain skills/FetchData/scripts/store.py skills/AnalyseData/scripts/analyse.py` | empty (no change) |
| E14 | Zero-findings grace | run against a package whose `findings` is empty | prints "run AnalyseData first", exit 0, no crash |
| E15 | Claude-as-judge | `Interceptor` screenshot of `$HTML` graded vs ChartDesign | PASS — figures render, titles+units present, one story annotation, direct labels |

## Stage boundary

Stage 3 = render only. A passing stage-3 skill does NOT find patterns (that is `AnalyseData`) and does
NOT build an interactive dashboard (that is `DataDashboard*`). E1 must show it computes no statistic.

## Geo-map mode (`scripts/map.py`) — significance-honest spatial render (a prior groundwater-trends tool, 2026-06)

Binary checks (a package with per-location `trend` findings + `meta.lat/lon`):

- [ ] `map.py --db PKG` writes a single self-contained `<stem>_map.html` that opens via file:// (no server) with 0 JS console errors.
- [ ] One marker per mappable finding at its `meta` lat/lon; a diverging colour legend is present.
- [ ] A non-significant finding renders HATCHED (marker fill = `url(#hatchN)`); a `suspect` finding renders grey (`#999999`); a significant one is solid.
- [ ] The renderer computes NO statistic — every shown value (slope, p, n, caveats) is read from the finding row (grep: no scipy/.corr()/theilslopes in map.py).
- [ ] No interpolated surface/heatmap between points (markers only); a multi-series finding or one lacking meta coords is skipped and reported, never guessed.
- [ ] Popup caveats are HTML-escaped (verbatim display), matching the story renderer's verbatim-caveat rule.
