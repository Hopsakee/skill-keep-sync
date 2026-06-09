# TellDataDashboard — Evals (durable binary pass/fail contract)

> CreateSkill Evals track. Each check is binary, graded in a fresh context against the stage-3
> live data-browsing dashboard spec. The skill passes only when ALL checks pass. Grade in a window
> that did NOT write the skill.

## Setup

```bash
SK=~/.claude/skills/TellDataDashboard/scripts/dashboard.py
LINT=~/.claude/skills/TellDataDashboard/scripts/chart_lint.py
DB=~/data/sqlite/wdodelta-hydro.db          # real: 3 sources, 19 series, has findings
SYN=~/data/sqlite/synthetic-generic.db      # domain-agnostic, fast
```

## Checks

| # | Check | Probe | Pass condition |
|---|-------|-------|----------------|
| E1 | fastlite only, no stdlib sqlite3 | `grep -n "import sqlite3" $SK` | no match |
| E2 | Findings page computes no statistic | `grep -nE "pearsonr\|corrcoef\|scipy\.stats\|\.corr\(" $SK` | no match (findings render is pure read) |
| E3 | Verbatim r at stored precision | `grep -n "\.6g" $SK` | present (fmt_r uses `.6g`) |
| E4 | No silent gap-fill | `grep -nE "connectgaps\|ffill\|interpolate\|fillna" $SK` | every `connectgaps` is `=False`; no fill/interpolate/fillna |
| E5 | Domain-agnostic | `grep -niE "precipitation\|discharge\|makkink\|knmi\|rws\|groundwater" $SK` | matches only in docstring/comments, never branch logic |
| E6 | Meta-driven schema (no infer_schema) | `grep -n "infer_schema" $SK` | no match |
| E7 | Read-only — package unchanged | `SELECT COUNT(*)` on data/meta/findings before & after `--check` | all three equal |
| E8 | Chart-lint clean | `uv run --script $LINT $SK` | exit 0 |
| E9 | One page PER table | `grep -c 'ui.tab(' $SK` | ≥3 (Data, Reeksen/meta, Bevindingen) |
| E10 | Multi-select on BOTH location and parameter | `grep -c "multiple=True" $SK` | ≥2 |
| E11 | `--inspect` works on real DB | `uv run --script $SK --db $DB --inspect` | exit 0; prints series/sources/parameters/meetpunten/period/findings/tabs |
| E12 | `--check` works on real DB | `uv run --script $SK --db $DB --check` | exit 0; prints "check OK …" |
| E13 | `--check` works on synthetic DB (agnostic) | `uv run --script $SK --db $SYN --check` | exit 0 |
| E14 | TTY guard | `echo "" \| uv run --script $SK` | clear error "--db is required" |
| E15 | Live page serves | launch `--port N`, `curl -s -o /dev/null -w '%{http_code}'` (GET) | 200 (NOT HEAD — that's 405) |
| E16 | Smart default is non-empty | open the live Data tab | the chart paints data (widest-coverage parameter + its covered locations), not "Geen data …" |
| E17 | Dual y-axis on 2 params; consistent colour | open Data tab, select 2 parameters | param 1 on left y-axis (solid), param 2 on right y-axis (dashed); a location is one consistent colour; units on each axis |
| E17b | Parameter cap = 2 | try to select a 3rd parameter | selection truncates to 2 + a "max 2" notify; `grep "MAX_PARAMS = 2"` present |
| E17c | Meetpunten filtered to selected parameter(s) | select a parameter | only points measuring it appear; `grep "def loc_options"` filters by `place_params & sel_params` |
| E17d | Per-point parameter codes | open Data tab | each Meetpunt shows a `[CODE]` prefix; a legend maps codes → parameter names |
| E17e | Slider handles show dates | hover/drag a slider handle | the popup shows the period date (ISO), not the index; `grep "left-label-value"` present |
| E18 | Browsable value table | open Data tab | a Periode·Meetpunt·Parameter·Waarde table renders for the selection; if capped, the cap is stated |
| E19 | Meta page = full provenance table | open Reeksen tab | every series listed with source/place/variable/unit/resolution/row_count |
| E20 | Findings page faithful + verbatim | open Bevindingen tab; compare r vs `findings.statistic` | r matches at `.6g`; caveats verbatim, count preserved |
| E21 | Empty/blank-selection state | clear all locations or parameters | a clear "Selecteer …" / "Geen data …" message, not a broken blank canvas |
| E22 | Upstream scripts untouched | `git -C ~/.claude status --porcelain skills/FetchData/scripts/store.py skills/AnalyseData/scripts/analyse.py` | empty |
| E23 | tz-mix doesn't crash | `grep -n "utc=True" $SK` | present in resample_one (mixed tz-aware/naive normalised) |
| E24 | Claude-as-judge | Interceptor screenshots of all three tabs (live browser) | PASS — Data shows multi-series faceted chart + table; Reeksen shows the meta table; Bevindingen shows verbatim findings |

## Stage boundary

Stage 3 (live) = BROWSE the data + faithfully surface findings, never re-analyse. A passing skill
does NOT find patterns (that is `AnalyseData`), does NOT read loose CSV (that is
`DataDashboardPython`), and does NOT recompute the canonical findings. E2 + E20 prove the findings
page is a faithful renderer; E4 proves no fabricated continuity; E9/E10/E16/E17 prove the
data-browsing core (page-per-table, multi-select points + parameters, honest faceted viz).
