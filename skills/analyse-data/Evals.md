# AnalyseData — Evals (durable binary pass/fail contract)

> CreateSkill Evals track. Each check is binary, graded in a fresh context against the rung-2 spec.
> Run the probes; the skill passes only when ALL checks pass.

## Setup

```bash
SK=~/.claude/skills/AnalyseData/scripts/analyse.py
DB=~/data/sqlite/wdodelta-hydro.db
```

## Checks

| # | Check | Probe | Pass condition |
|---|-------|-------|----------------|
| E1 | Domain-agnostic: no hydro literals in control flow | `grep -nE "precipitation|discharge|makkink|knmi|rws" scripts/analyse.py` | Matches appear ONLY in caveat strings / examples, never in `if`/branch logic |
| E2 | fastlite only | `grep -n "import sqlite3" scripts/analyse.py` | No match (0 lines) |
| E3 | STL runs end-to-end (rung 2) | `uv run --script $SK --db $DB --granularity monthly --deseasonalize stl --baseline /tmp/baseline_hydro_daily.json --lag-min 0 --lag-max 2` | Exits 0, writes findings, NO `NotImplementedError` / traceback; evidence/caveats record `deseasonalize=stl` |
| E4 | Autonomous pass writes findings | `uv run --script $SK --db $DB --granularity daily --lag-max 3` then `SELECT COUNT(*) FROM findings WHERE type='correlation'` | ≥1 finding row |
| E5 | Every finding has non-empty caveats[] | `SELECT COUNT(*) FROM findings WHERE caveats IS NULL OR caveats='[]'` | 0 |
| E6 | ET caveat auto-attaches | run a pass including a Makkink series; `SELECT caveats FROM findings WHERE columns LIKE '%makkink%'` | contains "potential" + "not actual" |
| E7 | Idempotent re-run | run the same pass twice; compare `COUNT(*)` | counts equal (no duplicates) |
| E8 | Baseline labelling | run with a 2-hypothesis baseline (one present pair, one absent series) | one finding labelled consistent/weaker/surprising, one labelled `absent` with no fabricated r |
| E9 | Human summary | stdout of any run | non-empty, self-contained prose naming findings + caveats |
| E10 | Never mutates data | `SELECT COUNT(*) FROM data` before/after a run | unchanged |
| E11 | Never charts | `grep -nE "matplotlib|plotly|seaborn|\.savefig" scripts/analyse.py` | 0 matches |
| E12 | store.py untouched | `git -C ~/.claude status --porcelain skills/FetchData/scripts/store.py` | empty (no change) |
| E13 | anomaly finding-type | `uv run --script $SK --db $DB --granularity daily --findings anomaly` then `SELECT type,caveats FROM findings WHERE type='anomaly'` | ≥1 row with `type='anomaly'`, each with non-empty `caveats[]` (incl. a threshold-dependence + serial-autocorrelation note) |
| E14 | trend finding-type | `uv run --script $SK --db $DB --granularity monthly --findings trend` then `SELECT type,caveats FROM findings WHERE type='trend'` | ≥1 row with `type='trend'`, statistic = Theil-Sen slope, each with non-empty `caveats[]` (incl. Mann-Kendall serial-autocorrelation note) |
| E15 | fact finding-type | `uv run --script $SK --db $DB --granularity daily --findings fact` then `SELECT type,caveats FROM findings WHERE type='fact'` | ≥1 row with `type='fact'`, statistic = mean, evidence has n/mean/std/min/max/coverage, non-empty `caveats[]` |
| E16 | effective-N widens p | run the SAME baseline pair twice: once default (eff-N on) and once `--no-eff-n`; read `evidence.p_corrected` for the autocorrelated `precipitation~groundwater_level` finding | eff-N `p_corrected` ≥ no-eff-n `p_corrected` (widened, not narrowed); eff-N evidence carries `n_eff`, `r1x`, `r1y` |
| E17 | two-sided scan surfaces negative lag | construct a baseline where `b` leads `a` (expected reverse lead) OR run autonomous `--two-sided --lag-max 5`; inspect findings | at least one finding reports a negative `lag` in evidence (reverse lead found without a hand-set negative `--lag-min`) |
| E18 | absent→resolved dedup | run a baseline twice on a hypothesis whose series is present, but FIRST inject an absent run for the same hypothesis tokens; after the resolved run `SELECT COUNT(*) FROM findings WHERE period LIKE 'baseline-absent%' AND period LIKE '%[hyp:precipitation~discharge]%'` | 0 stale `baseline-absent` rows for a hypothesis that has since resolved (the `[hyp:...]` tag supersedes both absent and resolved across the transition) |
| E19 | regression invariant | `uv run --script $SK --db $DB --granularity daily --deseasonalize none --baseline /tmp/baseline_hydro_daily.json --lag-min 0 --lag-max 3` | reports `precipitation ~ ...discharge: r=+0.49 ... n=92` (r and n unchanged from rung 1; p MAY differ due to eff-N) |

## Rung boundary

Rung 2 = correlation + anomaly + trend + fact + STL deseasonalization + effective-sample-size p +
two-sided lag scan + stable-hypothesis-identity dedup. The DEFAULT run (no `--findings` selector)
stays correlation-only with rung-1 behaviour so existing idempotency holds; the new finding-types
fire only when named via `--findings`. STL is OPT-IN (`--deseasonalize stl`); `monthly` and `none`
are unchanged. Rung 3 (a 2nd domain to re-prove agnosticism) is the next rung.

## Trend finding-type — significance + plausibility (a prior groundwater-trends tool, 2026-06)

Binary checks (synthetic package: declining / rising / flat / steep / 3-point series; `--findings trend`):

- [ ] A monotone declining series yields a trend row with `statistic` (slope) < 0; a rising series > 0.
- [ ] Each trend row's evidence has a `significant` boolean = (Mann–Kendall p < alpha); a clean monotone series is significant, a flat/alternating series is NOT.
- [ ] Each trend row exposes significance in `evidence.significant` (NOT in `confidence`, which stays null so it is not confused with correlation's corrected confidence).
- [ ] With `--trend-plausible-max B`, a series whose |slope| > B has evidence `suspect=true`; others `suspect=false`.
- [ ] A series with fewer than `--trend-min-n` valid points produces NO trend row (no fabricated slope).
- [ ] `--trend-window START,END` restricts the fit: evidence `window` records [START,END] and `n` drops to the in-window count.
