---
name: DataAnalysis
description: "Autonomous data analysis for a single CSV. Runs a deterministic profiler (`scripts/profile.py`, pandas + matplotlib) that writes `<name>_profile.json` + PNG charts (numeric distributions, correlation heatmap, categorical counts) NEXT TO the input file, then reads the profile + views the charts and writes `<name>_analysis.md` next to the input — a 3-bullet exec summary, findings each backed by a referenced chart, and the raw profile table at the bottom. Runs fully blind (no question needed); an optional focusing question narrows the narrative. All output lands in the dataset's own folder. USE WHEN analyse csv, analyze csv, profile dataset, data analysis, explore dataset, EDA, exploratory data analysis, summarise data, summarize csv, describe dataset, analyse data, what's in this csv, data report, chart this data, visualise dataset. NOT FOR Excel/Parquet/JSON inputs (CSV only for now — convert first), database queries (no connection layer), multi-file joins or pipelines, statistical modelling / ML training, ingesting documents into the Library (use _TO_LIBRARY), or wisdom extraction from prose (use ExtractWisdom)."
---

# DataAnalysis

Autonomous, single-CSV data analysis. Code does the math; the narrative pass does the prose.

The profiler is deterministic and reproducible — it computes the facts and renders the charts. The agent then reads those facts and writes a clean, understandable report. The split exists because stats + plots are exactly what code does well, and narrative synthesis is exactly what code does badly. Keep that boundary: **never hand-compute numbers the profiler already produced, and never bury a claim that has no chart to back it.**

## Voice Notification

**When executing the workflow, do BOTH:**

1. **Send voice notification**:
   ```bash
   curl -s -X POST http://localhost:31337/notify \
     -H "Content-Type: application/json" \
     -d '{"message": "Analysing the dataset in DataAnalysis"}' \
     > /dev/null 2>&1 &
   ```
2. **Output text notification**:
   ```
   Running **DataAnalysis** to profile and report on the dataset...
   ```

## Workflow Routing

| Workflow | Trigger | Steps |
|----------|---------|-------|
| **Analyse CSV** | "analyse csv", "profile dataset", "data analysis", "EDA" | The 3 steps below |

## Workflow (`analyse csv`)

1. **Run the profiler.**
   ```bash
   uv run --script ~/.claude/skills/DataAnalysis/scripts/profile.py <path-to.csv>
   ```
   It writes `<name>_profile.json` and a handful of `<name>_*.png` charts into the CSV's own
   folder, and prints a one-line summary (rows × cols, chart count). Use `--dry-run` to preview
   without writing, `--max-charts N` to cap the PNGs.

2. **Read the facts.** Read `<name>_profile.json` (the source of truth — shape, per-column
   dtype/missing/cardinality, numeric `describe()`, categorical top-values, top correlations,
   and a `charts` manifest mapping each PNG to what it describes). View the PNGs the manifest
   lists so each finding you write has a chart behind it.

3. **Write the report.** Write `<name>_analysis.md` next to the input with this shape:
   - **A 3-bullet executive summary at the top** — the three things that matter most.
   - **Findings** — each finding states an observation grounded in `profile.json` and references
     the chart that shows it (embed via the relative filename from the manifest, e.g.
     `![distribution of X](<name>_dist_x.png)`). No claim without a chart or a profile number
     behind it (the high-signal-chart discipline).
   - **Raw profile table at the bottom** — a compact markdown table of the per-column profile
     (dtype, missing %, n_unique) so the detail is there without cluttering the findings.

   If the user gave a focusing question, lead the findings with it; otherwise report what the
   data itself surfaces (blind mode).

## What gets written

| File | Written by | Behaviour |
|---|---|---|
| `<name>_profile.json` | `profile.py` | The facts. Overwritten cleanly on re-run. |
| `<name>_dist_*.png`, `<name>_corr_heatmap.png`, `<name>_counts_*.png` | `profile.py` | Chart evidence, capped by `--max-charts`. |
| `<name>_analysis.md` | narrative pass | The deliverable: exec summary + viz-backed findings + raw table. |

Everything lands in the **dataset's own folder** — never in a staging or Library path.

## Gotchas

- **Use `uv run --script`, not bare `python3`.** The pandas + matplotlib deps live in the
  script's PEP-723 inline metadata; a bare `python3` call fails with `ModuleNotFoundError`.
- **The profiler never writes prose, and the narrative pass never recomputes stats.** If you
  catch yourself eyeballing the CSV to compute a mean, stop — it's already in
  `profile.json["numeric_summary"]`. If you catch yourself stating a trend with no chart, either
  reference the matching PNG from the manifest or drop the claim.
- **Charts are capped (`--max-charts`, default 15).** Wide datasets won't emit one PNG per
  column. Categorical count-plots are skipped above 20 unique values (a 5000-bar chart is noise).
  If a column you want to discuss has no chart, raise `--max-charts` or note it from the profile
  numbers instead of inventing a visual.
- **CSV only.** Excel/Parquet/JSON are out of scope for now — convert to CSV first. This is a
  deliberate "simplest thing that lands" boundary, not an oversight; extend `load_csv` when a
  real second format shows up, don't bolt on guesses.
- **Output overwrites on re-run.** Filenames derive from the CSV stem, so a second run replaces
  the prior profile + charts in place. Intentional — keeps the dataset folder clean.

## Execution Log

After completing the workflow, append one JSONL entry:
```bash
echo '{"ts":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","skill":"DataAnalysis","workflow":"analyse-csv","input":"8_WORD_SUMMARY","status":"ok|error","duration_s":SECONDS}' >> ~/.claude/PAI/MEMORY/SKILLS/execution.jsonl
```

## Examples

**Example 1: blind analysis**
```
User: "analyse ~/data/sales.csv"
→ uv run --script .../profile.py ~/data/sales.csv  (writes sales_profile.json + charts)
→ reads profile + views PNGs
→ writes ~/data/sales_analysis.md: 3-bullet summary, viz-backed findings, raw table
```

**Example 2: focused question + preview**
```
User: "what drives revenue in ~/data/sales.csv?"
→ profile.py --dry-run first to confirm shape, then the real run
→ findings lead with the revenue correlations from top_correlations, each backed by a chart
```

## Related

- Prose synthesis pattern (for narrative depth): `ExtractWisdom`
