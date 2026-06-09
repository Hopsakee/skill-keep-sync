# Chart Design — high-signal rules

These rules decide whether a chart *carries its story* or just *displays data*. They are baked into the line + scatter figures in `scripts/build_dashboard.py` and checked by `scripts/chart_lint.py`.

> **Attribution.** The principles below were learned from Goodeye Labs'
> [`high-signal-chart-workflow`](https://github.com/hugobowne/show-us-your-agent-skills/tree/main/skills/high-signal-chart-workflow)
> (Randal S. Olson; CC BY-NC-ND 4.0). Ideas only — no code was copied, and this is
> not a redistribution of that skill. Our charts are interactive Plotly, not static
> Tufte PNGs, so the rules are *adapted*, not transcribed.

## The rules (and why each one earns its place)

1. **Direct labels, not a legend.** A legend forces the reader to bounce between a colour swatch and a line. Label each series at its *end point* in the line's own colour. For a single series, label it with the metric name. **Why:** the eye should never leave the data to decode it. (We set `showlegend=False` and add an end-of-line annotation.)

2. **Clean lines on dense series; markers only when sparse.** Markers on 1,000+ points become a smear. Draw markers only when there are few enough points to read them (`MARKER_MAX_POINTS`, default 40). **Why:** ink that doesn't add information subtracts from what does.

3. **Whisper-faint gridlines, or none.** The default dark grid competes with the data. Drop the x-grid; render the y-grid at ~12% opacity. **Why:** gridlines are scaffolding, not content — they should be felt, not seen.

4. **Axis titles with units.** Never ship a raw column name (`precip_mm_sum`) as an axis title. `prettify()` de-snakes the name and pulls a recognised unit token into a trailing `(unit)`. **Why:** a number without a unit is not a measurement. Good unit labels depend on good column names in the source data.

5. **One accent colour; a muted palette otherwise.** Single-series charts use one accent (`ACCENT`); multi-entity charts draw from a muted palette, one stable colour per entity. **Why:** colour should encode meaning (which series), not decorate.

6. **Every chart has a title.** State what the chart is about, in words, including the mode (level / change / index). **Why:** a chart that needs a caption to be understood isn't finished.

7. **One story annotation.** Auto-mark the single biggest swing in view (the largest anomaly: "grootste piek/dal"). **Why:** a high-signal chart makes one point land. Don't annotate everything — annotate the thing.

8. **No chart chrome.** Hide zero-lines and any axis spine that isn't carrying information. **Why:** maximise the data-ink ratio (Tufte).

## What we deliberately did NOT adopt

- **Static `dpi=300` / `bbox_inches="tight"` / width ≥ 1200 px** — those are for matplotlib PNG export. Our output is a live interactive Plotly app, so they don't apply.
- **The 3-parallel-variant generation + external Truesight "Tufte test" LLM-judge loop** — heavier workflow, depends on an external paid API. Deferred. `scripts/chart_lint.py` is our lightweight, local, deterministic stand-in for the "tell it how to check it" idea.

## "Tell it how to check it"

The most transferable idea from the source: *don't just tell the agent what a good chart is — give it a checker.* `scripts/chart_lint.py` statically inspects the chart-builder functions for the structural rules above (title set, axis titles set, legend disabled, no unconditional `lines+markers`, a direct-label annotation, a story annotation). Run it after any change to the chart functions:

```bash
uv run --script scripts/chart_lint.py scripts/build_dashboard.py
```

Exit 0 = clean; exit 1 = one failed-rule name per line.
