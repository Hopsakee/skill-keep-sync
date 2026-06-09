#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["fastlite", "pandas", "plotly"]
# ///
"""TellDataStory — stage 3 of the modular data pipeline (FetchData -> AnalyseData -> TellDataStory).

Minimal end-to-end STORY layer. Reads a SQLite package's `findings` rows (+ `caveats[]`, `evidence`)
and the `meta`/`data` tables, and renders a short SELF-CONTAINED human narrative plus one high-signal
Plotly chart per finding into two files next to the DB:
    <db-stem>_story.html   (narrative + caveats + interactive figures)
    <db-stem>_story.md     (narrative + caveats, readable cold)

Hard rules:
  - FAITHFUL RENDERER, NOT A SECOND ANALYST. Every number (r, lag, n, label) is read from the
    `findings` row verbatim. This script computes NO statistic of its own.
  - CAVEATS TRAVEL VERBATIM. Each string in caveats[] is rendered character-for-character.
  - DOMAIN-AGNOSTIC. Labels/units/meaning come from `meta` + the finding fields, never from
    constants in this file. The hydrology DB is a test board, not the subject.
  - fastlite for all SQLite access (no stdlib sqlite3). Reads only; never mutates data/findings.
  - The chart obeys the high-signal rules in References/ChartDesign.md and is checked by
    scripts/chart_lint.py. The local Claude-as-judge screenshot is the experiential gate.

This is the minimal story layer, not a dashboard. Interactive multi-control dashboards are
DataDashboardPython / DataDashboardTypeScript.
"""
from __future__ import annotations

import argparse
import html as _html
import json
import sys
from pathlib import Path

import plotly.graph_objects as go

from fastlite import Database

SKILL_VERSION = "0.1.0"

# Muted two-colour palette (ChartDesign rule 5: colour encodes which series, not decoration).
COLOR_A = "#4C9BE8"   # primary series accent
COLOR_B = "#E8A24C"   # secondary series accent
MARKER_MAX_POINTS = 40  # ChartDesign rule 2: markers only when sparse enough to read


# ---------------------------------------------------------------- IO (fastlite)
def open_db(path: str) -> Database:
    return Database(str(Path(path).expanduser()))


def _safe_json(raw, default):
    """Parse a JSON cell, degrading a malformed row instead of aborting the whole deliverable.
    A crashed finding is itself an under-report — never let one bad row hide every good one."""
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return default


def read_findings(db: Database) -> list[dict]:
    """All findings, oldest first, with JSON fields parsed. Malformed JSON degrades that one row
    (empty columns/caveats + a `_malformed` flag) rather than crashing the run."""
    rows = db.q("SELECT * FROM findings ORDER BY finding_id")
    out = []
    for r in rows:
        r = dict(r)
        cols = _safe_json(r.get("columns"), None)
        cav = _safe_json(r.get("caveats"), None)
        ev = _safe_json(r.get("evidence"), None)
        r["_malformed"] = cols is None or cav is None or ev is None
        r["columns"] = cols if isinstance(cols, list) else []
        r["caveats"] = cav if isinstance(cav, list) else []
        r["evidence"] = ev if isinstance(ev, dict) else {}
        out.append(r)
    return out


def prettify_var(variable: str) -> str:
    """De-snake a variable name for human reading. Domain-agnostic: works on any column name."""
    return variable.replace("_", " ").strip().capitalize() if variable else variable


def read_meta(db: Database) -> dict:
    """(source, location_id, variable) -> {label, unit}. The display label combines the VARIABLE
    (the meaningful 'what') with the location_label (the 'where') so the narrative is self-contained
    — e.g. 'Precipitation (KNMI 278)' rather than the cryptic station code 'KNMI 278 (RH)'."""
    out = {}
    for r in db.q("SELECT source, location_id, variable, unit, location_label FROM meta"):
        place = r.get("location_label") or r["location_id"]
        out[(r["source"], r["location_id"], r["variable"])] = {
            "label": f"{prettify_var(r['variable'])} — {place}",
            "unit": r.get("unit") or "",
        }
    return out


def split_key(key: str) -> tuple[str, str, str]:
    """'source:location_id:variable' -> parts. location_id may itself contain ':'."""
    parts = key.split(":")
    if len(parts) < 3:
        return ("", key, parts[-1])
    return (parts[0], ":".join(parts[1:-1]), parts[-1])


def read_series(db: Database, key: str) -> tuple[list[str], list[float]]:
    """Raw non-null observations for one series key, time-ordered. No resampling, no derived values."""
    source, location_id, variable = split_key(key)
    rows = db.q(
        "SELECT timestamp, value FROM data "
        "WHERE source=? AND location_id=? AND variable=? AND value IS NOT NULL "
        "ORDER BY timestamp",
        (source, location_id, variable),
    )
    ts = [r["timestamp"] for r in rows]
    vals = [r["value"] for r in rows]
    return ts, vals


def series_meta(meta: dict, key: str) -> dict:
    return meta.get(split_key(key), {"label": key, "unit": ""})


def overlap_window(ts_a: list[str], ts_b: list[str]) -> tuple[str, str] | None:
    """Intersection of two ISO-string time ranges (string compare is correct for ISO-8601). View
    windowing only — picks display bounds, computes nothing about the relationship."""
    if not ts_a or not ts_b:
        return None
    lo = max(ts_a[0], ts_b[0])
    hi = min(ts_a[-1], ts_b[-1])
    if lo > hi:
        return None
    return lo, hi


def clip(ts: list[str], vals: list[float], lo: str, hi: str) -> tuple[list[str], list[float]]:
    out_t, out_v = [], []
    for t, v in zip(ts, vals):
        if lo <= t <= hi:
            out_t.append(t)
            out_v.append(v)
    return out_t, out_v


# ---------------------------------------------------------------- narrative (deterministic)
def fmt_r(r) -> str:
    """Render the stored coefficient at its full stored precision. NOT rounded — rounding to 2-3
    decimals would fabricate a number absent from the findings row and break the verbatim invariant.
    `.6g` reproduces the stored value (e.g. 0.471501) without float-repr noise."""
    return f"{r:+.6g}"


def direction_phrase(r: float | None) -> str:
    """Read the SIGN of the already-computed r. Does not compute r."""
    if r is None:
        return "a relationship"
    if r > 0:
        return "a positive relationship (the two rise and fall together)"
    if r < 0:
        return "an inverse relationship (one tends to rise as the other falls)"
    return "no linear relationship"


def finding_title(f: dict, ma: dict, mb: dict) -> str:
    r = f.get("statistic")
    lag = f.get("evidence", {}).get("lag")
    bits = f"{ma['label']} ↔ {mb['label']}"
    if r is not None:
        bits += f": r = {fmt_r(r)}"
    if lag is not None:
        bits += f" at lag {lag}"
    return bits


def narrative_lines(f: dict, ma: dict, mb: dict, renderable: bool) -> list[str]:
    """Self-contained prose for one finding. Numbers quoted verbatim from the row/evidence."""
    ev = f.get("evidence", {})
    r = f.get("statistic")
    lag = ev.get("lag")
    n = ev.get("n")
    label = ev.get("baseline_label")
    source = ev.get("baseline_source")
    ftype = (f.get("type") or "correlation").strip().lower()

    if f.get("_malformed"):
        return [f"**{ma['label']} ↔ {mb['label']}** — this finding row could not be parsed "
                "(malformed JSON); it is shown but not narrated. Fix it in `AnalyseData`."]

    # Forward-guard: only correlation is narrated as "Pearson r". Future finding-types
    # (anomaly/trend/fact, the findings.type column already allows them) get a generic line so this
    # renderer can never over-claim a non-correlation finding as a correlation.
    if ftype != "correlation":
        generic = f"**{ma['label']} ↔ {mb['label']}** — a *{ftype}* finding"
        if r is not None:
            generic += f" (statistic = {fmt_r(r)})"
        generic += "."
        return [generic, "Read it with these caveats, exactly as the analysis recorded them:"]

    lead = f"**{ma['label']}** and **{mb['label']}** show {direction_phrase(r)}"
    facts = []
    if r is not None:
        facts.append(f"Pearson r = {fmt_r(r)}")
    if lag is not None:
        facts.append(f"at lag {lag}")
    if n is not None:
        facts.append(f"n = {n}")
    if facts:
        lead += ": " + ", ".join(facts)
    lead += "."
    if label:
        lead += f" Against the literature this was labelled **{label}**"
        if source:
            lead += f" ({source})"
        lead += "."
    else:
        lead += " (found in an exploratory pass — no literature baseline)."

    lines = [lead]
    if not renderable:
        lines.append(
            "The series for this finding are not both present (or do not overlap in time), so no "
            "chart is drawn — the caveats below explain why."
        )
    lines.append("Read it with these caveats, exactly as the analysis recorded them:")
    return lines


# ---------------------------------------------------------------- chart (Plotly, high-signal)
def build_figure(f: dict, ma: dict, mb: dict,
                 ta: list[str], va: list[float],
                 tb: list[str], vb: list[float]) -> go.Figure:
    """One high-signal figure for a finding. Title + axis-units + one story annotation + direct
    end-labels + legend disabled, per References/ChartDesign.md. Computes no statistic; r/lag in the
    title come from the finding row."""
    same_unit = ma["unit"] == mb["unit"] and ma["unit"] != ""
    fig = go.Figure()

    mode_a = "lines+markers" if len(ta) <= MARKER_MAX_POINTS else "lines"
    mode_b = "lines+markers" if len(tb) <= MARKER_MAX_POINTS else "lines"

    fig.add_trace(go.Scatter(x=ta, y=va, mode=mode_a, name=ma["label"],
                             line=dict(color=COLOR_A, width=1.6)))
    fig.add_trace(go.Scatter(x=tb, y=vb, mode=mode_b, name=mb["label"],
                             line=dict(color=COLOR_B, width=1.6),
                             yaxis=("y" if same_unit else "y2")))

    y1_title = ma["unit"] if same_unit else f"{ma['label']} ({ma['unit']})"
    # Secondary y-axis only when the two series carry different units. Toggled by value, not by a
    # second update_layout call — chart_lint.py checks EVERY update_layout for the high-signal kwargs,
    # so the figure must be configured in a SINGLE call with literal keyword arguments.
    y2_cfg = dict(
        title=(f"{mb['label']} ({mb['unit']})" if not same_unit else ""),
        overlaying="y", side="right", showgrid=False, zeroline=False,
        visible=(not same_unit),
    )
    fig.update_layout(
        title=finding_title(f, ma, mb),
        xaxis_title="time",
        yaxis_title=y1_title,
        showlegend=False,
        template="plotly_white",
        margin=dict(l=70, r=90, t=60, b=50),
        xaxis=dict(showgrid=False, zeroline=False),
        yaxis=dict(gridcolor="rgba(0,0,0,0.10)", zeroline=False),
        yaxis2=y2_cfg,
    )

    # Direct end-labels (ChartDesign rule 1: label each series, not a legend).
    if ta:
        fig.add_annotation(x=ta[-1], y=va[-1], text=ma["label"], showarrow=False,
                           xanchor="left", font=dict(color=COLOR_A, size=11), xshift=6)
    if tb:
        fig.add_annotation(x=tb[-1], y=vb[-1], text=mb["label"], showarrow=False,
                           xanchor="left", font=dict(color=COLOR_B, size=11), xshift=6,
                           yref=("y" if same_unit else "y2"))

    # One story annotation (ChartDesign rule 7): mark the peak of the primary series in view.
    # Pure extremum lookup for annotation placement — not a statistic about the relationship.
    if va:
        peak_i = max(range(len(va)), key=lambda i: va[i])
        fig.add_annotation(x=ta[peak_i], y=va[peak_i],
                           text=f"peak {ma['label']}", showarrow=True, arrowhead=2,
                           font=dict(size=11), bgcolor="rgba(255,255,255,0.7)")
    return fig


# ---------------------------------------------------------------- assembly
def render_html(db_path: Path, findings: list[dict], meta: dict, stamp: str) -> str:
    stem = db_path.stem
    parts = [
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">",
        f"<title>Data story — {_html.escape(stem)}</title>",
        "<style>",
        "body{font:16px/1.6 -apple-system,Segoe UI,Roboto,sans-serif;max-width:920px;"
        "margin:2.5rem auto;padding:0 1.2rem;color:#1c2230;background:#fafbfc;}",
        "h1{font-size:1.7rem;margin-bottom:.2rem;} h2{font-size:1.25rem;margin-top:2.4rem;}",
        ".meta{color:#667;font-size:.9rem;} .caveats{background:#f1f4f8;border-left:3px solid #4C9BE8;"
        "padding:.6rem 1rem;border-radius:4px;} .caveats li{margin:.3rem 0;}",
        "details{margin-top:2.5rem;color:#445;font-size:.92rem;}",
        "</style></head><body>",
        f"<h1>Data story — {_html.escape(stem)}</h1>",
        f"<p class=\"meta\">{len(findings)} finding(s) read from <code>{_html.escape(db_path.name)}</code>. "
        "Every statistic is quoted verbatim from the analysis stage; nothing is recomputed here. "
        f"Findings as of {_html.escape(stamp)}.</p>",
    ]
    first_fig = True
    for f in findings:
        ma = series_meta(meta, f["columns"][0]) if f["columns"] else {"label": "?", "unit": ""}
        mb = series_meta(meta, f["columns"][1]) if len(f["columns"]) > 1 else {"label": "?", "unit": ""}
        parts.append(f"<section><h2>{_html.escape(ma['label'])} ↔ {_html.escape(mb['label'])}</h2>")
        for line in narrative_lines(f, ma, mb, f.get("_renderable", False)):
            parts.append(f"<p>{_md_inline_to_html(line)}</p>")
        # Caveats VERBATIM: HTML-escape so the DISPLAYED text equals the stored string exactly even
        # when a caveat contains <, >, or & (escaping renders the same glyphs; without it the browser
        # would eat `<...>` as a tag and silently drop the hedge — an over-claim). The verbatim probe
        # compares the escaped form, mirroring this path.
        parts.append("<ul class=\"caveats\">")
        for cav in f["caveats"]:
            parts.append(f"<li>{_html.escape(cav)}</li>")
        parts.append("</ul>")
        if f.get("_fig") is not None:
            parts.append(f["_fig"].to_html(full_html=False,
                                           include_plotlyjs=("cdn" if first_fig else False),
                                           div_id=f"fig{f['finding_id']}",
                                           config={"displayModeBar": False}))
            first_fig = False
        parts.append("</section>")
    parts.append(
        "<details><summary>Data &amp; method</summary>"
        "<p>This is the story layer of a three-stage pipeline "
        "(<code>FetchData → AnalyseData → TellDataStory</code>). It is a faithful renderer: "
        "the correlation coefficient, lag, sample size and literature label are read verbatim from the "
        "<code>findings</code> table produced by <code>AnalyseData</code>; this stage computes no "
        "statistic. Each chart shows the raw observations of the two series over their overlapping time "
        "window (view windowing only — no resampling, no derived values). The caveats are rendered "
        "exactly as the analysis recorded them, so the story cannot claim more than the analysis did.</p>"
        "</details></body></html>"
    )
    return "\n".join(parts)


def _md_inline_to_html(line: str) -> str:
    """Minimal inline markdown -> HTML for **bold** in the deterministic narrative. Underscore-italic
    is deliberately NOT handled: a greedy `_`->`<em>` pass would mangle DB-derived labels and sources
    that contain underscores (the domain-agnostic principle: any package, any column name). The whole
    line is HTML-escaped first; only paired `**` markers this code emits are converted. Caveats are
    escaped + rendered separately and never pass through here."""
    esc = _html.escape(line)
    while esc.count("**") >= 2:
        esc = esc.replace("**", "<strong>", 1)
        esc = esc.replace("**", "</strong>", 1)
    return esc


def render_md(db_path: Path, findings: list[dict], meta: dict, stamp: str) -> str:
    stem = db_path.stem
    out = [
        f"# Data story — {stem}",
        "",
        f"{len(findings)} finding(s) read from `{db_path.name}`. Every statistic is quoted verbatim "
        f"from the analysis stage; nothing is recomputed here. Findings as of {stamp}.",
        "",
    ]
    for f in findings:
        ma = series_meta(meta, f["columns"][0]) if f["columns"] else {"label": "?", "unit": ""}
        mb = series_meta(meta, f["columns"][1]) if len(f["columns"]) > 1 else {"label": "?", "unit": ""}
        out.append(f"## {ma['label']} ↔ {mb['label']}")
        out.append("")
        for line in narrative_lines(f, ma, mb, f.get("_renderable", False)):
            out.append(line)
        out.append("")
        for cav in f["caveats"]:           # caveats VERBATIM
            out.append(f"- {cav}")
        out.append("")
        if f.get("_fig") is not None:
            out.append(f"_Chart: see `{stem}_story.html` for the interactive figure._")
            out.append("")
    out.append("---")
    out.append(
        "_Story layer of `FetchData → AnalyseData → TellDataStory`. Faithful renderer: "
        "r, lag, n and the literature label are read verbatim from the `findings` table; this stage "
        "computes no statistic. Caveats are rendered exactly as the analysis recorded them._"
    )
    return "\n".join(out)


def stamp_from(findings: list[dict]) -> str:
    """Deterministic 'generated' marker = newest finding's created_at. Never wall-clock, so two runs
    on the same package produce byte-identical output (idempotency)."""
    stamps = [f.get("created_at") for f in findings if f.get("created_at")]
    return max(stamps) if stamps else "unknown"


# ---------------------------------------------------------------- main
def main(argv=None):
    ap = argparse.ArgumentParser(description="TellDataStory — stage 3 story renderer")
    ap.add_argument("--db", help="SQLite package path (asks if omitted on a TTY)")
    ap.add_argument("--out-dir", help="Output directory (default: next to the DB)")
    args = ap.parse_args(argv)

    db_path = args.db
    if not db_path:
        if sys.stdin.isatty():
            db_path = input("Path to the SQLite package (.db): ").strip()
        else:
            ap.error("--db is required in non-interactive use")
    db_file = Path(db_path).expanduser()
    if not db_file.exists():
        ap.error(f"DB not found: {db_file}")

    db = open_db(str(db_file))
    findings = read_findings(db)
    if not findings:
        print(f"No findings in {db_file.name}. Run AnalyseData first to fill the findings table.")
        return 0

    meta = read_meta(db)

    # Attach a figure (or mark unrenderable) to each finding. Reads `data`; computes no statistic.
    rendered = 0
    for f in findings:
        f["_fig"] = None
        f["_renderable"] = False
        if len(f["columns"]) < 2:
            continue
        ta, va = read_series(db, f["columns"][0])
        tb, vb = read_series(db, f["columns"][1])
        win = overlap_window(ta, tb)
        if win is None:
            continue  # series absent or non-overlapping -> narrated honestly, no chart
        ca_t, ca_v = clip(ta, va, *win)
        cb_t, cb_v = clip(tb, vb, *win)
        if not ca_t or not cb_t:
            continue
        ma = series_meta(meta, f["columns"][0])
        mb = series_meta(meta, f["columns"][1])
        f["_fig"] = build_figure(f, ma, mb, ca_t, ca_v, cb_t, cb_v)
        f["_renderable"] = True
        rendered += 1

    stamp = stamp_from(findings)
    out_dir = Path(args.out_dir).expanduser() if args.out_dir else db_file.parent
    html_path = out_dir / f"{db_file.stem}_story.html"
    md_path = out_dir / f"{db_file.stem}_story.md"
    html_path.write_text(render_html(db_file, findings, meta, stamp), encoding="utf-8")
    md_path.write_text(render_md(db_file, findings, meta, stamp), encoding="utf-8")

    print(f"TellDataStory v{SKILL_VERSION}")
    print(f"  findings read : {len(findings)}")
    print(f"  charts drawn  : {rendered}")
    print(f"  html story    : {html_path}")
    print(f"  md story      : {md_path}")
    for f in findings:
        ma = series_meta(meta, f["columns"][0]) if f["columns"] else {"label": "?"}
        mb = series_meta(meta, f["columns"][1]) if len(f["columns"]) > 1 else {"label": "?"}
        r = f.get("statistic")
        rtxt = f"r={fmt_r(r)}" if r is not None else "r=n/a"
        chart = "chart" if f.get("_renderable") else "no-chart"
        print(f"    - {ma['label']} ~ {mb['label']}: {rtxt} ({len(f['caveats'])} caveats, {chart})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
