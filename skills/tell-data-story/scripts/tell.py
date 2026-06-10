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

# Per-skill ResampleHelper (VIZ driver: span + budget + min/max/mean envelope). Same-skill sibling
# import only — NO cross-skill import (ISA Decision 2026-06-09). `uv run --script tell.py` puts this
# dir on sys.path[0]. The envelope only shapes the CHART; finding values stay read-from-row.
from resample import read_enveloped, POINT_BUDGET

SKILL_VERSION = "0.2.0"

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
    """(source, location_id, variable) -> {label, unit, native_resolution}. The display label combines
    the VARIABLE (the meaningful 'what') with the location_label (the 'where') so the narrative is
    self-contained. native_resolution is the floor the envelope helper must not bucket below."""
    out = {}
    for r in db.q("SELECT source, location_id, variable, unit, location_label, native_resolution FROM meta"):
        place = r.get("location_label") or r["location_id"]
        out[(r["source"], r["location_id"], r["variable"])] = {
            "label": f"{prettify_var(r['variable'])} — {place}",
            "unit": r.get("unit") or "",
            "native_resolution": r.get("native_resolution"),
        }
    return out


def split_key(key: str) -> tuple[str, str, str]:
    """'source:location_id:variable' -> parts. location_id may itself contain ':'."""
    parts = key.split(":")
    if len(parts) < 3:
        return ("", key, parts[-1])
    return (parts[0], ":".join(parts[1:-1]), parts[-1])


def series_bounds(db: Database, key: str) -> tuple[str, str] | None:
    """(min_ts, max_ts) for one series key, or None if it has no non-null rows. Cheap index-only
    metadata pass — picks the display window, computes nothing about the relationship."""
    source, location_id, variable = split_key(key)
    row = list(db.q(
        "SELECT MIN(timestamp) AS lo, MAX(timestamp) AS hi FROM data "
        "WHERE source=? AND location_id=? AND variable=? AND value IS NOT NULL",
        (source, location_id, variable),
    ))
    if not row or row[0]["lo"] is None:
        return None
    return row[0]["lo"], row[0]["hi"]


def series_meta(meta: dict, key: str) -> dict:
    return meta.get(split_key(key), {"label": key, "unit": "", "native_resolution": None})


def overlap_window(b_a: tuple[str, str] | None, b_b: tuple[str, str] | None) -> tuple[str, str] | None:
    """Intersection of two ISO-string [min,max] ranges (string compare is correct for ISO-8601). View
    windowing only — picks display bounds, computes nothing about the relationship."""
    if not b_a or not b_b:
        return None
    lo = max(b_a[0], b_b[0])
    hi = min(b_a[1], b_b[1])
    if lo > hi:
        return None
    return lo, hi


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
def _band_alpha(hex_color: str, alpha: float) -> str:
    """#RRGGBB -> rgba(r,g,b,alpha) for a faint min/max band fill in the series' own accent."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _add_series(fig: go.Figure, rows: list[dict], color: str, label: str, yaxis: str) -> None:
    """Add one series as a min/max BAND (lo..hi, faint fill) + a MEAN line. The band edges are
    drawn width-0 so only the fill reads; the mean is the line the eye follows. fill='tonexty' fills
    between the hi trace and the immediately-preceding lo trace on the SAME yaxis — so lo then hi must
    be added consecutively. Markers only when sparse (ChartDesign rule 2 / chart_lint hardcoded_markers)."""
    if not rows:
        return
    xs = [r["t"] for r in rows]                 # DB ISO strings (never pandas Timestamp)
    lo = [r["lo"] for r in rows]
    hi = [r["hi"] for r in rows]
    mean = [r["mean"] for r in rows]
    band_mode = "lines"
    mean_mode = "lines+markers" if len(rows) <= MARKER_MAX_POINTS else "lines"
    # lo edge (invisible), then hi edge filling down to it -> the min/max band.
    # connectgaps=False: a bucket with no data is a missing row, and the line/band MUST break there
    # rather than bridge a flat segment across the hole — bridging would paint continuous data where
    # there is none (fabrication-by-rendering; the REAL-DATA-OR-NO-DATA principle). Buckets are dense
    # within a series' coverage, so this only breaks the line at genuine coverage gaps.
    fig.add_trace(go.Scatter(x=xs, y=lo, mode=band_mode, name=f"{label} min",
                             line=dict(color=color, width=0), hoverinfo="skip",
                             connectgaps=False, yaxis=yaxis))
    fig.add_trace(go.Scatter(x=xs, y=hi, mode=band_mode, name=f"{label} max",
                             line=dict(color=color, width=0), fill="tonexty",
                             fillcolor=_band_alpha(color, 0.15), hoverinfo="skip",
                             connectgaps=False, yaxis=yaxis))
    fig.add_trace(go.Scatter(x=xs, y=mean, mode=mean_mode, name=label,
                             line=dict(color=color, width=1.6),
                             connectgaps=False, yaxis=yaxis))


def build_figure(f: dict, ma: dict, mb: dict,
                 rows_a: list[dict], rows_b: list[dict]) -> go.Figure:
    """One high-signal figure for a finding: per series a mean LINE + a min/max BAND (envelope from
    the viz ResampleHelper). Title + axis-units + one story annotation + direct end-labels + legend
    disabled, per References/ChartDesign.md. Computes no statistic; r/lag in the title come verbatim
    from the finding row, and the band's MAX preserves any peak the downsampling would otherwise hide."""
    same_unit = ma["unit"] == mb["unit"] and ma["unit"] != ""
    yaxis_b = "y" if same_unit else "y2"
    fig = go.Figure()

    _add_series(fig, rows_a, COLOR_A, ma["label"], "y")
    _add_series(fig, rows_b, COLOR_B, mb["label"], yaxis_b)

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

    # Direct end-labels (ChartDesign rule 1: label each series, not a legend) — at the mean's end.
    if rows_a:
        fig.add_annotation(x=rows_a[-1]["t"], y=rows_a[-1]["mean"], text=ma["label"], showarrow=False,
                           xanchor="left", font=dict(color=COLOR_A, size=11), xshift=6)
    if rows_b:
        fig.add_annotation(x=rows_b[-1]["t"], y=rows_b[-1]["mean"], text=mb["label"], showarrow=False,
                           xanchor="left", font=dict(color=COLOR_B, size=11), xshift=6,
                           yref=("y" if same_unit else "y2"))

    # One story annotation (ChartDesign rule 7): mark the peak of the primary series in view, taken
    # from the band's MAX so the true extremum survives downsampling. Extremum lookup for annotation
    # placement only — not a statistic about the relationship.
    if rows_a:
        peak_i = max(range(len(rows_a)), key=lambda i: rows_a[i]["hi"])
        fig.add_annotation(x=rows_a[peak_i]["t"], y=rows_a[peak_i]["hi"],
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

    # Attach a figure (or mark unrenderable) to each finding. Reads `data` via the viz envelope
    # helper (bucket+min/max/mean, downsampled in SQL); computes no statistic. The store is read-only.
    from datetime import datetime
    rendered = 0
    for f in findings:
        f["_fig"] = None
        f["_renderable"] = False
        if len(f["columns"]) < 2:
            continue
        ba = series_bounds(db, f["columns"][0])
        bb = series_bounds(db, f["columns"][1])
        win = overlap_window(ba, bb)
        if win is None:
            continue  # series absent or non-overlapping -> narrated honestly, no chart
        ma = series_meta(meta, f["columns"][0])
        mb = series_meta(meta, f["columns"][1])
        sa, la, va_ = split_key(f["columns"][0])
        sb, lb, vb_ = split_key(f["columns"][1])
        try:
            span_days = max((datetime.fromisoformat(win[1]) - datetime.fromisoformat(win[0]))
                            .total_seconds() / 86400.0, 0.0)
        except ValueError:
            span_days = None
        rows_a, _ga = read_enveloped(db.conn, sa, la, va_, ma.get("native_resolution"),
                                     POINT_BUDGET, window=win, span_days=span_days)
        rows_b, _gb = read_enveloped(db.conn, sb, lb, vb_, mb.get("native_resolution"),
                                     POINT_BUDGET, window=win, span_days=span_days)
        if not rows_a or not rows_b:
            continue
        f["_fig"] = build_figure(f, ma, mb, rows_a, rows_b)
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
