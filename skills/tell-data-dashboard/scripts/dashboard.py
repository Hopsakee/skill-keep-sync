#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "nicegui>=2.0",
#     "plotly>=5.20",
#     "fastlite",
# ]
# ///
"""dashboard.py — TellDataDashboard: a live dashboard to BROWSE a finished SQLite PACKAGE.

Stage 3 (live-app sibling of TellDataStory) of the modular data pipeline
(FetchData -> AnalyseData -> {TellDataStory | TellDataDashboard}).

Reads the three-table package and serves a reactive NiceGUI dashboard with ONE PAGE PER TABLE:

  • Data       — pick a parameter (or two) AND one-or-many measurement points; read the data as a
                 line chart. ONE parameter -> single left y-axis. TWO parameters -> parameter 1 on
                 the LEFT y-axis (solid lines), parameter 2 on the RIGHT y-axis (dashed). Locations
                 are one consistent colour each; a value table sits below. The Meetpunten list is
                 filtered to points that actually measure the selected parameter(s), and each point
                 is prefixed with a short code for the parameters it carries. Max 2 parameters.
  • Reeksen    — the full `meta` provenance table (every series).
  • Bevindingen— the `findings` rows rendered FAITHFULLY (r, lag, n, caveats VERBATIM, .6g).

ADAPTIVE RESOLUTION (ISA 20260609 — VIZ driver). The package stores RAW, native-resolution data.
The dashboard never ships raw points to the browser: zoom IS the resolution selector. Each visible
series is read via `resample.read_enveloped`, which picks a bucket grain from the visible span
against a ~2500-point budget (floored at the series' native_resolution) and aggregates IN SQL into a
min/max/mean envelope. At full zoom (raw count under budget) raw points are returned untouched so
peaks are never hidden. Storage is never written. DOMAIN-AGNOSTIC: metric=variable, entity=location.

Usage:
    uv run --script dashboard.py --db package.db            # serve on :8080
    uv run --script dashboard.py --db package.db --inspect  # print detected shape, exit
    uv run --script dashboard.py --db package.db --check     # build UI+figures headless, exit 0
    uv run --script dashboard.py --db package.db --port 8099
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from fastlite import Database

# Per-skill ResampleHelper (sibling import within THIS skill — NOT a cross-skill import).
sys.path.insert(0, str(Path(__file__).resolve().parent))
import resample  # noqa: E402  (choose_grain / read_enveloped / POINT_BUDGET — the VIZ driver)

SKILL_VERSION = "0.4.0"

PALETTE = [
    "#38bdf8", "#f97316", "#a78bfa", "#34d399", "#f472b6", "#facc15",
    "#60a5fa", "#fb7185", "#4ade80", "#c084fc", "#fbbf24", "#22d3ee",
    "#e879f9", "#2dd4bf", "#fdba74", "#93c5fd", "#86efac", "#f0abfc",
]
MUTED_GRID = "rgba(148,163,184,0.12)"
MARKER_MAX_POINTS = 40
TABLE_ROW_CAP = 5000
MAX_PARAMS = 2                                   # dual y-axis ceiling
WINDOW_STEPS = 1000                              # slider granularity over the time axis (zoom)


def prettify_var(variable: str) -> str:
    if not variable:
        return variable
    return variable.replace("_", " ").strip().capitalize()


def param_codes(variables) -> dict:
    """Stable short letter-code per variable (domain-agnostic: derived from the name). Shown before
    each Meetpunt so the reader sees at a glance which parameters that point measures."""
    codes, used = {}, set()
    for v in sorted(variables):
        base = "".join(w[0] for w in v.split("_") if w)[:3].upper() or v[:2].upper()
        code, i = base, 1
        while code in used:
            i += 1
            code = f"{base}{i}"
        used.add(code)
        codes[v] = code
    return codes


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _parse_iso(s: str) -> datetime | None:
    """Tolerant ISO-8601 parse -> naive datetime (tz stripped) for span arithmetic only."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace(" ", "T"))
    except ValueError:
        try:
            dt = datetime.fromisoformat(str(s)[:19])
        except ValueError:
            return None
    return dt.replace(tzinfo=None)


# ── package read ──────────────────────────────────────────────────────────────
def open_db(path: str) -> Database:
    """fastlite handle for the small meta/findings reads."""
    return Database(str(Path(path).expanduser()))


def open_conn(path: str) -> sqlite3.Connection:
    """Read-only DBAPI connection for the envelope series reads (resample.read_enveloped)."""
    uri = f"file:{Path(path).expanduser()}?mode=ro"
    return sqlite3.connect(uri, uri=True, check_same_thread=False)


def _safe_json(raw, default):
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return default


def read_meta_rows(db: Database) -> list[dict]:
    out = []
    for r in db.q("SELECT source, location_id, variable, unit, aggregation, native_resolution, "
                  "row_count, lat, lon, location_label FROM meta"):
        d = dict(r)
        d["place"] = d.get("location_label") or d["location_id"]
        d["label_var"] = prettify_var(d["variable"])
        out.append(d)
    return out


def overall_span(conn: sqlite3.Connection):
    """(min_iso, max_iso, span_days) over the whole `data` table — read-only."""
    row = conn.execute("SELECT MIN(timestamp), MAX(timestamp) FROM data").fetchone()
    if not row or not row[0] or not row[1]:
        return None, None, 0
    lo, hi = row[0], row[1]
    lo_dt, hi_dt = _parse_iso(lo), _parse_iso(hi)
    days = int((hi_dt - lo_dt).total_seconds() / 86400.0) if (lo_dt and hi_dt) else 0
    return lo, hi, max(days, 0)


def read_findings(db: Database) -> list[dict]:
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


def split_key(key: str) -> tuple[str, str, str]:
    parts = key.split(":")
    if len(parts) < 3:
        return ("", key, parts[-1])
    return (parts[0], ":".join(parts[1:-1]), parts[-1])


def findings_label_map(meta_rows: list[dict]) -> dict:
    out = {}
    for m in meta_rows:
        out[(m["source"], m["location_id"], m["variable"])] = f"{m['label_var']} — {m['place']}"
    return out


def fmt_r(r) -> str:
    return f"{r:+.6g}"


def finding_title(f: dict, lab: dict) -> str:
    a = lab.get(split_key(f["columns"][0]), f["columns"][0]) if f["columns"] else "?"
    b = lab.get(split_key(f["columns"][1]), f["columns"][1]) if len(f["columns"]) > 1 else "?"
    r = f.get("statistic")
    lag = f.get("evidence", {}).get("lag")
    bits = f"{a} ↔ {b}"
    if r is not None:
        bits += f": r = {fmt_r(r)}"
    if lag is not None:
        bits += f" at lag {lag}"
    return bits


def finding_subline(f: dict) -> str:
    ev = f.get("evidence", {})
    ftype = (f.get("type") or "correlation").strip().lower()
    bits = [f"type: {ftype}"]
    if ev.get("n") is not None:
        bits.append(f"n = {ev['n']}")
    if ev.get("baseline_label"):
        lab = ev["baseline_label"]
        src = ev.get("baseline_source")
        bits.append(f"vs literature: {lab}" + (f" ({src})" if src else ""))
    if f.get("_malformed"):
        return "this finding row could not be parsed (malformed JSON) — shown, not narrated"
    return " · ".join(bits)


# ── transforms (within-series; applied to the per-bucket mean) ────────────────
def yoy(values):
    out = [None]
    for i in range(1, len(values)):
        a, b = values[i], values[i - 1]
        out.append(None if (a is None or b is None or b == 0) else (a - b) / abs(b) * 100.0)
    return out


def rebase(values):
    base = next((v for v in values if v is not None), None)
    if base is None or base == 0:
        return values
    return [None if v is None else v / base * 100.0 for v in values]


def apply_mode(values, mode):
    return yoy(values) if mode == "change" else (rebase(values) if mode == "rebase" else values)


# ── presentation layer (NiceGUI, one tab per table) ──────────────────────────
def build_dashboard(db_path: Path, port: int, check_only: bool = False):
    import plotly.graph_objects as go
    from nicegui import ui

    db = open_db(str(db_path))
    conn = open_conn(str(db_path))
    meta_rows = read_meta_rows(db)
    if not meta_rows:
        raise ValueError(f"no `meta` rows in {db_path.name} — is this a pipeline package?")

    findings = read_findings(db)
    flabels = findings_label_map(meta_rows)
    min_iso, max_iso, span = overall_span(conn)
    min_dt, max_dt = _parse_iso(min_iso), _parse_iso(max_iso)

    # series lookups (meta only — cheap; no data read just to set up)
    variables = sorted({m["variable"] for m in meta_rows})
    places = sorted({m["place"] for m in meta_rows})
    place_params: dict[str, set] = {}
    metric_labels, units, native_by_key, key_for, place_rowcount = {}, {}, {}, {}, {}
    for m in meta_rows:
        k = (m["source"], m["location_id"], m["variable"])
        native_by_key[k] = m.get("native_resolution")
        key_for.setdefault((m["variable"], m["place"]), k)
        place_params.setdefault(m["place"], set()).add(m["variable"])
        units.setdefault(m["variable"], m.get("unit") or "")
        unit = units[m["variable"]]
        metric_labels[m["variable"]] = f"{m['label_var']}" + (f" ({unit})" if unit else "")
        place_rowcount[(m["variable"], m["place"])] = int(m.get("row_count") or 0)

    colors = {p: PALETTE[i % len(PALETTE)] for i, p in enumerate(places)}
    pcodes = param_codes(variables)

    def rowcount_of_param(v):
        return sum(c for (var, _p), c in place_rowcount.items() if var == v)

    default_param = max(variables, key=rowcount_of_param) if variables else None

    def locs_for(params):
        ps = set(params or [])
        return [p for p in places if (place_params.get(p, set()) & ps)] if ps else list(places)

    def default_locs(params):
        cand = locs_for(params)
        cand = sorted(cand, key=lambda p: -max((place_rowcount.get((v, p), 0) for v in params),
                                               default=0))
        return cand[: min(4, len(cand))]

    init_params = [default_param] if default_param else []
    state = {
        "win": (min_iso, max_iso),            # current visible window (ISO bounds; None,None if empty)
        "sel_params": init_params,
        "sel_locations": default_locs(init_params),
        "mode": "level",
        "_env": {},                           # memo: (key, win) -> (rows, grain)
    }

    def iso_at(frac: float) -> str | None:
        if min_dt is None or max_dt is None:
            return None
        dt = min_dt + (max_dt - min_dt) * max(0.0, min(1.0, frac))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    def read_one(param, loc):
        """Envelope read for one (param, location) over the current window. Memoised per window so
        the chart and the table don't double-read. NEVER returns more than POINT_BUDGET points."""
        k = key_for.get((param, loc))
        if not k:
            return [], None
        win = state["win"]
        cache_key = (k, win)
        if cache_key in state["_env"]:
            return state["_env"][cache_key]
        window = win if (win and win[0] and win[1]) else None
        span_days = None
        if window:
            a, b = _parse_iso(window[0]), _parse_iso(window[1])
            if a and b:
                span_days = max((b - a).total_seconds() / 86400.0, 0.0)
        rows, grain = resample.read_enveloped(conn, k[0], k[1], k[2], native_by_key.get(k),
                                              window=window, span_days=span_days)
        state["_env"][cache_key] = (rows, grain)
        return rows, grain

    def label_of(param):
        return metric_labels.get(param, str(param))

    def data_figure():
        """One chart for the selected parameter(s). Each series = a min/max envelope BAND (level
        mode) + a mean LINE on top. 1 param -> single left y-axis; 2 params -> param 1 left (solid),
        param 2 right (dashed). Colour = location. x-values are ISO strings straight from SQL — never
        pandas Timestamp (banked NiceGUI serialization gotcha)."""
        fig = go.Figure()
        params = state["sel_params"][:MAX_PARAMS]
        show_band = state["mode"] == "level"
        grains: list[str] = []
        drew = False
        for pi, param in enumerate(params):
            yaxis = "y" if pi == 0 else "y2"
            dash = "solid" if pi == 0 else "dash"
            code = pcodes.get(param, "")
            for loc in state["sel_locations"]:
                rows, grain = read_one(param, loc)
                if not rows:
                    continue
                xs = [r["t"] for r in rows]                     # ISO strings (ISC-11)
                means = apply_mode([r["mean"] for r in rows], state["mode"])
                if all(v is None for v in means):
                    continue
                drew = True
                if grain:
                    grains.append(f"{label_of(param)}: {grain} ({len(rows)} pt)")
                col = colors[loc]
                mode = "lines+markers" if len(xs) <= MARKER_MAX_POINTS else "lines"
                if show_band:
                    his = [r["hi"] for r in rows]
                    los = [r["lo"] for r in rows]
                    fig.add_trace(go.Scatter(x=xs, y=his, mode="lines", line=dict(width=0),
                                             yaxis=yaxis, showlegend=False, hoverinfo="skip"))
                    fig.add_trace(go.Scatter(x=xs, y=los, mode="lines", line=dict(width=0),
                                             fill="tonexty", fillcolor=_rgba(col, 0.16),
                                             yaxis=yaxis, showlegend=False, hoverinfo="skip",
                                             name=f"{loc} (min–max)"))
                fig.add_trace(go.Scatter(x=xs, y=means, mode=mode, name=loc, yaxis=yaxis,
                                         line=dict(color=col, width=2, dash=dash),
                                         connectgaps=False, showlegend=False))
                last = next((k for k in range(len(means) - 1, -1, -1) if means[k] is not None), None)
                if last is not None:
                    tag = f"  {loc}" + (f" ({code})" if len(params) > 1 else "")
                    fig.add_annotation(x=xs[last], y=means[last], xanchor="left", showarrow=False,
                                       text=tag, font=dict(color=col, size=11),
                                       yref=("y" if pi == 0 else "y2"))
        ylabel = {"level": (label_of(params[0]) if params else "Waarde"),
                  "change": "Verandering t.o.v. vorige periode (%)",
                  "rebase": "Index (start = 100)"}[state["mode"]]
        y2title = label_of(params[1]) if len(params) > 1 else ""
        title = "  vs  ".join(label_of(p) for p in params) if params else "Geen parameter"
        fig.update_layout(template="plotly_dark", height=470, margin=dict(l=10, r=150, t=46, b=10),
                          title=dict(text=title, font=dict(size=14)), showlegend=False,
                          xaxis=dict(title="Periode (automatisch raster — zoom = resolutie)",
                                     showgrid=False, zeroline=False),
                          yaxis=dict(title=ylabel, showgrid=True, gridcolor=MUTED_GRID, zeroline=False),
                          yaxis2=dict(title=y2title, overlaying="y", side="right",
                                      showgrid=False, zeroline=False, visible=len(params) > 1),
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if state["mode"] == "change":
            fig.add_hline(y=0, line_dash="dot", line_color="#64748b")
        return fig, drew, grains

    def selection_rows():
        rows = []
        for param in state["sel_params"]:
            plabel = label_of(param)
            for loc in state["sel_locations"]:
                env, _grain = read_one(param, loc)
                for r in env:
                    if r["mean"] is None:
                        continue
                    rows.append({"periode": r["t"], "meetpunt": loc, "parameter": plabel,
                                 "gemiddelde": round(r["mean"], 6),
                                 "min": None if r["lo"] is None else round(r["lo"], 6),
                                 "max": None if r["hi"] is None else round(r["hi"], 6)})
                    if len(rows) > TABLE_ROW_CAP:
                        return rows[:TABLE_ROW_CAP], True
        return rows, False

    if check_only:
        fig, drew, grains = data_figure()
        srows, _ = selection_rows()
        _ = [finding_title(f, flabels) for f in findings if f["columns"]]
        # ISC-33 guard: no selected series exceeds the point budget on the full-span window.
        worst = max((len(read_one(p, l)[0]) for p in state["sel_params"]
                     for l in state["sel_locations"]), default=0)
        assert worst <= resample.POINT_BUDGET, f"point budget breached: {worst}"
        print(f"check OK — {len(meta_rows)} series, {len(places)} locations, "
              f"{len(variables)} variables, {len(findings)} findings; "
              f"span {span}d; max points/series {worst} ≤ {resample.POINT_BUDGET}; "
              f"grains {grains or '—'}; table rows {len(srows)}")
        return

    def window_text():
        lo, hi = state["win"]
        if not lo or not hi:
            return "—"
        return f"{str(lo)[:16]} – {str(hi)[:16]}"

    @ui.page("/")
    def page():
        ui.dark_mode().enable()
        ui.add_css("body{background:#0f172a;color:#e2e8f0}")
        with ui.column().classes("w-full max-w-6xl mx-auto p-4 gap-2"):
            ui.label(f"📦 {db_path.stem}").classes("text-2xl font-bold")
            ui.label(f"{len(meta_rows)} reeksen · {len(places)} meetpunten · "
                     f"{len(variables)} parameters · {len(findings)} bevindingen · "
                     f"adaptief raster (envelope, ≤{resample.POINT_BUDGET} pt/reeks)") \
                .classes("text-sm text-slate-400")

            with ui.tabs().classes("w-full") as tabs:
                ui.tab("data", "📈 Data")
                ui.tab("meta", "🗂️ Reeksen (meta)")
                ui.tab("findings", "💡 Bevindingen")
            with ui.tab_panels(tabs, value="data").classes("w-full"):
                with ui.tab_panel("data"):
                    build_data_tab(ui, data_figure, selection_rows, window_text, state,
                                   variables, places, place_params, pcodes, metric_labels,
                                   locs_for, default_locs, iso_at)
                with ui.tab_panel("meta"):
                    build_meta_tab(ui, meta_rows)
                with ui.tab_panel("findings"):
                    build_findings_tab(ui, findings, flabels)

    ui.run(port=port, reload=False, show=False, title=f"Dashboard · {db_path.stem}",
           storage_secret="tell-data-dashboard")


def build_data_tab(ui, data_figure, selection_rows, window_text, state,
                   variables, places, place_params, codes, metric_labels,
                   locs_for, default_locs, iso_at):
    def loc_label(place):
        cs = sorted(codes[v] for v in place_params.get(place, ()))
        return (f"[{'·'.join(cs)}] " if cs else "") + place

    def loc_options(sel_params):
        return {p: loc_label(p) for p in locs_for(sel_params)}

    with ui.column().classes("w-full gap-3"):
        with ui.row().classes("w-full gap-4 items-center flex-wrap"):
            param_sel = ui.select(metric_labels, value=state["sel_params"], multiple=True,
                                  label="Parameters (max 2)").props("use-chips").classes("min-w-72")
            loc_sel = ui.select(loc_options(state["sel_params"]), value=state["sel_locations"],
                                multiple=True, label="Meetpunten (één of meer)") \
                .props("use-chips").classes("min-w-72")

        ui.label("Codes: " + "  ·  ".join(f"{codes[v]} = {metric_labels.get(v, v)}"
                                          for v in variables)).classes("text-xs text-slate-500")

        def refresh_locs():
            opts = loc_options(state["sel_params"])
            loc_sel.options = opts
            state["sel_locations"] = [l for l in state["sel_locations"] if l in opts] \
                or default_locs(state["sel_params"])
            loc_sel.value = state["sel_locations"]
            loc_sel.update()

        def on_params(e):
            vals = list(e.value or [])
            if len(vals) > 2:
                vals = vals[:2]
                param_sel.value = vals
                param_sel.update()
                ui.notify("Maximaal 2 parameters tegelijk (links + rechts y-as).", type="warning")
            state.update(sel_params=vals)
            refresh_locs()
            charts.refresh()
            table.refresh()
        param_sel.on_value_change(on_params)

        def on_locs(e):
            state.update(sel_locations=list(e.value or []))
            charts.refresh()
            table.refresh()
        loc_sel.on_value_change(on_locs)

        with ui.row().classes("w-full gap-4 items-center flex-wrap"):
            ui.toggle({"level": "Niveau", "change": "YoY %", "rebase": "Index=100"}, value=state["mode"],
                      on_change=lambda e: (state.update(mode=e.value), charts.refresh()))

        @ui.refreshable
        def period_row():
            with ui.column().classes("min-w-96 w-full"):
                rlabel = ui.label(f"Venster: {window_text()}").classes("text-sm")
                ui.label("Sleep om in/uit te zoomen — het raster (week/dag/uur/15-min) volgt het "
                         "zichtbare venster automatisch.").classes("text-xs text-slate-500")
                rng = ui.range(min=0, max=WINDOW_STEPS,
                               value={"min": 0, "max": WINDOW_STEPS}).props("label").classes("w-full")
                # initial tooltips (state["win"] is already the full span at startup)
                _lo0, _hi0 = state["win"]
                rng.props(f'left-label-value="{str(_lo0)[:16]}" right-label-value="{str(_hi0)[:16]}"')

                def on_win(_e=None):
                    lo = iso_at(rng.value["min"] / WINDOW_STEPS)
                    hi = iso_at(rng.value["max"] / WINDOW_STEPS)
                    if lo and hi and lo > hi:
                        lo, hi = hi, lo
                    state["win"] = (lo, hi)
                    state["_env"] = {}                    # window changed -> drop the envelope memo
                    rlabel.set_text(f"Venster: {window_text()}")
                    rng.props(f'left-label-value="{str(lo)[:16]}" right-label-value="{str(hi)[:16]}"')
                    rng.update()
                    charts.refresh()
                    table.refresh()
                rng.on("update:model-value", on_win)
        period_row()

        @ui.refreshable
        def charts():
            if not state["sel_locations"] or not state["sel_params"]:
                ui.label("Selecteer ten minste één meetpunt én één parameter.").classes("text-slate-400")
                return
            fig, drew, grains = data_figure()
            if drew:
                ui.plotly(fig).classes("w-full")
                if grains:
                    ui.label("Raster (auto): " + "  ·  ".join(grains)).classes("text-xs text-slate-500")
            else:
                with ui.card().classes("w-full bg-slate-800"):
                    ui.label("Geen data voor deze selectie in dit venster.").classes("text-slate-400 text-sm")
            if state["mode"] == "level":
                ui.label("Lijn = gemiddelde per bin · band = min–max binnen die bin (pieken blijven "
                         "zichtbaar).").classes("text-xs text-slate-500")
            if len(state["sel_params"]) > 1:
                ui.label("Doorgetrokken lijn + linker y-as = parameter 1 · streepjeslijn + rechter "
                         "y-as = parameter 2 · kleur = meetpunt.").classes("text-xs text-slate-500")
        charts()

        ui.separator()
        ui.label("📄 Waarden (huidige selectie)").classes("text-lg font-semibold")

        @ui.refreshable
        def table():
            rows, truncated = selection_rows()
            cols = [
                {"name": "periode", "label": "Periode", "field": "periode", "sortable": True, "align": "left"},
                {"name": "meetpunt", "label": "Meetpunt", "field": "meetpunt", "sortable": True, "align": "left"},
                {"name": "parameter", "label": "Parameter", "field": "parameter", "sortable": True, "align": "left"},
                {"name": "gemiddelde", "label": "Gemiddelde", "field": "gemiddelde", "sortable": True, "align": "right"},
                {"name": "min", "label": "Min", "field": "min", "sortable": True, "align": "right"},
                {"name": "max", "label": "Max", "field": "max", "sortable": True, "align": "right"},
            ]
            if not rows:
                ui.label("Geen waarden in dit venster voor de selectie.").classes("text-slate-400 text-sm")
            else:
                ui.table(columns=cols, rows=rows, pagination=15).classes("w-full bg-slate-800").props("dense")
                if truncated:
                    ui.label(f"⚠️ Tabel beperkt tot {len(rows)} rijen — verfijn de selectie of het venster.") \
                        .classes("text-xs text-amber-400")
        table()

        with ui.expansion("📚 Data & methode").classes("w-full bg-slate-800"):
            ui.markdown(
                "**Read-only.** De `data`/`meta`/`findings`-tabellen worden alleen gelezen.\n\n"
                "**Adaptieve resolutie (zoom = resolutie).** Per zichtbaar venster kiest de dashboard "
                f"automatisch een bin-raster (jaar→15-min) zodat per reeks ≤{resample.POINT_BUDGET} "
                "punten naar de browser gaan, nooit de ruwe punten. Aggregatie gebeurt IN SQL "
                "(`GROUP BY` bin), niet in de browser.\n\n"
                "**Envelope, geen kaal gemiddelde.** Elke bin toont het GEMIDDELDE als lijn én het "
                "MIN–MAX als band, zodat een piek (bv. een afvoercrest) ook ver uitgezoomd zichtbaar "
                "blijft — een gemiddelde alleen zou pieken verbergen.\n\n"
                "**Vloer = native_resolution.** Een dag-reeks wordt nooit sub-dagelijks gebinned; een "
                "subhourly-reeks mag tot 15-min. Bij volledige inzoom (minder ruwe punten dan budget) "
                "worden de ruwe punten ongewijzigd getoond.\n\n"
                "**Meetpunten** zijn gefilterd op de gekozen parameter(s); de code tussen `[ ]` toont "
                "welke parameters dat punt meet.\n\n"
                "**YoY %** = (v − v_vorig)/|v_vorig|×100. **Index** = v/v_start×100 (op het bin-"
                "gemiddelde; de band wordt in die modi niet getoond).")


def build_meta_tab(ui, meta_rows):
    with ui.column().classes("w-full gap-2"):
        ui.label("Reeksen in dit package (meta / herkomst)").classes("text-lg font-semibold")
        ui.label("Eén rij per (bron · meetpunt · parameter). Alle reeksen, ongeacht selectie.") \
            .classes("text-sm text-slate-400")
        cols = [
            {"name": "source", "label": "Bron", "field": "source", "sortable": True, "align": "left"},
            {"name": "place", "label": "Meetpunt", "field": "place", "sortable": True, "align": "left"},
            {"name": "variable", "label": "Parameter", "field": "variable", "sortable": True, "align": "left"},
            {"name": "unit", "label": "Eenheid", "field": "unit", "sortable": True, "align": "left"},
            {"name": "native_resolution", "label": "Resolutie", "field": "native_resolution", "sortable": True, "align": "left"},
            {"name": "row_count", "label": "Rijen", "field": "row_count", "sortable": True, "align": "right"},
            {"name": "lat", "label": "Lat", "field": "lat", "sortable": True, "align": "right"},
            {"name": "lon", "label": "Lon", "field": "lon", "sortable": True, "align": "right"},
        ]
        rows = [{k: m.get(k) for k in ("source", "place", "variable", "unit",
                                       "native_resolution", "row_count", "lat", "lon")}
                for m in meta_rows]
        ui.table(columns=cols, rows=rows, pagination=20).classes("w-full bg-slate-800").props("dense")


def build_findings_tab(ui, findings, flabels):
    with ui.column().classes("w-full gap-2"):
        ui.label("Bevindingen uit de analyse (AnalyseData)").classes("text-lg font-semibold text-emerald-300")
        ui.label("Letterlijk uit de findings-tabel — r, lag, n en caveats verbatim. Niets herberekend.") \
            .classes("text-sm text-slate-400")
        if not findings:
            ui.label("Nog geen bevindingen — draai eerst AnalyseData op dit package.") \
                .classes("text-slate-400 text-sm")
            return
        for f in findings:
            with ui.card().classes("w-full bg-slate-800 border-l-4 border-emerald-400"):
                ui.markdown(f"**{finding_title(f, flabels)}**").classes("text-sm")
                ui.label(finding_subline(f)).classes("text-xs text-slate-400")
                for cav in f["caveats"]:
                    ui.label(f"• {cav}").classes("text-xs text-slate-300")


# ── CLI ──────────────────────────────────────────────────────────────────────
def main(argv=None):
    p = argparse.ArgumentParser(description="TellDataDashboard — browse a SQLite package live.")
    p.add_argument("--db", help="SQLite package path (asks if omitted on a TTY)")
    p.add_argument("--port", type=int, default=8080, help="port to serve on (default 8080)")
    p.add_argument("--period", help="(deprecated, ignored — resolution is now automatic from zoom)")
    p.add_argument("--inspect", action="store_true", help="print detected shape and exit")
    p.add_argument("--check", action="store_true", help="build UI+figures headless, then exit 0")
    args = p.parse_args(argv)

    if args.period:
        print("note: --period is deprecated and ignored — resolution adapts to the zoom window.",
              file=sys.stderr)

    db_path = args.db
    if not db_path:
        if sys.stdin.isatty():
            db_path = input("Path to the SQLite package (.db): ").strip()
        else:
            p.error("--db is required in non-interactive use")
    db_file = Path(db_path).expanduser()
    if not db_file.exists():
        p.error(f"DB not found: {db_file}")

    if args.inspect:
        db = open_db(str(db_file))
        conn = open_conn(str(db_file))
        meta_rows = read_meta_rows(db)
        findings = read_findings(db)
        _, _, span = overall_span(conn)
        sources = sorted({m["source"] for m in meta_rows})
        variables = sorted({m["variable"] for m in meta_rows})
        places = sorted({m["place"] for m in meta_rows})
        total_rows = sum(int(m.get("row_count") or 0) for m in meta_rows)
        print(f"TellDataDashboard v{SKILL_VERSION} — inspect")
        print(f"  package    : {db_file}")
        print(f"  series     : {len(meta_rows)}  ({total_rows} raw rows)")
        print(f"  sources    : {sources}")
        print(f"  parameters : {variables}")
        print(f"  meetpunten : {len(places)} {places if len(places) <= 12 else places[:12] + ['…']}")
        print(f"  span days  : {span}")
        print(f"  resolution : adaptive envelope (viz driver), ≤{resample.POINT_BUDGET} pt/series")
        print(f"  findings   : {len(findings)}")
        print(f"  tabs       : Data | Reeksen (meta) | Bevindingen")
        return 0

    build_dashboard(db_file, port=args.port, check_only=args.check)
    return 0


if __name__ == "__main__":
    sys.exit(main())
