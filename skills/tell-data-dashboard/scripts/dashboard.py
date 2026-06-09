#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "nicegui>=2.0",
#     "plotly>=5.20",
#     "fastlite",
#     "pandas>=2.0",
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

The package stores RAW, heterogeneous-resolution data; each series is DISPLAY-resampled to a chosen
period by MEAN (surfaced; empty buckets stay missing, connectgaps=False; no fill). Storage is never
touched (read-only). DOMAIN-AGNOSTIC: metric=variable, entity=location, labels/units from `meta`.

Usage:
    uv run --script dashboard.py --db package.db            # serve on :8080
    uv run --script dashboard.py --db package.db --inspect  # print detected shape, exit
    uv run --script dashboard.py --db package.db --check     # build UI+figures headless, exit 0
    uv run --script dashboard.py --db package.db --port 8099 --period W
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
from fastlite import Database

SKILL_VERSION = "0.3.0"

PALETTE = [
    "#38bdf8", "#f97316", "#a78bfa", "#34d399", "#f472b6", "#facc15",
    "#60a5fa", "#fb7185", "#4ade80", "#c084fc", "#fbbf24", "#22d3ee",
    "#e879f9", "#2dd4bf", "#fdba74", "#93c5fd", "#86efac", "#f0abfc",
]
MUTED_GRID = "rgba(148,163,184,0.12)"
MARKER_MAX_POINTS = 40
TABLE_ROW_CAP = 5000
MAX_PARAMS = 2                                   # dual y-axis ceiling

PERIOD_RULES = {"D": "D", "W": "W", "MS": "MS"}
PERIOD_LABELS = {"D": "Dag", "W": "Week", "MS": "Maand"}


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


# ── package read (fastlite, read-only by behaviour) ──────────────────────────
def open_db(path: str) -> Database:
    return Database(str(Path(path).expanduser()))


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


def read_series_raw(db: Database, source: str, location_id: str, variable: str):
    rows = db.q(
        "SELECT timestamp, value FROM data "
        "WHERE source=? AND location_id=? AND variable=? AND value IS NOT NULL "
        "ORDER BY timestamp",
        (source, location_id, variable),
    )
    return [r["timestamp"] for r in rows], [r["value"] for r in rows]


def overall_span_days(db: Database) -> int:
    row = list(db.q("SELECT MIN(timestamp) AS lo, MAX(timestamp) AS hi FROM data"))
    if not row or not row[0].get("lo") or not row[0].get("hi"):
        return 0
    lo = pd.to_datetime(row[0]["lo"], errors="coerce", utc=True)
    hi = pd.to_datetime(row[0]["hi"], errors="coerce", utc=True)
    if pd.isna(lo) or pd.isna(hi):
        return 0
    return int((hi - lo).days)


def auto_period(span_days: int) -> str:
    if span_days <= 366 * 2:
        return "D"
    if span_days <= 366 * 13:
        return "W"
    return "MS"


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


# ── transforms ───────────────────────────────────────────────────────────────
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


# ── adapter: package -> shared display grid ──────────────────────────────────
def resample_one(ts_iso: list[str], vals: list[float], rule: str) -> "pd.Series":
    if not ts_iso:
        return pd.Series(dtype="float64")
    idx = pd.to_datetime(ts_iso, errors="coerce", utc=True)
    s = pd.Series(vals, index=idx)
    s = s[~s.index.isna()]
    if s.empty:
        return pd.Series(dtype="float64")
    return s.resample(rule).mean()


def build_panel(raw_by_key: dict, meta_rows: list[dict], rule: str):
    resampled = {}
    period_set = set()
    for m in meta_rows:
        key = (m["source"], m["location_id"], m["variable"])
        ts, vals = raw_by_key.get(key, ([], []))
        rr = resample_one(ts, vals, rule)
        if rr.empty:
            continue
        resampled[key] = rr
        period_set.update(rr.index.tolist())

    ts_sorted = sorted(period_set)
    times = [t.strftime("%Y-%m-%d") for t in ts_sorted]
    tindex = {t: i for i, t in enumerate(ts_sorted)}

    variables = sorted({m["variable"] for m in meta_rows})
    places = sorted({m["place"] for m in meta_rows})
    metric_labels, units = {}, {}
    for m in meta_rows:
        units.setdefault(m["variable"], m.get("unit") or "")
        unit = units[m["variable"]]
        metric_labels[m["variable"]] = f"{m['label_var']}" + (f" ({unit})" if unit else "")

    series = {v: {p: [None] * len(times) for p in places} for v in variables}
    place_by_key = {(m["source"], m["location_id"], m["variable"]): m["place"] for m in meta_rows}
    for key, rr in resampled.items():
        var, place = key[2], place_by_key.get(key)
        if place is None:
            continue
        for t, val in rr.items():
            i = tindex.get(t)
            if i is not None:
                series[var][place][i] = None if pd.isna(val) else float(val)

    return times, places, variables, series, metric_labels, units


# ── presentation layer (NiceGUI, one tab per table) ──────────────────────────
def build_dashboard(db_path: Path, port: int, period_override: str | None = None,
                    check_only: bool = False):
    import plotly.graph_objects as go
    from nicegui import ui

    db = open_db(str(db_path))
    meta_rows = read_meta_rows(db)
    if not meta_rows:
        raise ValueError(f"no `meta` rows in {db_path.name} — is this a pipeline package?")

    raw_by_key = {}
    for m in meta_rows:
        key = (m["source"], m["location_id"], m["variable"])
        raw_by_key[key] = read_series_raw(db, *key)

    findings = read_findings(db)
    flabels = findings_label_map(meta_rows)
    span = overall_span_days(db)
    init_period = period_override if period_override in PERIOD_RULES else auto_period(span)

    # which parameters each place measures (period-independent — from meta)
    place_params: dict[str, set] = {}
    for m in meta_rows:
        place_params.setdefault(m["place"], set()).add(m["variable"])

    state = {"period": init_period}

    def rebuild():
        times, places, variables, series, mlabels, units = build_panel(
            raw_by_key, meta_rows, PERIOD_RULES[state["period"]])
        keep_params = [v for v in state.get("sel_params", []) if v in variables][:MAX_PARAMS]

        def covered(param):
            col = series.get(param, {})
            return [p for p in places if any(v is not None for v in col.get(p, []))]
        default_param = max(variables, key=lambda v: len(covered(v))) if variables else None
        sel_params = keep_params or ([default_param] if default_param else [])

        # locations restricted to those measuring a selected parameter
        valid = [p for p in places if place_params.get(p, set()) & set(sel_params)] if sel_params else places
        keep_locs = [p for p in state.get("sel_locations", []) if p in valid]
        sel_locs = keep_locs or valid[: min(4, len(valid))]

        state.update({
            "times": times, "places": places, "variables": variables, "series": series,
            "metric_labels": mlabels, "units": units,
            "colors": {p: PALETTE[i % len(PALETTE)] for i, p in enumerate(places)},
            "param_codes": param_codes(variables),
            "place_params": place_params,
            "sel_params": sel_params,
            "sel_locations": sel_locs,
            "mode": state.get("mode", "level"),
            "range": (0, max(0, len(times) - 1)),
        })

    rebuild()

    def label_of(param):
        return state["metric_labels"].get(param, str(param))

    def data_figure():
        """One chart for the selected parameter(s). 1 param -> single left y-axis. 2 params ->
        param 1 on the left y-axis (solid), param 2 on the right y-axis (dashed). Colour = location
        (consistent); conditional markers; direct end-labels; connectgaps=False; units on the axes."""
        fig = go.Figure()
        times, (lo, hi) = state["times"], state["range"]
        xs = times[lo:hi + 1]
        params = state["sel_params"][:MAX_PARAMS]
        mode = "lines+markers" if len(xs) <= MARKER_MAX_POINTS else "lines"
        drew = False
        for pi, param in enumerate(params):
            yaxis = "y" if pi == 0 else "y2"
            dash = "solid" if pi == 0 else "dash"
            code = state["param_codes"].get(param, "")
            for loc in state["sel_locations"]:
                if loc not in state["series"].get(param, {}):
                    continue
                vals = apply_mode(state["series"][param][loc], state["mode"])[lo:hi + 1]
                if all(v is None for v in vals):
                    continue
                drew = True
                col = state["colors"][loc]
                fig.add_trace(go.Scatter(x=xs, y=vals, mode=mode, name=loc, yaxis=yaxis,
                                         line=dict(color=col, width=2, dash=dash),
                                         connectgaps=False, showlegend=False))
                last = next((k for k in range(len(vals) - 1, -1, -1) if vals[k] is not None), None)
                if last is not None:
                    tag = f"  {loc}" + (f" ({code})" if len(params) > 1 else "")
                    fig.add_annotation(x=xs[last], y=vals[last], xanchor="left", showarrow=False,
                                       text=tag, font=dict(color=col, size=11),
                                       yref=("y" if pi == 0 else "y2"))
        ylabel = {"level": (label_of(params[0]) if params else "Waarde"),
                  "change": "Verandering t.o.v. vorige periode (%)",
                  "rebase": "Index (start = 100)"}[state["mode"]]
        y2title = label_of(params[1]) if len(params) > 1 else ""
        title = "  vs  ".join(label_of(p) for p in params) if params else "Geen parameter"
        fig.update_layout(template="plotly_dark", height=470, margin=dict(l=10, r=150, t=46, b=10),
                          title=dict(text=title, font=dict(size=14)), showlegend=False,
                          xaxis=dict(title=f"Periode ({PERIOD_LABELS[state['period']]})",
                                     showgrid=False, zeroline=False),
                          yaxis=dict(title=ylabel, showgrid=True, gridcolor=MUTED_GRID, zeroline=False),
                          yaxis2=dict(title=y2title, overlaying="y", side="right",
                                      showgrid=False, zeroline=False, visible=len(params) > 1),
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if state["mode"] == "change":
            fig.add_hline(y=0, line_dash="dot", line_color="#64748b")
        return fig, drew

    def selection_rows():
        rows, (lo, hi) = [], state["range"]
        for param in state["sel_params"]:
            for loc in state["sel_locations"]:
                col = state["series"].get(param, {}).get(loc)
                if not col:
                    continue
                for i in range(lo, hi + 1):
                    v = col[i]
                    if v is None:
                        continue
                    rows.append({"periode": state["times"][i], "meetpunt": loc,
                                 "parameter": label_of(param), "waarde": round(v, 6)})
                    if len(rows) > TABLE_ROW_CAP:
                        return rows[:TABLE_ROW_CAP], True
        return rows, False

    if check_only:
        _ = data_figure()
        _ = selection_rows()
        _ = [finding_title(f, flabels) for f in findings if f["columns"]]
        print(f"check OK — {len(meta_rows)} series, {len(state['places'])} locations × "
              f"{len(state['times'])} periods ({PERIOD_LABELS[state['period']]}), "
              f"{len(state['variables'])} variables, {len(findings)} findings")
        return

    def window_text():
        times, (lo, hi) = state["times"], state["range"]
        return f"{times[lo]} – {times[hi]}" if times else "—"

    @ui.page("/")
    def page():
        ui.dark_mode().enable()
        ui.add_css("body{background:#0f172a;color:#e2e8f0}")
        with ui.column().classes("w-full max-w-6xl mx-auto p-4 gap-2"):
            ui.label(f"📦 {db_path.stem}").classes("text-2xl font-bold")
            ui.label(f"{len(meta_rows)} reeksen · {len(state['places'])} meetpunten · "
                     f"{len(state['variables'])} parameters · {len(findings)} bevindingen · "
                     f"raster {PERIOD_LABELS[state['period']]} (display-resample, gemiddelde)") \
                .classes("text-sm text-slate-400")

            with ui.tabs().classes("w-full") as tabs:
                ui.tab("data", "📈 Data")
                ui.tab("meta", "🗂️ Reeksen (meta)")
                ui.tab("findings", "💡 Bevindingen")
            with ui.tab_panels(tabs, value="data").classes("w-full"):
                with ui.tab_panel("data"):
                    build_data_tab(ui, data_figure, selection_rows, window_text, state, rebuild)
                with ui.tab_panel("meta"):
                    build_meta_tab(ui, meta_rows)
                with ui.tab_panel("findings"):
                    build_findings_tab(ui, findings, flabels)

    ui.run(port=port, reload=False, show=False, title=f"Dashboard · {db_path.stem}",
           storage_secret="tell-data-dashboard")


def build_data_tab(ui, data_figure, selection_rows, window_text, state, rebuild):
    codes = state["param_codes"]
    place_params = state["place_params"]

    def loc_label(place):
        cs = sorted(codes[v] for v in place_params.get(place, ()))
        return (f"[{'·'.join(cs)}] " if cs else "") + place

    def loc_options(sel_params):
        ps = set(sel_params or [])
        locs = [p for p in state["places"] if (place_params.get(p, set()) & ps)] if ps else list(state["places"])
        return {p: loc_label(p) for p in locs}

    with ui.column().classes("w-full gap-3"):
        with ui.row().classes("w-full gap-4 items-center flex-wrap"):
            param_sel = ui.select(state["metric_labels"], value=state["sel_params"], multiple=True,
                                  label="Parameters (max 2)").props("use-chips").classes("min-w-72")
            loc_sel = ui.select(loc_options(state["sel_params"]), value=state["sel_locations"],
                                multiple=True, label="Meetpunten (één of meer)") \
                .props("use-chips").classes("min-w-72")

        # legend mapping the per-point codes to full parameter names
        ui.label("Codes: " + "  ·  ".join(f"{codes[v]} = {state['metric_labels'].get(v, v)}"
                                          for v in state["variables"])).classes("text-xs text-slate-500")

        def refresh_locs():
            opts = loc_options(state["sel_params"])
            loc_sel.options = opts
            state["sel_locations"] = [l for l in state["sel_locations"] if l in opts] or list(opts)[:4]
            loc_sel.value = state["sel_locations"]
            loc_sel.update()

        def on_params(e):
            vals = list(e.value or [])
            if len(vals) > 2:                       # enforce the dual-axis ceiling
                vals = vals[:2]
                param_sel.value = vals
                param_sel.update()
                ui.notify("Maximaal 2 parameters tegelijk (links + rechts y-as).", type="warning")
            state.update(sel_params=vals)
            refresh_locs()                          # only points that measure a selected parameter
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
            ui.select(PERIOD_LABELS, value=state["period"], label="Tijdraster",
                      on_change=lambda e: (state.update(period=e.value), rebuild(), refresh_locs(),
                                           period_row.refresh(), charts.refresh(), table.refresh())) \
                .classes("min-w-32")

        @ui.refreshable
        def period_row():
            with ui.column().classes("min-w-80"):
                rlabel = ui.label(f"Periode: {window_text()}")
                n = max(0, len(state["times"]) - 1)
                lo0, hi0 = state["range"]
                # `label` (not label-always): the date tooltips show only on hover/drag of a handle.
                rng = ui.range(min=0, max=n, value={"min": lo0, "max": hi0}).props("label")

                def set_tips():
                    t = state["times"]
                    lo, hi = state["range"]
                    if t:
                        rng.props(f'left-label-value="{t[lo]}" right-label-value="{t[hi]}"')
                        rng.update()
                set_tips()
                rng.on("update:model-value", lambda e: (
                    state.update(range=(int(rng.value["min"]), int(rng.value["max"]))),
                    rlabel.set_text(f"Periode: {window_text()}"),
                    set_tips(), charts.refresh(), table.refresh()))
        period_row()

        @ui.refreshable
        def charts():
            if not state["sel_locations"] or not state["sel_params"]:
                ui.label("Selecteer ten minste één meetpunt én één parameter.").classes("text-slate-400")
                return
            fig, drew = data_figure()
            if drew:
                ui.plotly(fig).classes("w-full")
            else:
                with ui.card().classes("w-full bg-slate-800"):
                    ui.label("Geen data voor deze selectie in dit venster.").classes("text-slate-400 text-sm")
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
                {"name": "waarde", "label": "Waarde", "field": "waarde", "sortable": True, "align": "right"},
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
                "**Meetpunten** zijn gefilterd op de gekozen parameter(s); de code tussen `[ ]` vóór "
                "elke naam toont welke parameters dat punt meet (zie de legenda).\n\n"
                "**Twee parameters:** parameter 1 staat op de linker y-as (doorgetrokken), parameter 2 "
                "op de rechter y-as (streepjes) — zo zijn verschillende eenheden te vergelijken. Max 2.\n\n"
                "**Tijdraster (resample).** Ongelijke resoluties worden display-geresampled met het "
                "GEMIDDELDE per bin (instelbaar). Alleen weergave; opslag verandert niet. Stroom-totalen "
                "(neerslag/afvoer) worden dus als bin-gemiddelde getoond, niet als som.\n\n"
                "**Lege bins blijven leeg** (`connectgaps=False`): geen opvulling/interpolatie, de lijn "
                "overbrugt gaten niet. Een bin met 1 of met 200 metingen toont als één punt.\n\n"
                "**YoY %** = (v − v_vorig)/|v_vorig|×100. **Index** = v/v_start×100.")


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
    p.add_argument("--period", choices=list(PERIOD_RULES), help="force display grid: D / W / MS")
    p.add_argument("--inspect", action="store_true", help="print detected shape and exit")
    p.add_argument("--check", action="store_true", help="build UI+figures headless, then exit 0")
    args = p.parse_args(argv)

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
        meta_rows = read_meta_rows(db)
        findings = read_findings(db)
        span = overall_span_days(db)
        period = args.period if args.period in PERIOD_RULES else auto_period(span)
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
        print(f"  period     : {period} ({PERIOD_LABELS[period]})  [auto unless --period]")
        print(f"  findings   : {len(findings)}")
        print(f"  tabs       : Data | Reeksen (meta) | Bevindingen")
        return 0

    build_dashboard(db_file, port=args.port, period_override=args.period, check_only=args.check)
    return 0


if __name__ == "__main__":
    sys.exit(main())
