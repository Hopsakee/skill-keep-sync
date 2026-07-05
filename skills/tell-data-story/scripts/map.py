#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["fastlite", "folium", "branca", "numpy"]
# ///
"""TellDataStory — geographic point-map mode (stage 3 sibling of the narrative story).

Renders a SELF-CONTAINED interactive HTML map of per-location findings: one marker per finding,
placed at its location's `meta.lat/lon`, coloured by the finding `statistic` on a diverging scale,
with **significance hatching** and **suspect greying** read straight from the finding's evidence.

Ported from the verified a prior groundwater-trends repo map. Same hard rules as the story renderer:
  - FAITHFUL RENDERER. statistic / p / significant / suspect / caveats are read from the `findings`
    row verbatim; this script computes NO statistic and draws NO surface between points (gaps stay gaps).
  - DOMAIN-AGNOSTIC. Coordinates, labels and units come from `meta`; nothing is hardcoded.
  - Self-contained HTML (opens via file://; tiles from a public CDN). fastlite, reads only.

Default maps `type='trend'` findings (slope per location). Output: <db-stem>_map.html

Run:  uv run map.py --db PATH [--finding-type trend] [--check]
"""
from __future__ import annotations
import argparse, html as _html, json, sys
from pathlib import Path
import numpy as np
import folium
from folium.plugins import GroupedLayerControl  # noqa: F401  (imported lazily-safe; not required)
import branca.colormap as cm
from fastlite import Database

SKILL_VERSION = "0.2.0"
# diverging: low/negative = red (falling), 0 = white, high/positive = blue (rising)
COLORS = ["#b2182b", "#ef8a62", "#fddbc7", "#f7f7f7", "#d1e5f0", "#67a9cf", "#2166ac"]


def open_db(path): return Database(str(Path(path).expanduser()))


def _safe_json(raw, default):
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return default


def meta_coords(db) -> dict:
    """(source:location_id:variable) -> {lat,lon,label,unit}. Coordinates are the join key to findings."""
    out = {}
    for m in db.q("SELECT * FROM meta"):
        key = f"{m['source']}:{m['location_id']}:{m['variable']}"
        out[key] = {"lat": m.get("lat"), "lon": m.get("lon"),
                    "label": m.get("location_label") or m["location_id"], "unit": m.get("unit")}
    return out


def read_point_findings(db, ftype: str, coords: dict) -> list[dict]:
    """Per-location findings of the given type that resolve to a single series WITH coordinates.
    Faithful: every value comes from the row. A finding over 2+ series (e.g. a correlation) or one
    without meta coords is skipped (it has no single map location) — reported, never guessed."""
    out, skipped = [], 0
    for f in db.q("SELECT * FROM findings WHERE type=? ORDER BY finding_id", (ftype,)):
        keys = _safe_json(f["columns"], [])
        if len(keys) != 1:
            skipped += 1; continue
        c = coords.get(keys[0])
        if not c or c["lat"] is None or c["lon"] is None:
            skipped += 1; continue
        ev = _safe_json(f["evidence"], {})
        out.append({"key": keys[0], "label": c["label"], "lat": float(c["lat"]), "lon": float(c["lon"]),
                    "unit": c["unit"], "stat": f["statistic"], "period": f["period"],
                    "p": ev.get("p"), "significant": bool(ev.get("significant", False)),
                    "suspect": bool(ev.get("suspect", False)), "n": ev.get("n"),
                    "caveats": _safe_json(f["caveats"], [])})
    if skipped:
        print(f"[map] {skipped} {ftype} finding(s) skipped (multi-series or no meta lat/lon) — not mapped.")
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description="TellDataStory geographic point-map mode")
    ap.add_argument("--db", required=True)
    ap.add_argument("--finding-type", dest="ftype", default="trend",
                    help="Which per-location finding type to map (default 'trend').")
    ap.add_argument("--out", default=None, help="Output HTML (default <db-stem>_map.html)")
    ap.add_argument("--check", action="store_true", help="Build + report counts; do not require a browser.")
    args = ap.parse_args(argv)

    db = open_db(args.db)
    coords = meta_coords(db)
    pts = read_point_findings(db, args.ftype, coords)
    if not pts:
        print(f"[map] no mappable '{args.ftype}' findings (need single-series findings with meta lat/lon).")
        return 1
    out = Path(args.out) if args.out else Path(args.db).expanduser().with_name(
        Path(args.db).stem + "_map.html")

    # mutually-exclusive render categories (suspect wins over significant)
    sus = sum(1 for p in pts if p["suspect"])
    sig = sum(1 for p in pts if p["significant"] and not p["suspect"])     # solid
    hatched = sum(1 for p in pts if not p["significant"] and not p["suspect"])
    vals = [p["stat"] for p in pts if p["stat"] is not None and not p["suspect"]]
    vmax = max(1e-9, float(np.percentile(np.abs(vals), 95))) if vals else 1.0
    caption = (f"{args.ftype} statistic — red=low/falling, blue=high/rising (±{vmax:.3g} clip)"
               if vals else f"no colour scale — all {len(pts)} points suspect/unscored (grey/hatched)")
    cmap = cm.LinearColormap(COLORS, vmin=-vmax, vmax=vmax, caption=caption)

    lats = [p["lat"] for p in pts]; lons = [p["lon"] for p in pts]
    m = folium.Map(location=[float(np.mean(lats)), float(np.mean(lons))],
                   zoom_start=7, tiles="CartoDB positron", control_scale=True)

    def bucket(v): return int(round((max(-vmax, min(vmax, v)) + vmax) / (2 * vmax) * (len(COLORS) - 1)))

    def fill_for(p):
        if p["suspect"] or p["stat"] is None:
            return "#999999"
        if not p["significant"]:
            return f"url(#hatch{bucket(p['stat'])})"   # hatched = not significant
        return cmap(max(-vmax, min(vmax, p["stat"])))

    for p in pts:
        st = "n.v.t." if p["stat"] is None else f"{p['stat']:+.4g}{(' '+p['unit']) if p['unit'] else ''}"
        ptxt = "" if p["p"] is None else f", p={p['p']:.3g}"
        ntxt = "" if p["n"] is None else f", n={p['n']}"
        status = "verdacht (suspect)" if p["suspect"] else ("significant" if p["significant"] else "niet significant")
        cav_html = "".join(f"<li>{_html.escape(str(c))}</li>" for c in p["caveats"])  # verbatim, escaped
        popup = (f'<div style="font-family:sans-serif;font-size:12px;min-width:240px;max-width:300px">'
                 f'<b>{_html.escape(str(p["label"]))}</b><br>'
                 f'<span style="color:#666">{_html.escape(p["key"])}</span><br>'
                 f'{args.ftype}: <b>{st}</b> <span style="color:#888">({status}{ptxt}{ntxt})</span>'
                 f'<ul style="margin:4px 0 0 14px;padding:0;max-height:140px;overflow:auto;font-size:10px;color:#555">'
                 f'{cav_html}</ul></div>')
        folium.CircleMarker(
            location=[p["lat"], p["lon"]], radius=6, weight=1, color="#333",
            fill=True, fill_color=fill_for(p), fill_opacity=0.9,
            popup=folium.Popup(popup, max_width=320),   # fresh Popup per marker (shared one breaks render)
            tooltip=f'{p["label"]} — {st}{"" if p["significant"] else " (n.s.)"}',
        ).add_to(m)

    cmap.add_to(m)

    # hatch patterns (one per colour bucket) for non-significant markers — url(#hatchN) resolves doc-wide.
    hatch = '<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs>'
    for i, c in enumerate(COLORS):
        hatch += (f'<pattern id="hatch{i}" patternUnits="userSpaceOnUse" width="4" height="4" '
                  f'patternTransform="rotate(45)"><rect width="4" height="4" fill="#ffffff"/>'
                  f'<line x1="0" y1="0" x2="0" y2="4" stroke="{c}" stroke-width="2"/></pattern>')
    hatch += '</defs></svg>'
    m.get_root().html.add_child(folium.Element(hatch))

    # collapsible, mobile-responsive methods panel. NB the media query is spaced as `{ #id` —
    # folium renders injected HTML through Jinja2 and reads a bare `{#` as a comment-open.
    methods = (
        '<style>'
        '#mapnote{position:fixed;bottom:14px;left:14px;z-index:9999;background:rgba(255,255,255,0.96);'
        'border:1px solid #999;border-radius:6px;font-family:sans-serif;font-size:11px;line-height:1.35;'
        'max-width:330px;box-shadow:0 1px 4px rgba(0,0,0,0.25)}'
        '#mapnote>summary{cursor:pointer;padding:7px 10px;font-weight:bold;list-style:none}'
        '#mapnote>summary::-webkit-details-marker{display:none}'
        '#mapnote .b{padding:0 10px 9px}'
        '@media (max-width:600px) { #mapnote{max-width:78vw} }'
        '</style>'
        '<details id="mapnote" open><summary>ℹ️ Kaart — methode</summary><div class="b">'
        f'{len(pts)} locaties · type <b>{_html.escape(args.ftype)}</b>. Kleur = statistiek (rood=laag/dalend, '
        f'blauw=hoog/stijgend). <b>Effen = significant</b>; <b>gearceerd = niet-significant</b> '
        f'({hatched} buizen); <b>grijs = verdacht</b> ({sus}). Waarden komen ongewijzigd uit de '
        f'<code>findings</code>-tabel (deze laag rekent niets na); geen interpolatie tussen punten.'
        '</div></details>'
        '<script>if(window.matchMedia("(max-width:600px)").matches){var _n=document.getElementById("mapnote");'
        'if(_n)_n.removeAttribute("open");}</script>')
    m.get_root().html.add_child(folium.Element(methods))

    m.save(str(out))
    size = out.stat().st_size / 1e6
    print(f"[map] {len(pts)} points ({sig} significant, {hatched} hatched, {sus} suspect); "
          f"vmax={vmax:.3g}; wrote {out} ({size:.2f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
