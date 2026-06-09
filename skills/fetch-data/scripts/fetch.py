# /// script
# requires-python = ">=3.10"
# dependencies = ["hydropandas", "pandas", "numpy", "requests", "rws-ddlpy"]
# ///
"""
FetchData — CLI entrypoint.

Pull raw data from a source into a SQLite package the caller names. One function per source;
the shared core (store.py) does all the writing. Adding a source = add a fetcher + one registry line.

Usage:
  uv run scripts/fetch.py --db PATH --source bro --wells "Zwolle:GMW000000040772:1,Kampen:GMW000000042761:1"
  uv run scripts/fetch.py --db PATH --source bro --wells "..." --from 1995-01-01 --to 2026-06-06 --min-obs 50
  uv run scripts/fetch.py --db PATH --source bro --wells "..." --export-csv out.csv
  uv run scripts/fetch.py --db PATH --source knmi --knmi-vars RH --xy 203000,503000 --from 2024-01-01 --to 2024-03-31
  uv run scripts/fetch.py --db PATH --source rws --rws-stations "Genemuiden:Q;Dalfsen, Vechterweerd:WATHTE" --from 2024-01-01 --to 2024-01-07

--db is optional: name it for scripted use, or omit it and the skill ASKS which DB to create/use
(this skill is on-demand, never headless). Convention: one DB under ~/data/sqlite/ — all hydrology
(BRO + KNMI + RWS) can share one file because the `source` column discriminates the series.
CSV is export-only, never the store.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make scripts/ importable so `store` and `sources.*` resolve regardless of CWD.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import store  # noqa: E402


def _parse_wells(spec: str) -> list[tuple[str, str, int]]:
    """'label:gmw:tube,label:gmw:tube' -> [(label, gmw, tube), ...]"""
    wells = []
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = chunk.split(":")
        if len(parts) != 3:
            raise SystemExit(f"--wells item must be 'label:gmw:tube', got: {chunk!r}")
        label, gmw, tube = parts
        wells.append((label.strip(), gmw.strip(), int(tube)))
    return wells


def _load_wells_file(spec: str) -> list[tuple[str, str, int]]:
    """Load a JSON well-list file -> [(label, gmw, tube), ...], the SAME shape as _parse_wells.
    `spec` is an explicit path (relative or absolute); the caller names the file — the skill does
    no privileged lookup into its own folder. Accepts either a bare list of {label,gmw,tube}
    objects, or an object with a 'wells' key holding that list (the shipped wells-wdodelta.json
    uses the object form for self-description)."""
    path = Path(spec).expanduser()
    if not path.exists():
        raise SystemExit(f"--wells-file {spec!r} not found (the caller names an explicit path)")
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f"--wells-file {path}: invalid JSON ({e})")
    items = doc.get("wells") if isinstance(doc, dict) else doc
    if not isinstance(items, list) or not items:
        raise SystemExit(f"--wells-file {path}: expected a non-empty list of wells "
                         f"(bare list or object with a 'wells' array)")
    wells = []
    for i, it in enumerate(items):
        if not isinstance(it, dict) or not {"label", "gmw", "tube"} <= it.keys():
            raise SystemExit(f"--wells-file {path}: item {i} must have label/gmw/tube, got: {it!r}")
        try:
            tube = int(it["tube"])
        except (TypeError, ValueError):
            raise SystemExit(f"--wells-file {path}: item {i} tube must be an integer, got: {it['tube']!r}")
        wells.append((str(it["label"]).strip(), str(it["gmw"]).strip(), tube))
    return wells


def _parse_xy(spec: str) -> list[tuple[float, float]]:
    """'x,y' (RD/EPSG:28992) -> [(x, y)]. One pair is enough for the meteo slice."""
    parts = [p.strip() for p in spec.split(",")]
    if len(parts) != 2:
        raise SystemExit(f"--xy must be 'x,y' (RD coords), got: {spec!r}")
    return [(float(parts[0]), float(parts[1]))]


def _parse_rws_stations(spec: str) -> list[tuple[str, str]]:
    """'Naam:Grootheid;Naam:Grootheid' -> [(naam, grootheid), ...].
    Semicolon separates stations because an RWS Naam can itself contain a comma
    (e.g. 'Dalfsen, Vechterweerd'). Grootheid is the last colon-separated field."""
    stations = []
    for chunk in spec.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        naam, sep, groot = chunk.rpartition(":")
        if not sep:
            raise SystemExit(f"--rws-stations item must be 'Naam:Grootheid', got: {chunk!r}")
        stations.append((naam.strip(), groot.strip()))
    return stations


def _load_stations_file(spec: str) -> list[tuple[str, str]]:
    """Load a JSON station-list file -> [(naam, grootheid), ...], the SAME shape as _parse_rws_stations.
    `spec` is an explicit path (relative or absolute); the caller names the file — the skill does no
    privileged lookup into its own folder (rung-3 doctrine: every path treated identically). Accepts
    either a bare list of {naam,grootheid} objects, or an object with a 'stations' key holding that
    list (the shipped stations-wdodelta.json uses the object form for self-description). The box stays
    a run-level --rws-box CLI filter, not part of this file — mirrors _load_wells_file (list only)."""
    path = Path(spec).expanduser()
    if not path.exists():
        raise SystemExit(f"--stations-file {spec!r} not found (the caller names an explicit path)")
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f"--stations-file {path}: invalid JSON ({e})")
    items = doc.get("stations") if isinstance(doc, dict) else doc
    if not isinstance(items, list) or not items:
        raise SystemExit(f"--stations-file {path}: expected a non-empty list of stations "
                         f"(bare list or object with a 'stations' array)")
    stations = []
    for i, it in enumerate(items):
        if not isinstance(it, dict) or not {"naam", "grootheid"} <= it.keys():
            raise SystemExit(f"--stations-file {path}: item {i} must have naam/grootheid, got: {it!r}")
        stations.append((str(it["naam"]).strip(), str(it["grootheid"]).strip()))
    return stations


def _resolve_db(spec: str | None) -> str:
    """Resolve the target DB path. The caller MAY name it with --db (scripted/explicit use); when
    omitted, ASK interactively — this skill is on-demand, never headless. Convention: one DB per
    concern under ~/data/sqlite/. A shared folder is safe: SQLite names its -wal/-shm/-journal
    sidecars per DB filename, so differently-named DBs in one dir never collide. store.connect
    expands ~ and creates parent dirs, so a '~/...'-prefixed reply is fine."""
    if spec:
        return spec
    if not sys.stdin.isatty():
        raise SystemExit(
            "--db omitted and no interactive terminal to ask on. Pass --db PATH "
            "(e.g. --db ~/data/sqlite/wdodelta-hydro.db)."
        )
    default = "~/data/sqlite/wdodelta-hydro.db"
    sqlite_dir = Path("~/data/sqlite").expanduser()
    existing = sorted(sqlite_dir.glob("*.db")) if sqlite_dir.is_dir() else []
    if existing:
        print("Existing SQLite databases in ~/data/sqlite/:", flush=True)
        for p in existing:
            print(f"  - {p}", flush=True)
    reply = input(f"Which database should I create or use? [default: {default}] ").strip()
    return reply or default


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch raw data from a source into a SQLite package.")
    ap.add_argument("--db", help="Target SQLite package path. Optional: omit to be asked interactively "
                                 "(convention: ~/data/sqlite/wdodelta-hydro.db; all hydrology in one DB).")
    ap.add_argument("--source", required=True, choices=["bro", "knmi", "rws"], help="Data source.")
    ap.add_argument("--from", dest="start", default="1995-01-01", help="Start date (ISO).")
    ap.add_argument("--to", dest="end", default="2026-06-06", help="End date (ISO).")
    ap.add_argument("--min-obs", type=int, default=50, help="Skip a series with fewer raw obs (BRO).")
    ap.add_argument("--wells", help="BRO: 'label:gmw:tube,...' (explicit triples).")
    ap.add_argument("--wells-file", dest="wells_file",
                    help="BRO: explicit path (relative or absolute) to a JSON well-list, e.g. "
                         "'References/wells-wdodelta.json'. Mutually exclusive with --wells.")
    ap.add_argument("--knmi-vars", dest="knmi_vars",
                    help="KNMI: comma-separated var codes, e.g. 'RH' or 'RH,EV24,TG'.")
    ap.add_argument("--xy", help="KNMI: 'x,y' RD/EPSG:28992 coordinate (read_knmi picks nearest station).")
    ap.add_argument("--rws-stations", dest="rws_stations",
                    help="RWS: 'Naam:Grootheid;Naam:Grootheid' (';' separates; Naam may contain commas).")
    ap.add_argument("--stations-file", dest="stations_file",
                    help="RWS: explicit path (relative or absolute) to a JSON station-list, e.g. "
                         "'References/stations-wdodelta.json'. Mutually exclusive with --rws-stations.")
    ap.add_argument("--rws-box", dest="rws_box",
                    help="RWS optional bounding box 'lat_min,lat_max,lon_min,lon_max' to disambiguate names.")
    ap.add_argument("--export-csv", dest="export_csv", help="Also export the tidy data table to CSV.")
    args = ap.parse_args()
    db_path = _resolve_db(args.db)  # ask up front (before any long fetch) when --db omitted

    if args.source == "bro":
        # Exactly one source of wells: explicit triples (--wells) XOR a JSON file (--wells-file).
        if bool(args.wells) == bool(args.wells_file):
            raise SystemExit("--source bro requires exactly one of --wells 'label:gmw:tube,...' "
                             "or --wells-file PATH|name")
        wells = _parse_wells(args.wells) if args.wells else _load_wells_file(args.wells_file)
        from sources.bro import fetch_bro
        payloads = fetch_bro(wells, args.start, args.end, args.min_obs)
    elif args.source == "knmi":
        if not args.knmi_vars or not args.xy:
            raise SystemExit("--source knmi requires --knmi-vars 'RH[,EV24,...]' and --xy 'x,y'")
        from sources.knmi import fetch_knmi
        codes = [c.strip() for c in args.knmi_vars.split(",") if c.strip()]
        payloads = fetch_knmi(_parse_xy(args.xy), codes, args.start, args.end)
    elif args.source == "rws":
        # Exactly one source of stations: explicit pairs (--rws-stations) XOR a JSON file (--stations-file).
        if bool(args.rws_stations) == bool(args.stations_file):
            raise SystemExit("--source rws requires exactly one of --rws-stations 'Naam:Grootheid;...' "
                             "or --stations-file PATH")
        stations = (_parse_rws_stations(args.rws_stations) if args.rws_stations
                    else _load_stations_file(args.stations_file))
        from sources.rws import fetch_rws
        box = None
        if args.rws_box:
            b = [float(x.strip()) for x in args.rws_box.split(",")]
            if len(b) != 4:
                raise SystemExit("--rws-box must be 'lat_min,lat_max,lon_min,lon_max'")
            box = (b[0], b[1], b[2], b[3])
        payloads = fetch_rws(stations, args.start, args.end, box)
    else:  # pragma: no cover - choices guards this
        raise SystemExit(f"unknown source: {args.source}")

    if not payloads:
        print("FATAL: no series fetched (nothing met min-obs / all lookups failed)", flush=True)
        return 1

    conn = store.connect(db_path)
    n = store.write_payloads(conn, payloads)
    print(f"=== wrote {n} rows across {len(payloads)} series -> {db_path} ===", flush=True)

    if args.export_csv:
        m = store.export_csv(conn, args.export_csv)
        print(f"=== exported {m} rows -> {args.export_csv} (export only; .db is the store) ===", flush=True)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
