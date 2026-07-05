#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["hydropandas", "pandas", "numpy", "requests"]
# ///
"""FetchData — BRO DISCOVERY mode.

The base skill FETCHES named series; this FINDS them. Given an area + a record-completeness rule +
a target count, it discovers BRO groundwater wells and writes a **wells-file** (`{label,gmw,tube,...}`)
that the normal fetch consumes verbatim:

    uv run discover_bro.py --box 52.40,52.85,5.80,6.75 --from 2001-04-01 --to 2026-03-31 --n 20 --out wells.json
    uv run fetch.py --source bro --wells-file wells.json   # then fetch the discovered series

Ported from the verified a prior groundwater-trends repo discovery. Key techniques folded in:
  - **Dispersed (farthest-point) cell order** so stopping at the target spreads evenly, never
    clustering in the first rows scanned.
  - **Cheap span pre-filter** (GLD observationsSummary, KBs) before the MB-scale seriesAsCsv.
  - **Per-key caching** (boxes / gldids / summaries / series) so a re-run or rate-limit resumes free.
  - **Dedup** against an existing wells-file (`--exclude`) so you can densify an area.

Idempotent + checkpointed (writes the wells-file after every kept well). REAL DATA OR NO DATA.
"""
from __future__ import annotations
import argparse, io, json, os, pickle, sys, time
os.environ.setdefault("TQDM_DISABLE", "1")
from pathlib import Path
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import requests

GMW_SEARCH = "https://publiek.broservices.nl/gm/gmw/v1/characteristics/searches"
GLD_SERIES_CSV = "https://publiek.broservices.nl/gm/gld/v1/seriesAsCsv/{gld}"
GLD_OBS_SUMMARY = "https://publiek.broservices.nl/gm/gld/v1/objects/{gld}/observationsSummary"
VALUE_COLS = ("Beoordeelde Waarde [m]", "Voorlopige Waarde [m]", "Onbekend Waarde [m]", "Controle Waarde [m]")


def local(tag): return tag.split("}", 1)[-1]


def dispersed_order(cells):
    """Farthest-point sampling: first K of this order cover the region evenly (no south-bias on early stop)."""
    n = len(cells)
    if n <= 2:
        return list(cells)
    cx = sum(c[0] for c in cells) / n; cy = sum(c[1] for c in cells) / n
    start = min(range(n), key=lambda i: (cells[i][0] - cx) ** 2 + (cells[i][1] - cy) ** 2)
    order = [start]; remaining = set(range(n)) - {start}
    mind = {i: (cells[i][0] - cells[start][0]) ** 2 + (cells[i][1] - cells[start][1]) ** 2 for i in remaining}
    while remaining:
        nxt = max(remaining, key=lambda i: mind[i]); order.append(nxt); remaining.discard(nxt)
        for i in remaining:
            d = (cells[i][0] - cells[nxt][0]) ** 2 + (cells[i][1] - cells[nxt][1]) ** 2
            if d < mind[i]:
                mind[i] = d
    return [cells[i] for i in order]


class Disco:
    def __init__(self, cache_dir, args):
        self.c = Path(cache_dir).expanduser()
        for sub in ("boxes", "gldids", "summary", "series"):
            (self.c / sub).mkdir(parents=True, exist_ok=True)
        self.a = args

    def gmw_search(self, lat, lon):
        cp = self.c / "boxes" / f"box2_{lat:.3f}_{lon:.3f}.pkl"
        if cp.exists():
            try: return pickle.loads(cp.read_bytes())
            except Exception: pass
        body = {"area": {"boundingBox": {
            "lowerCorner": {"lat": lat - self.a.box_half, "lon": lon - self.a.box_half_lon},
            "upperCorner": {"lat": lat + self.a.box_half, "lon": lon + self.a.box_half_lon}}}}
        r = requests.post(GMW_SEARCH + "?requestReference=fetchdisco", json=body, timeout=120)
        r.raise_for_status()
        root = ET.fromstring(r.content); wells = []
        for gmwc in root.iter():
            if local(gmwc.tag) != "GMW_C":
                continue
            broid = wlat = wlon = nitg = None; ntubes = 1
            for el in gmwc.iter():
                t = local(el.tag)
                if t == "broId" and broid is None: broid = (el.text or "").strip()
                elif t == "nitgCode" and nitg is None: nitg = (el.text or "").strip() or None
                elif t == "standardizedLocation":
                    for pos in el.iter():
                        if local(pos.tag) == "pos" and pos.text:
                            pp = pos.text.split()
                            if len(pp) == 2: wlat, wlon = float(pp[0]), float(pp[1])
                elif t == "numberOfMonitoringTubes":
                    try: ntubes = int((el.text or "1").strip())
                    except Exception: ntubes = 1
            if broid and wlat is not None:
                wells.append((broid, wlat, wlon, ntubes, nitg))
        cp.write_bytes(pickle.dumps(wells)); return wells

    def gld_ids(self, gmw, tube):
        cp = self.c / "gldids" / f"{gmw}_{tube}.json"
        if cp.exists():
            try: return json.loads(cp.read_text())
            except Exception: pass
        from hydropandas.io.bro import get_gld_ids_from_gmw
        ids = list(get_gld_ids_from_gmw(gmw, tube) or [])
        cp.write_text(json.dumps(ids)); return ids

    def span_ok(self, gld):
        cp = self.c / "summary" / f"{gld}.json"
        if cp.exists():
            try: summ = json.loads(cp.read_text())
            except Exception: summ = None
        else:
            summ = None
        if summ is None:
            r = requests.get(GLD_OBS_SUMMARY.format(gld=gld), timeout=60); r.raise_for_status()
            summ = r.json(); cp.write_text(json.dumps(summ))
        if not summ:
            return False
        starts, ends = [], []
        for o in summ:
            s = pd.to_datetime(o.get("startDate"), format="%d-%m-%Y", errors="coerce")
            e = pd.to_datetime(o.get("endDate"), format="%d-%m-%Y", errors="coerce")
            if pd.notna(s): starts.append(s)
            if pd.notna(e): ends.append(e)
        return bool(starts and ends and min(starts) <= self.a.first_by and max(ends) >= self.a.last_after)

    def gld_series(self, gld):
        cp = self.c / "series" / f"{gld}.pkl"
        if cp.exists():
            try: return pickle.loads(cp.read_bytes())
            except Exception: pass
        r = requests.get(GLD_SERIES_CSV.format(gld=gld), timeout=180); r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        ts = pd.to_datetime(df["Tijdstip"], unit="ms", errors="coerce", utc=True).dt.tz_convert(None)
        val = None
        for col in VALUE_COLS:
            if col in df.columns:
                c = pd.to_numeric(df[col], errors="coerce"); val = c if val is None else val.fillna(c)
        s = pd.Series(dtype="float64") if val is None else pd.Series(val.to_numpy(), index=ts).dropna().sort_index()
        if len(s):
            s = s[s.index.notna()]; s = s[~s.index.duplicated(keep="first")]
        cp.write_bytes(pickle.dumps(s)); return s

    def completeness(self, s):
        if s.empty: return False, 0
        w = s.loc[self.a.frm:self.a.to]
        if w.empty: return False, 0
        hy = np.where(w.index.month >= 4, w.index.year, w.index.year - 1)
        counts = pd.Series(1, index=hy).groupby(level=0).sum()
        y0, y1 = self.a.frm.year, self.a.to.year - 1
        counts = counts[(counts.index >= y0) & (counts.index <= y1)]
        covered = int((counts >= self.a.min_obs_year).sum())
        ok = (covered >= self.a.min_years and w.index.min() <= self.a.first_by and w.index.max() >= self.a.last_after)
        return ok, covered


def main(argv=None):
    ap = argparse.ArgumentParser(description="FetchData BRO discovery -> wells-file")
    ap.add_argument("--box", required=True, help="lat0,lat1,lon0,lon1 (WGS84)")
    ap.add_argument("--from", dest="frm", required=True, help="period start ISO (e.g. 2001-04-01)")
    ap.add_argument("--to", dest="to", required=True, help="period end ISO (e.g. 2026-03-31)")
    ap.add_argument("--n", type=int, default=20, help="target number of wells")
    ap.add_argument("--out", required=True, help="output wells-file JSON path")
    ap.add_argument("--exclude", default=None, help="existing wells-file to dedup against")
    ap.add_argument("--min-years", dest="min_years", type=int, default=23)
    ap.add_argument("--min-obs-year", dest="min_obs_year", type=int, default=6)
    ap.add_argument("--candidates-per-cell", dest="cands", type=int, default=40)
    ap.add_argument("--max-per-cell", dest="max_per_cell", type=int, default=2)
    ap.add_argument("--dlat", type=float, default=0.09)
    ap.add_argument("--dlon", type=float, default=0.13)
    ap.add_argument("--cache-dir", dest="cache_dir", default="~/.cache/FetchData_discover")
    ap.add_argument("--sleep", type=float, default=0.3)
    args = ap.parse_args(argv)

    la0, la1, lo0, lo1 = (float(x) for x in args.box.split(","))
    args.frm = pd.Timestamp(args.frm); args.to = pd.Timestamp(args.to)
    args.first_by = args.frm + pd.Timedelta(days=550)     # must start near the beginning
    args.last_after = args.to - pd.Timedelta(days=550)     # must run to near the end
    args.box_half = max(0.05, args.dlat * 0.6); args.box_half_lon = max(0.07, args.dlon * 0.6)
    d = Disco(args.cache_dir, args)

    def load_wells_file(p):
        """Read a wells-file once; accept a bare list or a {'wells': [...]} object (robust to BOM/
        whitespace via isinstance, not a first-byte guess). Returns a list of well dicts."""
        try:
            doc = json.loads(Path(p).expanduser().read_text())
        except Exception:
            return []
        return doc.get("wells", []) if isinstance(doc, dict) else (doc if isinstance(doc, list) else [])

    out = Path(args.out).expanduser()
    wells = load_wells_file(out) if out.exists() else []
    have = {w["gmw"] for w in wells if w.get("gmw")}
    if args.exclude:                                    # dedup vs an existing list; tolerate items lacking gmw
        for it in load_wells_file(args.exclude):
            if it.get("gmw"):
                have.add(it["gmw"])

    cells = []
    la = la0
    while la <= la1:
        lo = lo0
        while lo <= lo1:
            cells.append((round(la, 3), round(lo, 3))); lo += args.dlon
        la += args.dlat
    cells = dispersed_order(cells)
    print(f"[disco] {len(cells)} cells (dispersed); {len(have)} excluded; target +{args.n}", flush=True)

    def try_one(clat, clon):
        try:
            cands = d.gmw_search(clat, clon)
        except Exception as e:
            print(f"[disco {clat},{clon}] search FAIL {type(e).__name__} {str(e)[:70]}"); return False
        cands = [c for c in cands if c[0] not in have]
        cands.sort(key=lambda c: (c[1] - clat) ** 2 + (c[2] - clon) ** 2)
        for (gmw, wlat, wlon, nt, nitg) in cands[:args.cands]:
            for tube in range(1, min(nt, 4) + 1):
                try:
                    time.sleep(args.sleep)
                    glds = d.gld_ids(gmw, tube)
                    if not glds: continue
                    gld = glds[0]
                    if not d.span_ok(gld): continue
                    ok, cov = d.completeness(d.gld_series(gld))
                    if ok:
                        wells.append({"label": f"{gmw}-{tube}", "gmw": gmw, "tube": tube, "gld": gld,
                                      "nitg": nitg, "lat": round(wlat, 6), "lon": round(wlon, 6), "n_years": cov})
                        have.add(gmw)
                        out.write_text(json.dumps({"wells": wells}, indent=2))
                        print(f"[disco {clat},{clon}] KEPT {nitg or gmw} cov={cov} (+{len(wells)})", flush=True)
                        return True
                except Exception as e:
                    print(f"[disco] {gmw}_{tube} err {type(e).__name__} {str(e)[:60]}"); continue
        return False

    target_total = len(wells) + args.n
    for _pass in range(args.max_per_cell):
        if len(wells) >= target_total: break
        for (clat, clon) in cells:
            if len(wells) >= target_total: break
            try_one(clat, clon)
    out.write_text(json.dumps({"wells": wells}, indent=2))
    print(f"=== discovered {len(wells)} wells -> {out} (feed: fetch.py --source bro --wells-file {out}) ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
