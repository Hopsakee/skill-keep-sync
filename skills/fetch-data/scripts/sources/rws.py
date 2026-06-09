"""
RWS surface-water fetcher for FetchData.

Generalises the proven path in a prior hydrology-fetch script (fetch_rws):
  ddlpy.locations()  --select by Naam + Grootheid.Code-->  one station
  ddlpy.measurements(station, start, end)  -->  raw (typically 10-min) observation series.

🔴 hydropandas.read_waterinfo(extent=) is broken in 0.18.1 — we drive ddlpy directly (proven path).

CRITICAL DIFFERENCES from the proving ground (which built a wide DAILY-resampled panel):
  - NO daily resample — FetchData stores RAW, native-resolution (sub-daily) observations. aggregation='raw'.
  - Sentinel: DDL encodes missing as 999999999. We mask |value| >= 1e8 to NULL (kept as rows, value NULL)
    and record sentinel_masked in meta. We do NOT drop the rows — a sentinel timestamp is real signal that
    the station reported "no value", and REAL DATA OR NO DATA means NULL, not a vanished row.
  - Timezone: ddlpy's index is tz-AWARE. We tz_convert("UTC") — NEVER tz_convert(None) (the proving ground
    dropped the offset; we keep it so cross-source joins can't misalign).

Its ONLY job: hit the API, hand back tidy SeriesPayloads. The shared core (store.py) writes them.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# store.py lives one directory up; importable when scripts/ is on sys.path (fetch.py arranges that).
from store import SeriesPayload

SENTINEL = 1e8  # DDL 999999999 missing-value marker; mask |value| >= this to NULL

# Grootheid.Code : (variable, fallback_unit) — the API-reported unit wins; this is only the fallback.
RWS_VARS: dict[str, tuple[str, str]] = {
    "WATHTE": ("waterlevel", "cm"),
    "Q":      ("discharge",  "m3/s"),
}


def _native_resolution(idx: pd.DatetimeIndex) -> str:
    if len(idx) < 3:
        return "unknown"
    med = pd.Series(idx).sort_values().diff().dropna().median()
    hours = med / pd.Timedelta(hours=1)
    if hours < 1.0:
        return "subhourly"
    if hours <= 1.5:
        return "hourly"
    if hours <= 30:
        return "daily-ish"
    return "irregular"


def _unit_from(m: pd.DataFrame, sel_row: pd.Series, fallback: str) -> str:
    """Prefer the API-reported unit, else the fallback map. Tries the measurements frame's
    'Eenheid.Code' column first, then the location row's field."""
    try:
        if "Eenheid.Code" in m.columns:
            col = m["Eenheid.Code"].dropna()
            if len(col) and str(col.iloc[0]).strip():
                return str(col.iloc[0]).strip()
    except Exception:
        pass
    try:
        val = sel_row.get("Eenheid.Code")
        if isinstance(val, str) and val.strip():
            return val.strip()
    except Exception:
        pass
    return fallback


def fetch_rws(
    stations: list[tuple[str, str]],   # [(Naam, Grootheid.Code), ...]; e.g. [("Genemuiden", "Q")]
    start: str,
    end: str,
    box: tuple[float, float, float, float] | None = None,  # (lat_min, lat_max, lon_min, lon_max)
) -> list[SeriesPayload]:
    import ddlpy  # lazy import (rws-ddlpy distribution)

    locs = ddlpy.locations()
    mask = pd.Series(True, index=locs.index)
    if box is not None:
        lat0, lat1, lon0, lon1 = box
        mask = locs.Lat.between(lat0, lat1) & locs.Lon.between(lon0, lon1)

    start_dt = pd.Timestamp(start).to_pydatetime()
    end_dt = pd.Timestamp(end).to_pydatetime()

    payloads: list[SeriesPayload] = []
    for naam, groot in stations:
        variable, fallback_unit = RWS_VARS.get(groot, (groot.lower(), "unknown"))
        # Whole-station guarded: one station's API failure skips THAT station, never aborts the run.
        try:
            sel = locs[mask & (locs.Naam == naam) & (locs["Grootheid.Code"] == groot)]
            if not len(sel):
                print(f"[rws] {naam}/{groot}: not found — skipped", flush=True)
                continue
            sel_row = sel.iloc[0]
            m = ddlpy.measurements(sel_row, start_date=start_dt, end_date=end_dt)
            if not len(m):
                print(f"[rws] {naam}/{groot}: no measurements in range — skipped", flush=True)
                continue

            v = pd.to_numeric(m["Meetwaarde.Waarde_Numeriek"], errors="coerce")
            sentinel_mask = v.abs() >= SENTINEL
            n_sentinel = int(sentinel_mask.sum())
            v = v.mask(sentinel_mask)  # 999999999 -> NaN -> stored NULL (rows kept)

            idx = pd.DatetimeIndex(m.index)
            if idx.tz is None:
                idx = idx.tz_localize("Europe/Amsterdam", nonexistent="shift_forward", ambiguous="NaT")
            idx = idx.tz_convert("UTC")  # keep the UTC offset — never tz_convert(None)

            s = pd.Series(v.to_numpy(), index=idx)
            s = s[s.index.notna()].sort_index()
            s = s[~s.index.duplicated(keep="first")]  # RAW — no resampling

            location_id = str(sel.index[0])  # RWS station code (locs is indexed by code)
            unit = _unit_from(m, sel_row, fallback_unit)
            timestamps = [t.isoformat() for t in s.index.to_pydatetime()]  # -> '...+00:00'
            values = [None if (val is None or (isinstance(val, float) and np.isnan(val))) else float(val)
                      for val in s.to_numpy()]
            payloads.append(SeriesPayload(
                source="rws",
                location_id=location_id,
                variable=variable,
                unit=unit,
                aggregation="raw",
                timestamps=timestamps,
                values=values,
                native_resolution=_native_resolution(s.index),
                endpoint="ddlpy.measurements",
                request_params={"naam": naam, "grootheid": groot, "station_code": location_id,
                                "start": start, "end": end, "timezone": "UTC", "box": box},
                sentinel_masked=(f"|value|>={SENTINEL:.0f} -> NULL (n={n_sentinel})"
                                 if n_sentinel else None),
                location_label=naam,
                lat=float(sel_row.get("Lat")) if "Lat" in sel_row.index else None,
                lon=float(sel_row.get("Lon")) if "Lon" in sel_row.index else None,
            ))
            print(f"[rws] {naam}/{groot} ({location_id}): {len(s)} raw obs "
                  f"({s.index.min()} -> {s.index.max()}), unit={unit}, sentinel_masked={n_sentinel}",
                  flush=True)
        except Exception as e:
            print(f"[rws] {naam}/{groot} FAILED: {type(e).__name__}: {str(e)[:160]} — skipped", flush=True)
            continue
    return payloads
