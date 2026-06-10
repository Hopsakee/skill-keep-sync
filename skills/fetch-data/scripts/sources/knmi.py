"""
KNMI meteo fetcher for FetchData.

Generalises the proven path in a prior hydrology-fetch script (read_knmi):
  xy coordinate  --(hydropandas read_knmi)-->  ObsCollection  -->  one daily meteo series per var.

KNMI's daily product already returns DAILY aggregates, so native_resolution='daily' and the
series is stored as-is (aggregation records whether the daily value is a mean or a sum per var).

CRITICAL DIFFERENCES from the proving ground (which built a wide weekly panel):
  - NO weekly resample, NO merge — FetchData stores the raw daily series (the raw-store rule).
  - UNIT GOTCHA: hydropandas returns KNMI precip (RH) and Makkink evaporation (EV24) in METRES.
    We multiply ×1000 and store the honest unit 'mm'. Storing metres labelled 'mm' bit us before.
  - Timestamps: KNMI daily values are by local (Europe/Amsterdam) day. We localise the daily date to
    Amsterdam midnight, convert to UTC, and record timezone in request_params — so cross-source joins
    (BRO/RWS) cannot silently misalign by an offset/DST hour.

Its ONLY job: hit the API, hand back tidy SeriesPayloads. The shared core (store.py) writes them.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# store.py lives one directory up; importable when scripts/ is on sys.path (fetch.py arranges that).
from store import SeriesPayload

# code : (variable, unit, aggregation, scale_to_store)
# scale_to_store multiplies the raw hydropandas value before storage. RH/EV24 arrive in METRES → ×1000=mm.
KNMI_VARS: dict[str, tuple[str, str, str, float]] = {
    "TG":   ("temp_mean",           "degC", "daily_mean", 1.0),
    "SQ":   ("sun_hours",           "h",    "daily_sum",  1.0),
    "RH":   ("precipitation",       "mm",   "daily_sum",  1000.0),
    "EV24": ("evaporation_makkink", "mm",   "daily_sum",  1000.0),
    "FG":   ("wind_speed",          "m/s",  "daily_mean", 1.0),
}
_TZ = "Europe/Amsterdam"


def _obs_series(obs) -> pd.Series:
    """Primary numeric value column of a hydropandas Obs, as a clean datetime-indexed Series."""
    df = pd.DataFrame(obs)
    if len(df) == 0:
        return pd.Series(dtype="float64")
    df = df[~df.index.duplicated(keep="first")]
    if "values" in df.columns and pd.api.types.is_numeric_dtype(df["values"]):
        s = df["values"]
    else:
        num = df.select_dtypes(include=[np.number])
        if num.shape[1] == 0:
            return pd.Series(dtype="float64")
        s = num.iloc[:, 0]
    s = pd.to_numeric(s, errors="coerce")
    s.index = pd.to_datetime(s.index, errors="coerce")
    return s[s.index.notna()].sort_index()


def _station_id(obs, x: float, y: float) -> str:
    """Resolve a STABLE KNMI station identifier from the Obs — the KNMI station NUMBER only.
    `obs.name` (e.g. 'RH_HEINO_278') is deliberately NOT a fallback: it is a different key than the
    station number, so mixing them across runs would split one station into two series and break the
    idempotent PK (the slice-1 multi-tube identity lesson). If the station number is genuinely absent
    we fall back to a deterministic coordinate id and WARN — never to the variable-shaped name."""
    v = getattr(obs, "station", None)
    if v not in (None, ""):
        return str(v)
    print(f"[knmi] WARNING: obs.station missing — using coordinate id xy_{int(x)}_{int(y)}", flush=True)
    return f"xy_{int(x)}_{int(y)}"


def _daily_dates_to_utc_iso(idx: pd.DatetimeIndex) -> list[str]:
    """KNMI daily values are keyed to a CALENDAR DATE; the raw stamp's arbitrary 01:00 local instant
    is not meaningful for a daily aggregate. Anchor each observation's date to UTC midnight so the
    date label is stable across DST and carries an explicit +00:00 offset.

    This deliberately AVOIDS localizing the 01:00 stamp to Amsterdam then converting to UTC: that
    produced 00:00Z in winter but 23:00Z of the PREVIOUS day in summer (DST +02:00) — a silent
    one-day shift of the daily total. Taking the calendar date directly is season-invariant."""
    if idx.tz is not None:
        idx = idx.tz_convert(_TZ)  # if ever tz-aware, read the date in its local day first
    dates = pd.to_datetime(pd.DatetimeIndex(idx).date).tz_localize("UTC")
    return [t.isoformat() for t in dates.to_pydatetime()]


def fetch_knmi(
    xy: list[tuple[float, float]],   # RD/EPSG:28992 coordinate(s); read_knmi picks the nearest station
    codes: list[str],                # KNMI var codes, e.g. ['RH']
    start: str,
    end: str,
    min_obs: int = 1,
) -> list[SeriesPayload]:
    import hydropandas as hpd  # heavy; lazy import

    payloads: list[SeriesPayload] = []
    for code in codes:
        if code not in KNMI_VARS:
            print(f"[knmi] {code}: unknown var (known: {','.join(KNMI_VARS)}) — skipped", flush=True)
            continue
        variable, unit, aggregation, scale = KNMI_VARS[code]
        # Per-var guarded: one var's API failure skips THAT var, never aborts the run.
        try:
            oc = hpd.read_knmi(xy=xy, meteo_vars=[code], starts=start, ends=end,
                               fill_missing_obs=False, raise_exceptions=False)
            obs = oc.obs.iloc[0]
            s = _obs_series(obs).loc[start:end]
            if len(s) < min_obs:
                print(f"[knmi] {code}: only {len(s)} obs — skipped (min_obs={min_obs})", flush=True)
                continue
            x = float(getattr(obs, "x", xy[0][0]))
            y = float(getattr(obs, "y", xy[0][1]))
            station = _station_id(obs, x, y)
            timestamps = _daily_dates_to_utc_iso(pd.DatetimeIndex(s.index))
            # scale (metres→mm for RH/EV24) and NaN→None in one pass
            values = [None if pd.isna(v) else float(v) * scale for v in s.to_numpy()]
            payloads.append(SeriesPayload(
                source="knmi",
                location_id=station,           # KNMI station id — the monitoring point
                variable=variable,
                unit=unit,
                aggregation=aggregation,
                timestamps=timestamps,
                values=values,
                native_resolution="daily",
                endpoint="hydropandas.read_knmi",
                request_params={"code": code, "xy": [list(p) for p in xy],
                                "start": start, "end": end,
                                "timezone": "UTC",
                                "timestamp_basis": "calendar date at UTC midnight (daily aggregate)",
                                "source_stamp_tz": "Europe/Amsterdam-naive (01:00 KNMI day stamp, discarded)",
                                "scaled_to_unit": unit, "scale_factor": scale},
                location_label=f"KNMI {station} ({code})",
                lat=None, lon=None,
            ))
            print(f"[knmi] {code} station {station}: {len(s)} daily obs "
                  f"({s.index.min().date()} -> {s.index.max().date()}), unit={unit}", flush=True)
        except Exception as e:
            print(f"[knmi] {code} FAILED: {type(e).__name__}: {str(e)[:160]} — skipped", flush=True)
            continue
    return payloads
