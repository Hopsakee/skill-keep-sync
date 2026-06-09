"""
BRO groundwater fetcher for FetchData.

Generalises the proven path in a prior hydrology-fetch script:
  GMW well + tube  --(hydropandas get_gld_ids_from_gmw)-->  GLD id
  GLD id  --(raw BRO seriesAsCsv endpoint)-->  observation series   (~17x faster than WaterML XML)

CRITICAL DIFFERENCE from the proving ground: that script did `.resample("D").mean()` inside the
fetch. We DO NOT. FetchData stores raw, native-resolution observations; resampling is
AnalyseData's job (raw-store rule). aggregation is therefore 'raw'.

Its ONLY job: hit the API, hand back tidy SeriesPayloads. The shared core writes them.
"""
from __future__ import annotations

import io

import numpy as np
import pandas as pd
import requests

# store.py lives one directory up; importable when scripts/ is on sys.path (fetch.py arranges that).
from store import SeriesPayload

GLD_SERIES_CSV = "https://publiek.broservices.nl/gm/gld/v1/seriesAsCsv/{gld}"
VARIABLE = "groundwater_level"
UNIT = "m NAP"
_VALUE_COLS = (
    "Beoordeelde Waarde [m]", "Voorlopige Waarde [m]",
    "Onbekend Waarde [m]", "Controle Waarde [m]",
)


def _native_resolution(idx: pd.DatetimeIndex) -> str:
    if len(idx) < 3:
        return "unknown"
    med = pd.Series(idx).sort_values().diff().dropna().median()
    days = med / pd.Timedelta(days=1)
    if days < 0.9:
        return "subdaily"
    if days <= 1.5:
        return "daily"
    if days <= 8:
        return "weekly-ish"
    return "irregular"


def _gld_raw_series(gld_id: str) -> pd.Series:
    """Raw GLD level series (m NAP) — coalesce best-available value column, NO resampling."""
    r = requests.get(GLD_SERIES_CSV.format(gld=gld_id), timeout=120)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), low_memory=False)
    # BRO Tijdstip is epoch-ms in UTC. Keep it tz-aware UTC so isoformat() emits an explicit
    # +00:00 offset — downstream cross-source joins (KNMI/RWS) must not silently misalign.
    ts = pd.to_datetime(df["Tijdstip"], unit="ms", errors="coerce", utc=True)
    val = None
    for col in _VALUE_COLS:
        if col in df.columns:
            c = pd.to_numeric(df[col], errors="coerce")
            val = c if val is None else val.fillna(c)
    if val is None:
        return pd.Series(dtype="float64")
    s = pd.Series(val.to_numpy(), index=ts).dropna()
    s = s[s.index.notna()].sort_index()
    s = s[~s.index.duplicated(keep="first")]
    return s  # RAW — not resampled


def fetch_bro(
    wells: list[tuple[str, str, int]],   # (label, gmw_id, tube_nr)
    start: str,
    end: str,
    min_obs: int = 50,
) -> list[SeriesPayload]:
    from hydropandas.io.bro import get_gld_ids_from_gmw  # heavy; lazy import

    payloads: list[SeriesPayload] = []
    for label, gmw, tube in wells:
        # Whole well guarded: a network blip on one well skips THAT well, never aborts the run
        # (and the store commits per-series, so already-fetched wells stay persisted).
        try:
            glds = get_gld_ids_from_gmw(gmw, tube)
            if not glds:
                print(f"[bro] {label} {gmw}_{tube}: no GLD", flush=True)
                continue
            gld = glds[0]  # one tube CAN map to multiple GLDs; slice-1 takes the first
            if len(glds) > 1:
                print(f"[bro] {label} {gmw}_{tube}: {len(glds)} GLDs — using {gld}, ignoring rest",
                      flush=True)
            # tz-aware UTC index; string bounds are localized to the index tz by pandas
            s = _gld_raw_series(gld).loc[start:end]
            if len(s) < min_obs:
                print(f"[bro] {label}: only {len(s)} raw obs — skipped (min_obs={min_obs})", flush=True)
                continue
            timestamps = [t.isoformat() for t in s.index.to_pydatetime()]  # -> '...+00:00'
            values = [None if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)
                      for v in s.to_numpy()]
            payloads.append(SeriesPayload(
                source="bro",
                # well+tube is the monitoring point — two tubes of one well must NOT collide on the PK
                location_id=f"{gmw}-{tube}",
                variable=VARIABLE,
                unit=UNIT,
                aggregation="raw",
                timestamps=timestamps,
                values=values,
                native_resolution=_native_resolution(s.index),
                endpoint=GLD_SERIES_CSV.format(gld=gld),
                request_params={"gmw": gmw, "tube": tube, "gld": gld,
                                "start": start, "end": end, "timezone": "UTC"},
                location_label=label,
            ))
            print(f"[bro] {label} {gld}: {len(s)} raw obs "
                  f"({s.index.min().date()} -> {s.index.max().date()})", flush=True)
        except Exception as e:
            print(f"[bro] {label} {gmw}_{tube} FAILED: {type(e).__name__}: {str(e)[:160]} — skipped",
                  flush=True)
            continue
    return payloads
