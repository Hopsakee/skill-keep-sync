"""
FetchData — shared packaging core.

The ONE place that knows the on-disk contract: a single SQLite file with three tables —
`data` (tidy observations), `meta` (per-series provenance), `findings` (created empty here,
owned/populated by AnalyseData). Every source fetcher hands this module a list of
`SeriesPayload`s; this module writes them. Adding a source never touches this file.

Pure stdlib (sqlite3) on purpose: the storage contract must not drift with pandas/hydropandas
versions. Fetchers may use pandas; they convert to plain Python (ISO-string timestamps,
float|None values) before handing payloads here.

Design contract (locked 2026-06-06):
  - tidy/long always; one row per observation
  - NaN -> SQL NULL; timestamps as ISO-8601 text
  - idempotent re-fetch via composite PK + INSERT OR REPLACE
  - no resampling / merging / analysis here
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

SKILL_VERSION = "0.1.0"


@dataclass
class SeriesPayload:
    """One time series from one source/location/variable, already tidy and raw."""
    source: str
    location_id: str
    variable: str
    unit: str
    aggregation: str                 # 'raw' | 'weekly_mean' | ... (what the API actually returned)
    timestamps: list[str]            # ISO-8601 strings
    values: list[float | None]       # NaN already converted to None
    native_resolution: str | None = None   # 'daily' | 'irregular' | '10min' ...
    quality: list[str | None] | None = None  # per-row flag, or None
    endpoint: str | None = None
    request_params: dict | None = None
    sentinel_masked: str | None = None
    location_label: str | None = None
    lat: float | None = None
    lon: float | None = None

    def __post_init__(self) -> None:
        if len(self.timestamps) != len(self.values):
            raise ValueError(
                f"timestamps ({len(self.timestamps)}) and values ({len(self.values)}) "
                f"length mismatch for {self.source}/{self.location_id}/{self.variable}"
            )
        if self.quality is not None and len(self.quality) != len(self.timestamps):
            raise ValueError("quality length must match timestamps length")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS data (
    source       TEXT NOT NULL,
    location_id  TEXT NOT NULL,
    variable     TEXT NOT NULL,
    timestamp    TEXT NOT NULL,          -- ISO-8601
    value        REAL,                   -- NULL = missing (NaN)
    unit         TEXT NOT NULL,
    quality      TEXT,
    aggregation  TEXT NOT NULL,          -- 'raw' | 'weekly_mean' | ...
    PRIMARY KEY (source, location_id, variable, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_data_series ON data (source, location_id, variable);

CREATE TABLE IF NOT EXISTS meta (
    source            TEXT NOT NULL,
    location_id       TEXT NOT NULL,
    variable          TEXT NOT NULL,
    unit              TEXT,
    aggregation       TEXT,
    native_resolution TEXT,
    fetched_at        TEXT,
    endpoint          TEXT,
    request_params    TEXT,              -- JSON
    row_count         INTEGER,
    sentinel_masked   TEXT,
    skill_version     TEXT,
    location_label    TEXT,
    lat               REAL,
    lon               REAL,
    PRIMARY KEY (source, location_id, variable)
);

CREATE TABLE IF NOT EXISTS findings (
    finding_id  INTEGER PRIMARY KEY,
    type        TEXT,                    -- correlation | anomaly | trend | fact
    columns     TEXT,                    -- JSON array of series keys
    period      TEXT,
    statistic   REAL,
    confidence  REAL,
    caveats     TEXT,                    -- JSON array (load-bearing: carries over-claim guards)
    evidence    TEXT,
    created_at  TEXT,
    created_by  TEXT                     -- which AnalyseData run
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open (creating parent dirs + the three tables if absent) the package at db_path."""
    p = Path(db_path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _now_iso() -> str:
    # NOTE: real wall-clock is fine in a skill subprocess (unlike workflow scripts).
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_series(conn: sqlite3.Connection, payload: SeriesPayload) -> int:
    """Write one tidy series + upsert its provenance. Idempotent (INSERT OR REPLACE on PK).
    Returns the number of observation rows written."""
    rows = []
    for i, (ts, val) in enumerate(zip(payload.timestamps, payload.values)):
        q = payload.quality[i] if payload.quality is not None else None
        rows.append((
            payload.source, payload.location_id, payload.variable, ts,
            None if val is None else float(val),
            payload.unit, q, payload.aggregation,
        ))
    conn.executemany(
        "INSERT OR REPLACE INTO data "
        "(source, location_id, variable, timestamp, value, unit, quality, aggregation) "
        "VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta "
        "(source, location_id, variable, unit, aggregation, native_resolution, fetched_at, "
        " endpoint, request_params, row_count, sentinel_masked, skill_version, "
        " location_label, lat, lon) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            payload.source, payload.location_id, payload.variable, payload.unit,
            payload.aggregation, payload.native_resolution, _now_iso(),
            payload.endpoint,
            json.dumps(payload.request_params) if payload.request_params is not None else None,
            len(rows), payload.sentinel_masked, SKILL_VERSION,
            payload.location_label, payload.lat, payload.lon,
        ),
    )
    conn.commit()
    return len(rows)


def write_payloads(conn: sqlite3.Connection, payloads: list[SeriesPayload]) -> int:
    """Write many series; return total rows written."""
    return sum(write_series(conn, p) for p in payloads)


def export_csv(conn: sqlite3.Connection, out_path: str | Path) -> int:
    """Export the tidy `data` table to CSV (export only — CSV is never the store).
    Returns rows exported."""
    import csv
    cur = conn.execute(
        "SELECT source, location_id, variable, timestamp, value, unit, quality, aggregation "
        "FROM data ORDER BY source, location_id, variable, timestamp"
    )
    cols = [d[0] for d in cur.description]
    out = Path(out_path).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for row in cur:
            w.writerow(row)
            n += 1
    return n
