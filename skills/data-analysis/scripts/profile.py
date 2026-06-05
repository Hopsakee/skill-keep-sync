# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pandas>=2.0",
#   "matplotlib>=3.7",
# ]
# ///
"""Deterministic CSV profiler for the DataAnalysis skill.

Loads one CSV, computes a structured profile, renders a handful of PNG charts,
and writes everything NEXT TO the input file. The agent then reads the
`<name>_profile.json` + views the PNGs to write the narrative `<name>_analysis.md`.

This script does the math (reproducible, deterministic). It never writes prose —
that is the narrative pass's job. Output filenames are derived from the input
stem so re-running overwrites cleanly and nothing escapes the dataset's folder.

Usage:
    uv run --script profile.py /path/to/data.csv
    uv run --script profile.py /path/to/data.csv --dry-run
    uv run --script profile.py /path/to/data.csv --max-charts 8
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless — no display, just file output
import matplotlib.pyplot as plt
import pandas as pd

# Caps keep a wide dataset from emitting hundreds of charts. The narrative pass
# only needs enough visual evidence to back its claims, not one chart per column.
MAX_CATEGORICAL_CARDINALITY = 20  # skip count-plots for high-cardinality columns
TOP_CORRELATIONS = 10
SAMPLE_VALUES = 5


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_safe(value):
    """Coerce numpy / pandas scalars to plain JSON-serialisable Python types."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):  # numpy scalar
        return value.item()
    return value


def load_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as exc:  # surface the real reason, don't swallow it
        sys.exit(f"error: could not read CSV {path}: {exc}")


def profile_columns(df: pd.DataFrame) -> list[dict]:
    n = len(df)
    out = []
    for col in df.columns:
        s = df[col]
        missing = int(s.isna().sum())
        out.append(
            {
                "name": str(col),
                "dtype": str(s.dtype),
                "missing_count": missing,
                "missing_pct": round(100 * missing / n, 2) if n else 0.0,
                "n_unique": int(s.nunique(dropna=True)),
                "sample_values": [
                    _json_safe(v) for v in s.dropna().unique()[:SAMPLE_VALUES]
                ],
            }
        )
    return out


def numeric_summary(df: pd.DataFrame) -> dict:
    num = df.select_dtypes(include="number")
    if num.empty:
        return {}
    desc = num.describe().to_dict()
    return {
        col: {stat: _json_safe(val) for stat, val in stats.items()}
        for col, stats in desc.items()
    }


def categorical_summary(df: pd.DataFrame) -> dict:
    cat = df.select_dtypes(exclude="number")
    out = {}
    for col in cat.columns:
        counts = df[col].value_counts(dropna=True).head(SAMPLE_VALUES * 2)
        out[str(col)] = {
            "n_unique": int(df[col].nunique(dropna=True)),
            "top_values": [
                {"value": _json_safe(idx), "count": int(cnt)}
                for idx, cnt in counts.items()
            ],
        }
    return out


def top_correlations(df: pd.DataFrame) -> list[dict]:
    num = df.select_dtypes(include="number")
    if num.shape[1] < 2:
        return []
    corr = num.corr(numeric_only=True)
    seen = set()
    pairs = []
    for a in corr.columns:
        for b in corr.columns:
            if a == b or (b, a) in seen:
                continue
            seen.add((a, b))
            r = corr.loc[a, b]
            if pd.notna(r):
                pairs.append({"a": str(a), "b": str(b), "r": round(float(r), 4)})
    pairs.sort(key=lambda p: abs(p["r"]), reverse=True)
    return pairs[:TOP_CORRELATIONS]


def render_charts(df: pd.DataFrame, stem: str, out_dir: Path, max_charts: int) -> list[dict]:
    """Render distribution histograms, a correlation heatmap, and categorical
    count bars. Returns chart manifest entries (relative paths) for the JSON."""
    charts: list[dict] = []
    num = df.select_dtypes(include="number")
    cat = df.select_dtypes(exclude="number")

    def _remaining() -> int:
        return max_charts - len(charts)

    # 1. Numeric distributions
    for col in num.columns:
        if _remaining() <= 0:
            break
        series = num[col].dropna()
        if series.empty:
            continue
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.hist(series, bins=min(30, max(10, series.nunique())), color="#3B82F6")
        ax.set_title(f"Distribution: {col}")
        ax.set_xlabel(str(col))
        ax.set_ylabel("count")
        fig.tight_layout()
        fname = f"{stem}_dist_{_slug(col)}.png"
        fig.savefig(out_dir / fname, dpi=110)
        plt.close(fig)
        charts.append({"path": fname, "kind": "distribution", "describes": str(col)})

    # 2. Correlation heatmap
    if _remaining() > 0 and num.shape[1] >= 2:
        corr = num.corr(numeric_only=True)
        fig, ax = plt.subplots(figsize=(0.6 * len(corr) + 3, 0.6 * len(corr) + 2.5))
        im = ax.imshow(corr, cmap="RdBu_r", vmin=-1, vmax=1)
        ax.set_xticks(range(len(corr)))
        ax.set_yticks(range(len(corr)))
        ax.set_xticklabels(corr.columns, rotation=45, ha="right", fontsize=8)
        ax.set_yticklabels(corr.columns, fontsize=8)
        ax.set_title("Correlation heatmap")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        fig.tight_layout()
        fname = f"{stem}_corr_heatmap.png"
        fig.savefig(out_dir / fname, dpi=110)
        plt.close(fig)
        charts.append({"path": fname, "kind": "correlation", "describes": "numeric columns"})

    # 3. Categorical counts (only sane-cardinality columns)
    for col in cat.columns:
        if _remaining() <= 0:
            break
        nun = df[col].nunique(dropna=True)
        if nun == 0 or nun > MAX_CATEGORICAL_CARDINALITY:
            continue
        counts = df[col].value_counts(dropna=True).head(MAX_CATEGORICAL_CARDINALITY)
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.bar([str(i) for i in counts.index], counts.values, color="#3B82F6")
        ax.set_title(f"Counts: {col}")
        ax.set_ylabel("count")
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
        fig.tight_layout()
        fname = f"{stem}_counts_{_slug(col)}.png"
        fig.savefig(out_dir / fname, dpi=110)
        plt.close(fig)
        charts.append({"path": fname, "kind": "counts", "describes": str(col)})

    return charts


def _slug(name) -> str:
    return "".join(c if c.isalnum() else "_" for c in str(name)).strip("_").lower()


def main() -> None:
    ap = argparse.ArgumentParser(description="Profile a CSV for the DataAnalysis skill.")
    ap.add_argument("csv", type=Path, help="path to the input CSV")
    ap.add_argument("--dry-run", action="store_true", help="compute + print summary, write nothing")
    ap.add_argument("--max-charts", type=int, default=15, help="cap on PNG charts emitted")
    args = ap.parse_args()

    csv_path = args.csv.expanduser().resolve()
    if not csv_path.is_file():
        sys.exit(f"error: no such file: {csv_path}")

    df = load_csv(csv_path)
    out_dir = csv_path.parent
    stem = csv_path.stem

    profile = {
        "source": str(csv_path),
        "generated_at": _iso_now(),
        "generated_by": "DataAnalysis/profile.py",
        "shape": {"rows": int(df.shape[0]), "cols": int(df.shape[1])},
        "columns": profile_columns(df),
        "numeric_summary": numeric_summary(df),
        "categorical_summary": categorical_summary(df),
        "top_correlations": top_correlations(df),
        "charts": [],
    }

    if args.dry_run:
        print(
            f"dry-run: {csv_path.name} → {profile['shape']['rows']} rows × "
            f"{profile['shape']['cols']} cols | "
            f"{len(profile['numeric_summary'])} numeric, "
            f"{len(profile['categorical_summary'])} categorical | "
            f"would write {stem}_profile.json + charts to {out_dir}"
        )
        return

    profile["charts"] = render_charts(df, stem, out_dir, args.max_charts)

    profile_path = out_dir / f"{stem}_profile.json"
    profile_path.write_text(json.dumps(profile, indent=2, ensure_ascii=False))

    print(
        f"ok: wrote {profile_path.name} + {len(profile['charts'])} charts to {out_dir} "
        f"({profile['shape']['rows']} rows × {profile['shape']['cols']} cols)"
    )


if __name__ == "__main__":
    main()
