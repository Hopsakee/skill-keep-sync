"""Fetch KiWIS timeseries data for a given location and parameter type.

Usage
-----
uv run python .windsurf/workflows/kisters-query/scripts/fetch_timeseries.py \\
    --location "gemaal Stroink" --parameter Q --from 2024-12-01 --to 2024-12-31

uv run python .windsurf/workflows/kisters-query/scripts/fetch_timeseries.py \\
    --location "gemaal Stroink" --parameter Q --period P30D

uv run python .windsurf/workflows/kisters-query/scripts/fetch_timeseries.py \\
    --location "Stroink" --parameter H --from 2024-01-01 --to 2024-12-31 --output stroink_wl_2024.csv
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

SKILL_ROOT = Path(__file__).parent  # .windsurf/workflows/kisters-query/scripts/
sys.path.insert(0, str(SKILL_ROOT))

from kiwis import get_ts_values, kiwis, save_to_csv  # noqa: E402

PARAM_CONFIG: dict[str, dict] = {
    'Q': {
        'description': 'Discharge / debiet (m³/s)',
        'site_type_hints': ['Gemaal', 'MP Debiet', 'Debiet'],
        'station_name_hints': ['Gemaal'],
        'param_name_contains': ['Debiet'],
        'unit_contains': ['m3/s'],
        'ts_preference': ['momentaan.all', 'momentaanall', 'momentaan.v', 'cmd.CS.p', 'mean.1h', 'mean.1d'],
    },
    'H': {
        'description': 'Water level (m NAP)',
        'site_type_hints': ['Waterstand', 'Stuw', 'Inlaat', 'Gemaal'],
        'station_name_hints': ['Hoogwaterzijde', 'Laagwaterzijde', 'Waterstand', 'Inlaat'],
        'param_name_contains': ['Waterstand', 'Peil'],
        'unit_contains': ['NAP'],
        'ts_preference': ['momentaan.all', 'momentaanall', 'momentaan.v', 'cmd.CS.p', 'mean.1h', 'mean.1d'],
    },
    'V': {
        'description': 'Volume (m³)',
        'site_type_hints': ['Gemaal', 'Reservoir', 'Boezem'],
        'station_name_hints': ['Gemaal', 'Volume'],
        'param_name_contains': ['Volume'],
        'unit_contains': ['m3', 'cubic'],
        'ts_preference': ['momentaan.all', 'momentaanall', 'momentaan.v', 'cmd.CS.p', 'mean.1h', 'mean.1d', 'totals.1d'],
    },
    'P': {
        'description': 'Precipitation (mm)',
        'site_type_hints': ['Neerslag', 'Pluviometer', 'Regen'],
        'station_name_hints': ['Neerslag', 'Regen'],
        'param_name_contains': ['Neerslag', 'Regen', 'Precip'],
        'unit_contains': ['mm'],
        'ts_preference': ['momentaan.all', 'momentaanall', 'momentaan.v', 'cmd.CS.p', 'totals.1h', 'totals.1d'],
    },
}

RESOLUTION_PREFERENCE: dict[str, list[str]] = {
    'hourly': ['mean.1h', 'mean.1d'],
    'daily': ['mean.1d', 'mean.1h'],
}


def _name_search_variants(location: str) -> list[str]:
    """Generate progressively shorter search terms from a location name.

    Parameters
    ----------
    location : str
        Full location name, e.g. 'gemaal Stroink'.

    Returns
    -------
    list[str]
        Search variants from most specific to least, e.g. ['gemaal Stroink', 'Stroink', 'gemaal'].
    """
    words = location.split()
    variants = [location]
    for word in reversed(words):
        if word.lower() not in (location.lower(),) and len(word) > 2:
            variants.append(word)
    return list(dict.fromkeys(variants))  # deduplicate while preserving order


def _search_sites_by_name(location: str) -> tuple[pd.DataFrame, str]:
    """Search for sites by name, trying case variants (original, upper, lower) per term.

    Parameters
    ----------
    location : str
        Partial or full location name.

    Returns
    -------
    tuple[pd.DataFrame, str]
        (df_sites, matched_term) — empty DataFrame if nothing found.
    """
    search_variants = _name_search_variants(location)
    for term in search_variants:
        for variant in dict.fromkeys([term, term.upper(), term.lower()]):
            df = kiwis(
                "getSiteList",
                site_name=f"*{variant}*",
                returnfields="site_id,site_no,site_name,site_type_name",
            )
            if not df.empty:
                return df, term
    return pd.DataFrame(), location


def _first_matching_row(df: pd.DataFrame, col: str, hints: list[str]) -> Optional[pd.Series]:
    """Return the first row where col contains any hint (case-insensitive), or None."""
    for hint in hints:
        matches = df[df[col].str.contains(hint, case=False, na=False)]
        if not matches.empty:
            return matches.iloc[0]
    return None


def discover_location(
    location: Optional[str],
    parameter: str,
    site_no_override: Optional[str] = None,
) -> None:
    """Print all available sites, stations, and timeseries for a location and parameter.

    Parameters
    ----------
    location : str, optional
        Partial or full location name. Not required when site_no_override is given.
    parameter : str
        Parameter type key: 'Q', 'H', 'V', 'P', etc.
    site_no_override : str, optional
        If provided, skip name search and use this site_no directly.
    """
    config = PARAM_CONFIG.get(parameter.upper(), {})
    param_contains = config.get('param_name_contains', [parameter])
    unit_contains = config.get('unit_contains', [])

    if site_no_override:
        df_sites = kiwis(
            "getSiteList",
            site_no=site_no_override,
            returnfields="site_id,site_no,site_name,site_type_name",
        )
        matched_term = site_no_override
    else:
        df_sites, matched_term = _search_sites_by_name(location)

    label = site_no_override or location
    if df_sites.empty:
        print(f"No sites found matching '{label}'")
        return

    if not site_no_override and matched_term != location:
        print(f"No exact match for '{location}', showing results for '{matched_term}'.")

    print(f"\nFound {len(df_sites)} site(s) matching '{label}':")
    print()

    total_ts = 0
    for _, site in df_sites.iterrows():
        site_no = str(site["site_no"])
        df_stations = kiwis(
            "getStationList",
            site_no=site_no,
            returnfields="station_no,station_name",
        )
        site_header_printed = False
        for _, sta in df_stations.iterrows():
            station_no = str(sta["station_no"])
            df_ts = kiwis(
                "getTimeseriesList",
                station_no=station_no,
                returnfields="ts_id,ts_shortname,stationparameter_name,ts_unitname,coverage",
            )
            if df_ts.empty:
                continue
            param_mask = df_ts["stationparameter_name"].str.contains(
                "|".join(param_contains), case=False, na=False
            )
            unit_mask = (
                df_ts["ts_unitname"].str.contains("|".join(unit_contains), case=False, na=False)
                if unit_contains
                else pd.Series(False, index=df_ts.index)
            )
            matching_ts = df_ts[param_mask | unit_mask]
            if matching_ts.empty:
                continue
            if not site_header_printed:
                print(f"  Site:     {site['site_name']} ({site['site_type_name']})")
                print(f"  site_no:  {site_no}")
                site_header_printed = True
            print(f"    Station:     {sta['station_name']}")
            print(f"    station_no:  {station_no}")
            print(f"    {'ts_id':<14} {'ts_shortname':<16} {'parameter':<25} {'unit':<14} {'from':<12} {'to'}")
            print(f"    {'-'*14} {'-'*16} {'-'*25} {'-'*14} {'-'*12} {'-'*12}")
            for _, ts in matching_ts.iterrows():
                from_date = str(ts.get("from", ""))[:10]
                to_date = str(ts.get("to", ""))[:10]
                print(
                    f"    {ts['ts_id']:<14} {ts['ts_shortname']:<16} "
                    f"{ts['stationparameter_name']:<25} {ts['ts_unitname']:<14} "
                    f"{from_date:<12} {to_date}"
                )
                total_ts += 1
            print()

    if total_ts == 0:
        print(f"No timeseries found for parameter '{parameter}' at any station matching '{location}'.")
        return

    print(f"Total: {total_ts} timeseries found.")
    print(
        "\nTo fetch data, re-run with --site-no <site_no> --station-no <station_no> "
        "(and optionally --ts-shortname <shortname> or --ts-id <ts_id>)."
    )


def find_station(
    location: Optional[str],
    parameter: str,
    site_no_override: Optional[str] = None,
    station_no_override: Optional[str] = None,
) -> tuple[str, str]:
    """Find the best matching site_no and station_no for a location and parameter.

    Parameters
    ----------
    location : str
        Partial or full location name (e.g. 'Stroink').
    parameter : str
        Parameter type key: 'Q', 'H', 'V', 'P', etc.
    site_no_override : str, optional
        If provided, skip site search and use this site_no directly.
    station_no_override : str, optional
        If provided, skip station selection and use this station_no directly.

    Returns
    -------
    tuple[str, str]
        (site_no, station_no)

    Raises
    ------
    ValueError
        If no matching site or station can be found.
    """
    if site_no_override and station_no_override:
        print(f"Using specified site_no={site_no_override}, station_no={station_no_override}")
        return site_no_override, station_no_override

    config = PARAM_CONFIG.get(parameter.upper(), {})
    site_hints = config.get('site_type_hints', [])
    station_hints = config.get('station_name_hints', [])

    if site_no_override:
        site_no = site_no_override
        print(f"Using specified site_no={site_no}")
    else:
        df_sites, matched_term = _search_sites_by_name(location)
        if df_sites.empty:
            raise ValueError(f"No sites found matching '{location}'")
        if matched_term != location:
            print(f"No exact match for '{location}', found results for '{matched_term}'.")

        print(f"Found {len(df_sites)} site(s) matching '{location}':")
        print(df_sites[["site_no", "site_name", "site_type_name"]].to_string(index=False))

        site_row = _first_matching_row(df_sites, "site_type_name", site_hints)
        if site_row is None:
            site_row = df_sites.iloc[0]
            print(f"\nNo preferred site type found ({site_hints}), using first: {site_row['site_name']}")
        else:
            print(f"\nSelected site: {site_row['site_name']} ({site_row['site_no']})")

        site_no = str(site_row["site_no"])

    if station_no_override:
        print(f"Using specified station_no={station_no_override}")
        return site_no, station_no_override

    df_stations = kiwis(
        "getStationList",
        site_no=site_no,
        returnfields="station_id,station_no,station_name,site_no",
    )
    if df_stations.empty:
        raise ValueError(f"No stations found at site {site_no}")

    station_row = _first_matching_row(df_stations, "station_name", station_hints)
    if station_row is None:
        station_row = df_stations.iloc[0]

    station_no = str(station_row["station_no"])
    print(f"Selected station: {station_row['station_name']} ({station_no})")

    return site_no, station_no


def find_timeseries(
    station_no: str,
    parameter: str,
    ts_shortname_override: Optional[str] = None,
    resolution: str = "raw",
) -> str:
    """Find the best timeseries ID for a station and parameter.

    Parameters
    ----------
    station_no : str
        Station number from KiWIS.
    parameter : str
        Parameter type key: 'Q', 'H', 'V', 'P', etc.

    Returns
    -------
    str
        Timeseries ID (ts_id).

    Raises
    ------
    ValueError
        If no matching timeseries is found for the parameter.
    """
    config = PARAM_CONFIG.get(parameter.upper(), {})

    if ts_shortname_override:
        df_ts_all = kiwis(
            "getTimeseriesList",
            station_no=station_no,
            returnfields="ts_id,ts_shortname,stationparameter_name,ts_unitname",
        )
        if df_ts_all.empty:
            print(
                f"Warning: no timeseries returned for station '{station_no}' — "
                "check that station_no is correct (use --discover to verify)."
            )
        elif "ts_shortname" not in df_ts_all.columns:
            print(f"Warning: unexpected response for station '{station_no}' (missing ts_shortname column).")
        else:
            match = df_ts_all[df_ts_all["ts_shortname"] == ts_shortname_override]
            if not match.empty:
                ts_id = str(match.iloc[0]["ts_id"])
                print(f"Using specified timeseries: {ts_shortname_override} (ts_id={ts_id})")
                return ts_id
            print(f"Warning: ts_shortname '{ts_shortname_override}' not found, falling back to auto-selection.")
    param_contains = config.get('param_name_contains', [parameter])
    unit_contains = config.get('unit_contains', [])
    ts_preference = RESOLUTION_PREFERENCE.get(resolution, config.get('ts_preference', ['momentaan.all', 'momentaanall', 'momentaan.v', 'cmd.CS.p', 'mean.1h', 'mean.1d']))

    df_ts = kiwis(
        "getTimeseriesList",
        station_no=station_no,
        returnfields="ts_id,ts_shortname,stationparameter_name,ts_unitname,coverage",
    )
    if df_ts.empty:
        raise ValueError(f"No timeseries found for station {station_no}")

    param_mask = df_ts["stationparameter_name"].str.contains(
        "|".join(param_contains), case=False, na=False
    )
    unit_mask = (
        df_ts["ts_unitname"].str.contains("|".join(unit_contains), case=False, na=False)
        if unit_contains
        else pd.Series(False, index=df_ts.index)
    )
    filtered = df_ts[param_mask | unit_mask]

    if filtered.empty:
        available = df_ts["stationparameter_name"].unique().tolist()
        raise ValueError(
            f"No timeseries found for parameter '{parameter}' at station {station_no}.\n"
            f"Available parameters: {available}"
        )

    print(f"\nFound {len(filtered)} candidate timeseries for parameter {parameter}:")
    print(filtered[["ts_id", "ts_shortname", "stationparameter_name", "ts_unitname"]].to_string(index=False))

    for pref in ts_preference:
        match = filtered[filtered["ts_shortname"] == pref]
        if not match.empty:
            ts_id = str(match.iloc[0]["ts_id"])
            print(f"Using timeseries: {pref} (ts_id={ts_id})")
            return ts_id

    ts_id = str(filtered.iloc[0]["ts_id"])
    print(f"No preferred resolution found ({ts_preference}), using first (ts_id={ts_id})")
    return ts_id


def build_output_filename(
    location: Optional[str],
    parameter: Optional[str],
    from_dt: Optional[str],
    to_dt: Optional[str],
    period: Optional[str],
) -> str:
    """Generate a descriptive CSV filename from query parameters."""
    loc_slug = (location or "data").lower().replace(" ", "_").replace("/", "_")
    if period:
        time_part = period.lower()
    elif from_dt and to_dt:
        time_part = f"{from_dt}_to_{to_dt}"
    elif from_dt:
        time_part = f"from_{from_dt}"
    else:
        time_part = "recent"
    return f"{loc_slug}_{(parameter or 'data').lower()}_{time_part}.csv"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch KiWIS timeseries data for a location and parameter.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--location", default=None,
        help="Location name or partial name (e.g. 'Stroink', 'gemaal Stroink'). Not required when --site-no or --ts-id is given.",
    )
    parser.add_argument(
        "--parameter", default=None,
        help="Parameter type: Q (discharge), H (water level), V (volume), P (precipitation). Not required when --ts-id is given.",
    )
    parser.add_argument("--from", dest="from_dt", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_dt", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--period", default=None,
        help="ISO 8601 period, e.g. P30D (last 30 days), P1Y (last year). Alternative to --from/--to.",
    )
    parser.add_argument("--output", default=None, help="Output CSV filename (auto-generated if omitted)")
    parser.add_argument(
        "--discover", action="store_true",
        help="List all available sites/stations/timeseries for the location+parameter and exit.",
    )
    parser.add_argument("--site-no", dest="site_no", default=None, help="Override site selection (use exact site_no)")
    parser.add_argument("--station-no", dest="station_no", default=None, help="Override station selection (use exact station_no)")
    parser.add_argument("--ts-shortname", dest="ts_shortname", default=None, help="Override timeseries selection (e.g. mean.1d, mean.1h)")
    parser.add_argument(
        "--ts-id", dest="ts_id", default=None,
        help="Skip all station/timeseries resolution and fetch directly using this ts_id (from --discover output)",
    )
    parser.add_argument(
        "--resolution", choices=["raw", "hourly", "daily"], default="raw",
        help="Timeseries resolution to prefer: raw (default, momentaan.*/cmd.CS.p), hourly (mean.1h), daily (mean.1d)",
    )
    args = parser.parse_args()

    if not args.location and not args.site_no and not args.ts_id:
        parser.error("--location is required unless --site-no or --ts-id is given.")
    if not args.parameter and not args.ts_id:
        parser.error("--parameter is required unless --ts-id is given.")

    if args.discover:
        if not args.parameter:
            parser.error("--parameter is required for --discover mode.")
        discover_location(args.location, args.parameter, site_no_override=args.site_no)
        return

    param_label = args.parameter or "data"
    loc_label = args.location or args.site_no or args.ts_id
    print(f"\nFetching {param_label} data for '{loc_label}'...\n")

    if args.ts_id:
        print(f"Using specified ts_id={args.ts_id} (skipping station/timeseries resolution)")
        ts_id: str | None = args.ts_id
        _site_no = args.site_no or ""
        station_no = args.station_no or ""
    else:
        _site_no, station_no = find_station(args.location, args.parameter, args.site_no, args.station_no)

        ts_id = None
        try:
            ts_id = find_timeseries(station_no, args.parameter, args.ts_shortname, args.resolution)
        except ValueError as exc:
            print(f"Warning: {exc}")
            print("Trying other stations at the same site...")
            df_stations = kiwis(
                "getStationList",
                site_no=_site_no,
                returnfields="station_no,station_name",
            )
            for _, row in df_stations.iterrows():
                if str(row["station_no"]) == station_no:
                    continue
                try:
                    ts_id = find_timeseries(str(row["station_no"]), args.parameter, args.ts_shortname, args.resolution)
                    station_no = str(row["station_no"])
                    print(f"Found parameter at station: {row['station_name']} ({station_no})")
                    break
                except ValueError:
                    continue

    if ts_id is None:
        print(f"Error: could not find parameter '{args.parameter}' at any station for '{args.location}'.")
        sys.exit(1)

    print(f"\nFetching values (from={args.from_dt}, to={args.to_dt}, period={args.period})...")
    df = get_ts_values(ts_id, from_dt=args.from_dt, to_dt=args.to_dt, period=args.period)

    if df.empty:
        print("No data returned for the specified period.")
        sys.exit(1)

    print(f"\nRetrieved {len(df)} measurements.")
    print(f"\n{df.to_string(index=False)}")
    print(f"\nSummary:\n{df.describe()}")

    output_file = args.output or build_output_filename(
        args.location, args.parameter, args.from_dt, args.to_dt, args.period
    )
    save_to_csv(df, output_file)


if __name__ == "__main__":
    main()
