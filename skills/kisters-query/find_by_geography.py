"""Find KiWIS measurement sites near a geographic location using OSM/Nominatim.

Resolves the location via Nominatim. If the OSM feature has a meaningful bounding
box (e.g. a waterway like Meppelerdiep), that bbox is used as the search area.
For point locations (e.g. a city name), a radius is used instead.
A haversine distance from the centroid is always computed and shown in the output.

Usage
-----
# All water level sites near the Meppelerdiep (default 1 km buffer for waterways)
uv run python .windsurf/workflows/kisters-query/scripts/find_by_geography.py \\
    --near "Meppelerdiep" --parameter H

# All discharge sites within 7 km of Zwolle that have 2024 measurements
uv run python .windsurf/workflows/kisters-query/scripts/find_by_geography.py \\
    --near "Zwolle" --parameter Q --radius 7 --from 2024-01-01 --to 2024-12-31

# All sites near Meppelerdiep, any parameter, measured in 2024
uv run python .windsurf/workflows/kisters-query/scripts/find_by_geography.py \\
    --near "Meppelerdiep" --from 2024-01-01 --to 2024-12-31
"""

import argparse
import math
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

SKILL_ROOT = Path(__file__).parent  # .windsurf/workflows/kisters-query/scripts/
sys.path.insert(0, str(SKILL_ROOT))

from kiwis import kiwis, save_to_csv  # noqa: E402

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "wiski-llm-test/1.0"}
DEFAULT_POINT_RADIUS_KM = 0.05   # default radius for point locations (e.g. a city)
DEFAULT_AREA_BUFFER_KM = 0.05    # default buffer for area/waterway features (e.g. Meppelerdiep)

_PARAM_RULES: dict[str, dict] = {
    'H': {'name_contains': ['Waterstand', 'Peil'], 'unit_contains': ['NAP'], 'name_prefix': 'H'},
    'Q': {'name_contains': ['Debiet'], 'unit_contains': ['m3/s']},
    'V': {'name_contains': ['Volume'], 'unit_contains': ['m3']},
    'P': {'name_contains': ['Neerslag', 'Regen', 'Precip'], 'unit_contains': ['mm']},
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in km between two WGS84 points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + (
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _bbox_from_radius(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    """Compute bbox (min_lon, min_lat, max_lon, max_lat) around a point."""
    lat_delta = radius_km / 111.0
    lon_delta = radius_km / (111.0 * math.cos(math.radians(lat)))
    return lon - lon_delta, lat - lat_delta, lon + lon_delta, lat + lat_delta


def _bbox_span_km(
    min_lon: float, min_lat: float, max_lon: float, max_lat: float
) -> tuple[float, float]:
    """Return (width_km, height_km) of a bounding box."""
    mid_lat = (min_lat + max_lat) / 2
    width_km = haversine_km(mid_lat, min_lon, mid_lat, max_lon)
    height_km = haversine_km(min_lat, min_lon, max_lat, min_lon)
    return width_km, height_km


def _nominatim_query(name: str) -> dict:
    """Single Nominatim lookup; returns parsed dict or empty dict if not found."""
    r = requests.get(
        NOMINATIM_URL,
        params={'q': name, 'format': 'json', 'limit': 1, 'addressdetails': 0},
        headers=NOMINATIM_HEADERS,
        timeout=10,
    )
    r.raise_for_status()
    results = r.json()
    if not results:
        return {}
    result = results[0]
    lat = float(result['lat'])
    lon = float(result['lon'])
    bb = result.get('boundingbox', [])
    bbox = (float(bb[2]), float(bb[0]), float(bb[3]), float(bb[1])) if len(bb) == 4 else (lon, lat, lon, lat)
    return {
        'lat': lat,
        'lon': lon,
        'display_name': result.get('display_name', name),
        'osm_type': result.get('osm_type', ''),
        'osm_class': result.get('class', ''),
        'bbox': bbox,
    }


def lookup_location(name: str) -> dict:
    """Query Nominatim for a location name and return coordinates and bbox.

    Tries the exact name first, then falls back to:
    - Name without spaces (e.g. 'Meppeler Diep' → 'Meppelerdiep')
    - Name with ', Netherlands' appended

    Parameters
    ----------
    name : str
        Location name, e.g. 'Meppelerdiep' or 'Zwolle, Netherlands'.

    Returns
    -------
    dict
        Keys: lat, lon, display_name, osm_type, osm_class,
        bbox as (min_lon, min_lat, max_lon, max_lat). Empty dict if not found.
    """
    loc = _nominatim_query(name)
    if loc:
        return loc

    no_spaces = name.replace(' ', '')
    if no_spaces != name:
        loc = _nominatim_query(no_spaces)
        if loc:
            print(f"  ('{name}' niet gevonden; automatisch gevonden als '{no_spaces}')")
            return loc

    with_nl = f"{name}, Netherlands"
    loc = _nominatim_query(with_nl)
    if loc:
        print(f"  ('{name}' niet gevonden; automatisch gevonden als '{with_nl}')")
        return loc

    return {}


def find_sites_in_area(
    location_name: str,
    radius_km: Optional[float] = None,
    parameter: Optional[str] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Find all KiWIS sites near a geographic location.

    Uses Nominatim to resolve the location. For features with spatial extent
    (waterways, areas) the OSM bbox + buffer is used. For point locations a
    radius circle around the centroid is used.

    Default search distance depends on feature type:
    - Waterway/area (e.g. Meppelerdiep): 1 km buffer around OSM bbox
    - Point location (e.g. Zwolle): 5 km radius from centroid
    Override with ``radius_km``.

    Note: getSiteList does NOT support bbox on this KiWIS instance.
    getStationList is used instead and results are aggregated to unique sites.

    Parameters
    ----------
    location_name : str
        Location to resolve via Nominatim.
    radius_km : float, optional
        Override the default buffer/radius in km.
    parameter : str, optional
        Parameter type to filter on client-side (e.g. 'Q').

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, dict]
        (df_sites, df_stations, nominatim location info dict).

    Raises
    ------
    ValueError
        If the location cannot be found via Nominatim.
    """
    print(f"Looking up '{location_name}' via OpenStreetMap (Nominatim)...")
    loc = lookup_location(location_name)
    if not loc:
        raise ValueError(
            f"Location '{location_name}' not found via Nominatim. "
            "Try a more specific name, e.g. 'Meppelerdiep, Overijssel'."
        )

    print(f"Gevonden: {loc['display_name']}")
    print(f"Centroïde: {loc['lat']:.4f}°N, {loc['lon']:.4f}°E  (OSM type: {loc['osm_class']})")

    min_lon, min_lat, max_lon, max_lat = loc['bbox']
    width_km, height_km = _bbox_span_km(min_lon, min_lat, max_lon, max_lat)

    if width_km > 0.5 or height_km > 0.5:
        effective_km = radius_km if radius_km is not None else DEFAULT_AREA_BUFFER_KM
        print(
            f"OSM-feature heeft een oppervlak van {width_km:.1f} km × {height_km:.1f} km — "
            f"bbox wordt gebruikt met {effective_km:.1f} km buffer."
        )
        lat_buf = effective_km / 111.0
        lon_buf = effective_km / (111.0 * math.cos(math.radians(loc['lat'])))
        query_bbox = (min_lon - lon_buf, min_lat - lat_buf, max_lon + lon_buf, max_lat + lat_buf)
        is_area = True
    else:
        effective_km = radius_km if radius_km is not None else DEFAULT_POINT_RADIUS_KM
        print(
            f"OSM-resultaat is een punt — alle meetpunten binnen {effective_km:.1f} km "
            f"van {loc['display_name'].split(',')[0]} worden opgehaald."
        )
        query_bbox = _bbox_from_radius(loc['lat'], loc['lon'], effective_km)
        is_area = False

    bbox_str = f"{query_bbox[0]:.6f},{query_bbox[1]:.6f},{query_bbox[2]:.6f},{query_bbox[3]:.6f}"
    print(f"\nKiWIS query met bbox: {bbox_str}...")

    station_params: dict = {
        "bbox": bbox_str,
        "returnfields": "station_no,station_name,site_no,site_name,station_latitude,station_longitude",
    }
    # NOTE: do NOT add parametertype_name here — the server-side filter misses compound
    # parameter names like 'Q [m3/s] [NVT] [OW]'. Client-side filtering is done later.

    df_stations = kiwis("getStationList", **station_params)

    if df_stations.empty:
        print("Geen KiWIS-meetpunten gevonden in dit gebied.")
        return pd.DataFrame(), pd.DataFrame(), loc

    print(f"KiWIS retourneerde {len(df_stations)} station(s) in de bbox.")

    df_stations = df_stations[
        (df_stations['station_latitude'] != '') & (df_stations['station_longitude'] != '')
    ].copy()
    df_stations['station_latitude'] = df_stations['station_latitude'].astype(float)
    df_stations['station_longitude'] = df_stations['station_longitude'].astype(float)
    df_stations['distance_km'] = df_stations.apply(
        lambda row: haversine_km(loc['lat'], loc['lon'], row['station_latitude'], row['station_longitude']),
        axis=1,
    )

    if not is_area:
        # Point location: KiWIS bbox is square, trim to circle.
        df_stations = df_stations[df_stations['distance_km'] <= effective_km]
    # For area features: KiWIS bbox already scoped to OSM bbox + buffer — keep all.

    if df_stations.empty:
        return pd.DataFrame(), pd.DataFrame(), loc

    # Aggregate to unique sites using closest station per site as representative
    df_sites = (
        df_stations.sort_values('distance_km')
        .groupby('site_no', as_index=False)
        .first()
    )[['site_no', 'site_name', 'station_latitude', 'station_longitude', 'distance_km']]
    df_sites = df_sites.rename(
        columns={'station_latitude': 'site_latitude', 'station_longitude': 'site_longitude'}
    )
    df_sites = df_sites.sort_values('distance_km').reset_index(drop=True)

    df_stations_out = df_stations[['station_no', 'station_name', 'site_no', 'distance_km']].copy()

    return df_sites, df_stations_out, loc


def _ts_matches_parameter(row: pd.Series, parameter: str) -> bool:
    """Return True if a timeseries or parameter row matches the given parameter type.

    Works with rows from both getTimeseriesList (ts_unitname) and
    getParameterList (parametertype_unitname).
    """
    rules = _PARAM_RULES.get(parameter.upper())
    if not rules:
        return True

    name = str(row.get('stationparameter_name', ''))
    unit = str(row.get('ts_unitname', '') or row.get('parametertype_unitname', ''))

    for kw in rules.get('name_contains', []):
        if kw.lower() in name.lower():
            return True
    for kw in rules.get('unit_contains', []):
        if kw.lower() in unit.lower():
            return True
    if 'name_prefix' in rules and name.upper().startswith(rules['name_prefix']):
        return True
    return False


def _ts_covers_period(row: pd.Series, from_dt: Optional[str], to_dt: Optional[str]) -> bool:
    """Return True if a timeseries row has data and its coverage overlaps the period.

    Returns False if the timeseries has no data at all (empty 'from' field),
    regardless of whether a date range is requested.
    """
    ts_from = str(row.get('from', ''))[:10]
    ts_to = str(row.get('to', ''))[:10]
    if not ts_from:  # timeseries exists but has no measurements
        return False
    if from_dt and ts_to and ts_to < from_dt:
        return False
    if to_dt and ts_from and ts_from > to_dt:
        return False
    return True


def _filter_sites_by_parameter(df_sites: pd.DataFrame, parameter: str) -> pd.DataFrame:
    """Phase 1 — fast: keep only sites that have the given parameter type.

    Uses getParameterList (one call per site). Handles compound parameter names
    like 'Q [m3/s] [NVT] [OW]' that the server-side filter misses.

    Parameters
    ----------
    df_sites : pd.DataFrame
        Candidate sites (must have 'site_no').
    parameter : str
        Parameter type, e.g. 'H', 'Q'.

    Returns
    -------
    pd.DataFrame
        Subset of df_sites whose sites have at least one matching parameter.
    """
    matching_rows = []
    for _, site in df_sites.iterrows():
        site_no = str(site['site_no'])
        df_params = kiwis(
            "getParameterList",
            site_no=site_no,
            returnfields="stationparameter_name,parametertype_unitname",
        )
        if df_params.empty:
            continue
        if any(_ts_matches_parameter(row, parameter) for _, row in df_params.iterrows()):
            matching_rows.append(site)
    if not matching_rows:
        return pd.DataFrame(columns=df_sites.columns)
    return pd.DataFrame(matching_rows).reset_index(drop=True)


def filter_sites_by_coverage(
    df_sites: pd.DataFrame,
    parameter: Optional[str],
    from_dt: Optional[str],
    to_dt: Optional[str],
    df_stations: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Phase 2 — slower: verify actual measurements exist and check date coverage.

    Uses getTimeseriesList with coverage per station. Only includes sites whose
    matching timeseries actually contain data (non-empty 'from' field) and whose
    coverage overlaps the requested date range (if given).

    Parameters
    ----------
    df_sites : pd.DataFrame
        Candidate sites (must have 'site_no'). Should already be pre-filtered
        by ``_filter_sites_by_parameter`` for performance.
    parameter : str, optional
        Parameter type, e.g. 'H', 'Q'.
    from_dt : str, optional
        Start date YYYY-MM-DD.
    to_dt : str, optional
        End date YYYY-MM-DD.
    df_stations : pd.DataFrame, optional
        Pre-fetched stations with 'station_no' and 'site_no' columns.
        When given, skips the per-site getStationList call.

    Returns
    -------
    pd.DataFrame
        Subset of df_sites that have at least one timeseries with actual data.
    """
    matching_rows = []

    for _, site in df_sites.iterrows():
        site_no = str(site['site_no'])

        if df_stations is not None and not df_stations.empty:
            station_nos = df_stations.loc[
                df_stations['site_no'] == site_no, 'station_no'
            ].tolist()
        else:
            fetched = kiwis(
                "getStationList", site_no=site_no, returnfields="station_no,station_name"
            )
            station_nos = fetched['station_no'].tolist() if not fetched.empty else []

        if not station_nos:
            continue

        site_matched = False
        for station_no in station_nos:
            if site_matched:
                break
            df_ts = kiwis(
                "getTimeseriesList",
                station_no=str(station_no),
                returnfields="ts_id,ts_shortname,stationparameter_name,ts_unitname,coverage",
            )
            if df_ts.empty:
                continue
            for _, ts in df_ts.iterrows():
                param_ok = _ts_matches_parameter(ts, parameter) if parameter else True
                period_ok = _ts_covers_period(ts, from_dt, to_dt)
                if param_ok and period_ok:
                    site_matched = True
                    break

        if site_matched:
            matching_rows.append(site)

    if not matching_rows:
        return pd.DataFrame(columns=df_sites.columns)
    return pd.DataFrame(matching_rows).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find KiWIS measurement sites near a geographic location.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--near", required=True,
                        help="Location name, e.g. 'Meppelerdiep' or 'Zwolle'.")
    parser.add_argument("--radius", type=float, default=None,
                        help=(
                            f"Radius/buffer in km. Default: {DEFAULT_AREA_BUFFER_KM} km for "
                            f"waterways/areas, {DEFAULT_POINT_RADIUS_KM} km for cities/points."
                        ))
    parser.add_argument("--parameter", default=None,
                        help="Parameter type: H (water level), Q (discharge), V (volume), P (precipitation).")
    parser.add_argument("--from", dest="from_dt", default=None,
                        help="Only include sites with data from this date (YYYY-MM-DD).")
    parser.add_argument("--to", dest="to_dt", default=None,
                        help="Only include sites with data up to this date (YYYY-MM-DD).")
    parser.add_argument("--output", default=None,
                        help="CSV filename in data/ (auto-generated if omitted).")
    parser.add_argument("--confirm", action="store_true",
                        help="Skip the >25 sites warning and proceed with the coverage check anyway.")
    args = parser.parse_args()

    print(f"\n=== Meetpunten in de buurt van '{args.near}' ===\n")

    try:
        df_sites, df_stations, loc = find_sites_in_area(
            args.near, radius_km=args.radius, parameter=args.parameter
        )
    except ValueError as exc:
        print(f"Fout: {exc}")
        sys.exit(1)

    if df_sites.empty:
        print("Geen meetpunten gevonden.")
        sys.exit(0)

    print(f"\n{len(df_sites)} site(s) gevonden:")
    print(df_sites[['site_no', 'site_name', 'distance_km']].to_string(index=False))

    if args.parameter or args.from_dt or args.to_dt:
        # Phase 1: fast parameter pre-filter via getParameterList (always run, no warning).
        # Reduces the candidate set before the slower Phase 2 coverage check.
        if args.parameter:
            print(f"\nFase 1: parameter filter '{args.parameter}'...")
            df_sites = _filter_sites_by_parameter(df_sites, args.parameter)
            if df_sites.empty:
                print("Geen sites gevonden met bijpassend parameter type.")
                sys.exit(0)
            print(f"{len(df_sites)} site(s) hebben parameter '{args.parameter}'.")

        # Phase 2: verify actual data exists + optional date coverage check.
        # Auto-confirm when the user explicitly specified a small radius (≤ 1 km) —
        # that already shows intentional precision; no need to ask again.
        auto_confirm = args.confirm or (args.radius is not None and args.radius <= 1.0)
        if len(df_sites) > 25 and not auto_confirm:
            print(
                f"\nLet op: {len(df_sites)} sites na parameter filter. De data-check "
                f"kan lang duren voor zo veel sites.\n"
                f"Overweeg de selectie te verkleinen:\n"
                f"  --radius 1   (kleinere straal)\n"
                f"  --near \"<specifieker naam>\"  (specifiekere locatie)\n"
                f"\nOf voeg --confirm toe om toch door te gaan met {len(df_sites)} sites."
            )
            sys.exit(0)

        parts = []
        if args.parameter:
            parts.append(f"parameter '{args.parameter}'")
        if args.from_dt or args.to_dt:
            parts.append(f"periode {args.from_dt or '...'} t/m {args.to_dt or '...'}")
        print(f"\nFase 2: controleer data beschikbaarheid voor {' en '.join(parts)}...")
        print("(Kan even duren — per site worden tijdreeksen opgehaald.)\n")

        df_filtered = filter_sites_by_coverage(
            df_sites, args.parameter, args.from_dt, args.to_dt, df_stations=df_stations
        )

        if df_filtered.empty:
            print("Geen sites gevonden met bijpassende metingen.")
            sys.exit(0)

        print(f"\n{len(df_filtered)} site(s) met bijpassende metingen:\n")
        print(df_filtered[['site_no', 'site_name', 'distance_km']].to_string(index=False))
        df_sites = df_filtered

    if args.output:
        output_file = args.output
    else:
        loc_slug = args.near.lower().replace(' ', '_').replace(',', '')
        param_part = f"_{args.parameter.lower()}" if args.parameter else ""
        year_part = f"_{args.from_dt[:4]}" if args.from_dt else ""
        output_file = f"sites_near_{loc_slug}{param_part}{year_part}.csv"

    save_to_csv(df_sites, output_file)


if __name__ == "__main__":
    main()
