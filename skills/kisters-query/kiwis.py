"""Reusable helpers for querying the KiWIS (WISKI) REST API.

Self-contained module — no project-level dependencies.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import requests

BASE_URL = "http://192.168.168.50:8080/KiWIS/KiWIS"
REQUEST_TIMEOUT = 30
BASE_PARAMS: dict[str, str] = {
    'datasource': '0',
    'service': 'kisters',
    'type': 'queryServices',
    'format': 'json',
}
DATA_DIR = Path("data")


def kiwis(request: str, **kwargs) -> pd.DataFrame:
    """Query the KiWIS API and return the result as a DataFrame.

    Parameters
    ----------
    request : str
        The KiWIS request type (e.g. 'getSiteList', 'getTimeseriesList').
    **kwargs
        Additional query parameters passed to the API.

    Returns
    -------
    pd.DataFrame
        Response as a DataFrame (row 0 = headers). Empty if no data returned.
    """
    r = requests.get(
        BASE_URL,
        params={**BASE_PARAMS, 'request': request, **kwargs},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    if not data or len(data) < 2:
        return pd.DataFrame()
    return pd.DataFrame(data[1:], columns=data[0])


def get_ts_values(
    ts_id: str,
    from_dt: Optional[str] = None,
    to_dt: Optional[str] = None,
    period: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch timeseries values from the KiWIS API.

    Parameters
    ----------
    ts_id : str
        The timeseries ID to fetch values for.
    from_dt : str, optional
        Start datetime string, e.g. '2024-12-01'.
    to_dt : str, optional
        End datetime string, e.g. '2024-12-31'.
    period : str, optional
        ISO 8601 duration, e.g. 'P7D' for last 7 days.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: Timestamp, Value (and any extras).
        Empty DataFrame if no data is available.
    """
    params = {**BASE_PARAMS, 'request': 'getTimeseriesValues', 'ts_id': ts_id}
    if from_dt:
        params['from'] = from_dt
    if to_dt:
        params['to'] = to_dt
    if period:
        params['period'] = period
    r = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    result = r.json()
    if not result:
        return pd.DataFrame()
    ts = result[0]
    cols = [c.strip() for c in ts['columns'].split(',')]
    df = pd.DataFrame(ts['data'], columns=cols)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    return df


def save_to_csv(df: pd.DataFrame, filename: str) -> Path:
    """Save a DataFrame to a CSV file in the data directory.

    Creates the data directory if it does not exist.

    Parameters
    ----------
    df : pd.DataFrame
        Data to save.
    filename : str
        File name (without directory prefix), e.g. 'stroink_discharge_dec2024.csv'.

    Returns
    -------
    Path
        Absolute path to the saved CSV file.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / filename
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")
    return output_path
