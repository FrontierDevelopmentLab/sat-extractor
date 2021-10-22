import contextlib
import datetime
import functools
from typing import List
from typing import Tuple

import joblib
import pyproj


@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""

    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()


def get_dates_in_range(
    start: datetime.datetime,
    end: datetime.datetime,
    interval: int,
) -> List[Tuple[datetime.datetime, datetime.datetime]]:
    """Get all the possible date pairs between start and end for a given interval.

    Args:
        start (datetime): start date
        end (datetime): end date (included)
        interval (int): interval in days

    Returns:
        Tuple[datetime.datetime, datetime.datetime]: A list of all posible (start,end) dates
    """
    # Get all the date ranges for the given interval
    delta = datetime.timedelta(days=interval)
    dates = []
    while start <= end:
        to_date = start + delta
        dates.append((start, to_date))
        start = to_date
    return dates


def get_utm_zone(lat, lon):
    """A function to grab the UTM zone number for any lat/lon location"""
    zone_str = str(int((lon + 180) / 6) + 1)

    if (lat >= 56.0) & (lat < 64.0) & (lon >= 3.0) & (lon < 12.0):
        zone_str = "32"
    elif (lat >= 72.0) & (lat < 84.0):
        if (lon >= 0.0) & (lon < 9.0):
            zone_str = "31"
        elif (lon >= 9.0) & (lon < 21.0):
            zone_str = "33"
        elif (lon >= 21.0) & (lon < 33.0):
            zone_str = "35"
        elif (lon >= 33.0) & (lon < 42.0):
            zone_str = "37"

    return zone_str


def get_utm_epsg(lat, lon, utm_zone=None):
    """A function to combine the UTM zone number and the hemisphere into an EPSG code"""

    if utm_zone is None:
        utm_zone = get_utm_zone(lat, lon)

    if lat > 0:
        return int(f"{str(326)+str(utm_zone)}")
    else:
        return int(f"{str(327)+str(utm_zone)}")


# SentinHub proj functions:
@functools.lru_cache(maxsize=5)
def get_transform_function(crs_from: str, crs_to: str, always_xy=True):
    return pyproj.Transformer.from_proj(
        projection(crs_from),
        projection(crs_to),
        always_xy=always_xy,
    ).transform


@functools.lru_cache(maxsize=5)
def projection(crs):
    if crs == "WGS84":
        proj_str = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    else:
        proj_str = f"EPSG:{crs}"
    return pyproj.Proj(proj_str, preserve_units=True)
