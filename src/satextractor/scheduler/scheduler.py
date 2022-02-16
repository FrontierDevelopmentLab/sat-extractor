import datetime
from collections import defaultdict
from itertools import compress
from typing import Any
from typing import List
from typing import Union

import geopandas as gpd
import numpy as np
import pandas as pd
import pystac
import shapely
import zarr
from joblib import delayed
from joblib import Parallel
from loguru import logger
from satextractor.models import ExtractionTask
from satextractor.models import Tile
from satextractor.models.constellation_info import BAND_INFO
from satextractor.tiler import split_region_in_utm_tiles
from satextractor.utils import get_dates_in_range
from satextractor.utils import tqdm_joblib
from sentinelhub import CRS
from tqdm import tqdm
from zarr.errors import ArrayNotFoundError
from zarr.errors import ContainsGroupError
from zarr.errors import PathNotFoundError


def filter_already_extracted_tasks(fs_mapper, storage_path, extraction_tasks):

    tiles = set([tile.id for task in extraction_tasks for tile in task.tiles])
    constellations = set([task.constellation for task in extraction_tasks])

    tile_constellation_sensing_times = defaultdict(np.array)

    # Get the existing dates for the task tiles and constellation
    for tile_id in tiles:
        for constellation in constellations:
            try:
                patch_constellation_path = f"{storage_path}/{tile_id}/{constellation}"

                timestamps_path = f"{patch_constellation_path}/timestamps"
                existing_timestamps = zarr.open_array(fs_mapper(timestamps_path), "r")[
                    :
                ]
                existing_timestamps = np.array(
                    [
                        np.datetime64(datetime.datetime.fromisoformat(el))
                        for el in existing_timestamps
                    ],
                )

                tile_constellation_sensing_times[
                    patch_constellation_path
                ] = existing_timestamps
            except (PathNotFoundError, ContainsGroupError, ArrayNotFoundError):
                continue

    non_extracted = []
    for task in extraction_tasks:
        first_tile = task.tiles[0]  # we only need one for this check
        patch_constellation_path = (
            f"{storage_path}/{first_tile.id}/{task.constellation}"
        )
        dates = tile_constellation_sensing_times.get(patch_constellation_path)
        if dates is None or (task.sensing_time not in dates):
            non_extracted.append(task)
    return non_extracted


def create_tasks_by_splits(
    tiles: List[Tile],
    split_m: int,
    item_collection: Union[str, pystac.ItemCollection],
    constellations: List[str],
    bands: List[str] = None,
    interval: int = 1,
    n_jobs: int = -1,
    verbose: int = 0,
    overwrite: bool = False,
    storage_path: str = None,
    fs_mapper: Any = None,
) -> List[ExtractionTask]:
    """Group tiles in splits of given split_m size. It creates a task per split
    with the tiles contained by that split and the intersection with the
    stac items.
    An extraction task is created for each band listed as param (eo:bands extension otherwise)
    and for each revisit in the given date range


    Args:
        tiles (List[Tile]): The tiles to separate in zones
        split_m (int): the split square size in m,
        item_collection (Union[str, ItemCollection]): Path to geojson or pystac ItemCollectIon object
        bands (List[str]): the bands to extract
        interval (int): the day intervale between revisits
        n_jobs (int): n_jobs used by joblib
        verbos (int): verbose for joblib


    Returns:
        List[ExtractionTask]: List of extraction tasks ready to deploy
    """

    if not overwrite:
        if not fs_mapper:
            raise Exception("'fs_mapper' can't be None if 'overwrite' is set to False")

    logger.info("Loading items geojson...")
    if isinstance(item_collection, str):
        stac_items = pystac.ItemCollection.from_file(item_collection)
        gdf = gpd.GeoDataFrame.from_file(item_collection)
    else:  # stac_items is already an ItemCollection
        stac_items = item_collection
        gdf = gpd.GeoDataFrame.from_features(stac_items.to_dict())

    gdf.datetime = pd.to_datetime(gdf.datetime).dt.tz_localize(None)

    tiles_gdf = cluster_tiles_in_utm(tiles, split_m)

    logger.info("Creating extraction tasks for each constellations, date, and band ...")
    tasks: List[ExtractionTask] = []

    task_tracker = 0

    for constellation in constellations:

        # Get all the date ranges for the given interval
        dates = get_dates_in_range(
            gdf.loc[gdf.constellation == constellation, "datetime"]
            .min()
            .to_pydatetime(),
            gdf.loc[gdf.constellation == constellation, "datetime"]
            .max()
            .to_pydatetime(),
            interval,
        )

        if bands is not None:
            run_bands = [
                b["band"].name
                for kk, b in BAND_INFO[constellation].items()
                if b["band"].name in bands
            ]
        else:
            run_bands = [b["band"].name for kk, b in BAND_INFO[constellation].items()]

        logger.info(f"Getting cluster item indexes for {constellation} in parallel...")
        with tqdm_joblib(tqdm(desc="Extraction Tasks creation.", total=len(dates))):
            cluster_items = Parallel(n_jobs=n_jobs, verbose=verbose)(
                delayed(get_cluster_items_indexes)(
                    gdf[
                        (gdf.datetime >= start)
                        & (gdf.datetime <= end)
                        & (gdf.constellation == constellation)
                    ],
                    tiles_gdf,
                )
                for start, end in dates
            )

        for i, date_cluster_item in enumerate(cluster_items):
            for k, v in date_cluster_item.items():
                if v:
                    c_tiles = tiles_gdf[tiles_gdf["cluster_id"] == k]
                    c_items_geom = gdf.iloc[v].unary_union
                    t_indexes = c_tiles[
                        c_tiles.geometry.apply(c_items_geom.contains)
                    ].index
                    if not t_indexes.empty:
                        c_items = pystac.ItemCollection(
                            [stac_items.items[item_index] for item_index in v],
                        )
                        region_tiles = [tiles[t_index] for t_index in t_indexes]
                        sensing_time = dates[i][0]

                        for b in run_bands:
                            tasks.append(
                                ExtractionTask(
                                    task_id=str(task_tracker),
                                    tiles=region_tiles,
                                    item_collection=c_items,
                                    band=b,
                                    constellation=constellation,
                                    sensing_time=sensing_time,
                                ),
                            )
                            task_tracker += 1

    logger.info(f"There are a total of {len(tasks)} tasks")

    if not overwrite:
        logger.info(
            "Filtering already extracted tasks. Checking existing dates in storage...",
        )

        filtered_extraction_tasks = filter_already_extracted_tasks(
            fs_mapper,
            storage_path,
            tasks,
        )

        logger.info(
            f"{len(tasks) - len(filtered_extraction_tasks)} tasks were filtered because they already exists in storage",
        )
        tasks = filtered_extraction_tasks

    return tasks


def cluster_tiles_in_utm(tiles: List[Tile], split_m: int) -> gpd.GeoDataFrame:
    """Group tiles in splits of given split_m size.


    Args:
        tiles (List[Tile]): The tiles to separate in zones
        split_m (int): the split square size in m,

    Returns:
        gpd.GeoDataFrame: The resulting geopandas df of the tiles and their clusters
    """

    tiles_geom = gpd.GeoSeries([shapely.geometry.box(*t.bbox_wgs84) for t in tiles])

    # Split the tiles regions in UTM squares of size split_m
    logger.info("Creating multipolygon of the tiles geometries...")
    tiles_geom_multi = shapely.geometry.MultiPolygon(tiles_geom.values)
    splits = split_region_in_utm_tiles(
        tiles_geom_multi,
        crs=CRS.WGS84,
        bbox_size=split_m,
    )

    tiles_gdf = gpd.GeoDataFrame({"geometry": tiles_geom})
    for cluster_i, s in enumerate(splits):
        contained_tile_indexes = [i for i in range(len(tiles)) if s.contains(tiles[i])]
        tiles_gdf.loc[contained_tile_indexes, "cluster_id"] = cluster_i

    return tiles_gdf


def get_cluster_items_indexes(
    items_gdf: gpd.GeoDataFrame,
    tile_clusters: gpd.GeoDataFrame,
) -> dict:
    """Given an items geodataframe and a tile clusters geodataframe,
    return the items indexes that belong to each cluster.

    Args:
        items_gdf (gpd.GeoDataFrame): the items gdf
        tile_clusters (gpd.GeoDataFrame): the tile cluster gdf that should contain
                                          a cluster_id col

    Returns:
        dict: a dictionary where keys are clusters and values item indexes
    """
    cluster_item_indexes = {}
    clusters = tile_clusters.cluster_id.unique()
    for c in clusters:
        s_geom_bounds = shapely.geometry.MultiPolygon(
            tile_clusters[tile_clusters.cluster_id == c].geometry.values,
        )
        prep_geom = shapely.ops.prep(s_geom_bounds)
        s = items_gdf.geometry.apply(prep_geom.intersects)

        # Select the tiles for that cluster
        region_items = list(compress(items_gdf.index.values.tolist(), s))

        cluster_item_indexes[c] = region_items

    return cluster_item_indexes
