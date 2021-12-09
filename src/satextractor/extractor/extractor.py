import os
from typing import Any
from typing import List
from typing import Tuple

import numpy as np
import rasterio
from affine import Affine
from loguru import logger
from rasterio import warp
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.merge import merge as riomerge
from satextractor.models import ExtractionTask
from satextractor.models import Tile


def get_window_union(
    tiles: List[Tile],
    ds: rasterio.io.DatasetReader,
) -> rasterio.windows.Window:

    """Get the window union to read all tiles from the geotiff.

    Args:
        tiles (List[Tile]): the tiles
        ds (rasterio.io.DatasetReader): the rasterio dataset to read (for the transform)

    Returns:
        rasterio.windows.Window: The union of all tile windows.
    """

    windows = []

    for tile in tiles:

        bounds_arr_tile_crs = tile.bbox
        bounds_arr_rast_crs = warp.transform_bounds(
            CRS.from_epsg(tile.epsg),
            ds.crs,
            *bounds_arr_tile_crs,
        )

        window = rasterio.windows.from_bounds(*bounds_arr_rast_crs, ds.transform)

        windows.append(window)

    return rasterio.windows.union(windows)


def get_proj_win(tiles: List[Tile]) -> Tuple[int, int, int, int]:
    """Get the projection bounds window of the tiles.

    Args:
        tiles (List[Tile]): the tiles

    Returns:
        [Tuple[int, int, int, int]]: upperleft_x,upperleft_y,lowerright_x,lowerright_y
    """
    ulx = min([t.bbox[0] for t in tiles])
    uly = max([t.bbox[3] for t in tiles])
    lrx = max([t.bbox[2] for t in tiles])
    lry = min([t.bbox[1] for t in tiles])
    return int(ulx), int(uly), int(lrx), int(lry)


def get_tile_pixel_coords(tiles: List[Tile], raster_file: str) -> List[Tuple[int, int]]:
    """Get the tile coord in pixels for the given file. Returns

    Args:
        tiles (List[Tile]): tile list
        file (str): the raster_file to get the coords from

    Returns:
        List[Tuple[int, int]]: The coords in pixels
    """
    xs, ys = zip(*[(tile.bbox[0], tile.bbox[3]) for tile in tiles])

    with rasterio.open(raster_file) as ds:
        rows, cols = rasterio.transform.rowcol(ds.transform, xs, ys)

    return list(zip(rows, cols))


def download_and_extract_tiles_window(
    fs: Any,
    task: ExtractionTask,
    resolution: int,
) -> List[str]:
    """Download and extract from the task assets the data for the window from each asset.

    Args:
        task (ExtractionTask): The extraction task
        resolution (int): The target resolution

    Returns:
        List[str]: A list of files that store the crops of the original assets
    """

    # task tiles all have same CRS, so get their max extents and crs
    left, top, right, bottom = get_proj_win(task.tiles)
    epsg = task.tiles[0].epsg

    # set the transforms for the output file
    dst_transform = Affine(resolution, 0.0, left, 0.0, -resolution, top)
    out_shp = (int((right - left) / resolution), int((top - bottom) / resolution))

    outfiles = []

    band = task.band
    urls = [item.assets[band].href for item in task.item_collection.items]

    for ii, url in enumerate(urls):
        with fs.open(url) as f:
            with rasterio.open(f) as ds:
                window = get_window_union(task.tiles, ds)

                if task.band == "BQA":
                    resampling = Resampling.nearest
                else:
                    resampling = Resampling.bilinear

                rst_arr = ds.read(
                    1,
                    window=window,
                    out_shape=out_shp,
                    fill_value=0,
                    boundless=True,
                    resampling=resampling,
                )

        out_f = f"{task.task_id}_{ii}.tif"

        with rasterio.open(
            out_f,
            "w",
            driver="GTiff",
            count=1,
            width=out_shp[0],
            height=out_shp[1],
            transform=dst_transform,
            crs=CRS.from_epsg(epsg),
            dtype=rst_arr.dtype,
        ) as dst:

            dst.write(rst_arr, indexes=1)

        outfiles.append(out_f)

    return outfiles


def task_mosaic_patches(
    cloud_fs: Any,
    task: ExtractionTask,
    method: str = "max",
    resolution: int = 10,
    dst_path="merged.jp2",
) -> List[np.ndarray]:
    """Get tile patches from the mosaic of a given task

    Args:
        cloud_fs (Any): the cloud_fs to access the files
        task (ExtractionTask): The task
        method (str, optional): The method to use while merging the assets. Defaults to "max".
        resolution (int, optional): The target resolution. Defaults to 10.
        dst_path (str): path to store the merged files

    Returns:
        List[np.ndarray]: The tile patches as numpy arrays
    """

    out_files = download_and_extract_tiles_window(cloud_fs, task, resolution)

    out_f = f"{task.task_id}_{dst_path}"
    datasets = [rasterio.open(f) for f in out_files]
    riomerge(
        datasets,
        method=method,
        dst_path=out_f,
        dst_kwds={
            "QUALITY": "100",
            "REVERSIBLE": "YES",
        },
    )

    coords = get_tile_pixel_coords(task.tiles, out_f)
    patches = []
    bboxes = [t.bbox_size for t in task.tiles]

    with rasterio.open(out_f) as ds:
        # Loop through your list of coords
        for i, (py, px) in enumerate(coords):
            # Build a window
            w = ds.read(
                1,
                window=rasterio.windows.Window(
                    px,
                    py,
                    int(bboxes[i][0]) // resolution,
                    int(bboxes[i][1]) // resolution,
                ),
            )
            patches.append(w)

    logger.info("Cleaning files.")
    for f in out_files:
        os.remove(f)
    os.remove(out_f)
    return patches
