import os
from typing import Any
from typing import Callable
from typing import List
from typing import Tuple

import numpy as np
import rasterio
from loguru import logger
from osgeo import gdal
from osgeo import osr
from rasterio.merge import merge as riomerge
from satextractor.models import ExtractionTask
from satextractor.models import Tile


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
    download_f: Callable,
    task: ExtractionTask,
    resolution: int,
) -> List[str]:
    """Download and extract from the task assets the window bounding the tiles.
    i.e a crop of the original assets will

    Args:
        download_f (Callable): The download function to use. It should return a BytesIO
                               to read the content.
        task (ExtractionTask): The extraction task
        resolution (int): The target resolution

    Returns:
        List[str]: A list of files that store the crops of the original assets
    """
    band = task.band
    urls = [item.assets[band].href for item in task.item_collection.items]

    epsg = task.tiles[0].epsg
    out_files = []
    for i, url in enumerate(urls):
        content = download_f(url)

        gdal.FileFromMemBuffer(f"/vsimem/{task.task_id}_content", content.read())
        d = gdal.Open(f"/vsimem/{task.task_id}_content", gdal.GA_Update)

        proj = osr.SpatialReference(wkt=d.GetProjection())
        proj = proj.GetAttrValue("AUTHORITY", 1)
        d = None

        proj_win = get_proj_win(task.tiles)

        if int(proj) != epsg:
            file = gdal.Warp(
                f"{task.task_id}_warp.vrt",
                f"/vsimem/{task.task_id}_content",
                dstSRS=f"EPSG:{epsg}",
                creationOptions=["QUALITY=100", "REVERSIBLE=YES"],
            )
        else:
            file = f"/vsimem/{task.task_id}_content"

        out_f = f"{task.task_id}_{i}.jp2"
        gdal.Translate(
            out_f,
            file,
            projWin=proj_win,
            projWinSRS=f"EPSG:{epsg}",
            xRes=resolution,
            yRes=-resolution,
            resampleAlg="bilinear",
            creationOptions=["QUALITY=100", "REVERSIBLE=YES"],
        )
        file = None
        out_files.append(out_f)
    return out_files


def task_mosaic_patches(
    cloud_fs: Any,
    download_f: Callable,
    task: ExtractionTask,
    method: str = "first",
    resolution: int = 10,
    dst_path="merged.jp2",
) -> List[np.ndarray]:
    """Get tile patches from the mosaic of a given task

    Args:
        download_f (Callable): The function to download the task assets
        task (ExtractionTask): The task
        method (str, optional): The method to use while merging the assets. Defaults to "first".
        resolution (int, optional): The target resolution. Defaults to 10.
        dst_path (str): path to store the merged files

    Returns:
        List[np.ndarray]: The tile patches as numpy arrays
    """
    out_files = download_and_extract_tiles_window(download_f, task, resolution)

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
