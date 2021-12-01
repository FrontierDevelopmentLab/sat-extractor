from typing import List
from typing import Union

import shapely
from satextractor.models import Tile
from sentinelhub import CRS
from sentinelhub import UtmGridSplitter


def split_region_in_utm_tiles(
    region: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    crs: CRS = CRS.WGS84,
    bbox_size: int = 10000,
    **kwargs,
) -> List[Tile]:
    """Split a given geometry in squares measured in meters.
    It splits the region in utm grid and the convert back to given crs.

    Args:
        region (UnionList[shapely.geometry.Polygon, shapely.geometry.MultiPolygon]): The region to split from
        bbox_size (int): bbox size in meters

    Returns:
        [List[Tile]]: The Tiles representing each of the boxes
    """
    utm_splitter = UtmGridSplitter([region], crs, bbox_size)
    crs_bboxes = utm_splitter.get_bbox_list()
    info_bboxes = utm_splitter.get_info_list()

    tiles = [
        Tile(
            zone=info["utm_zone"],
            row=info["utm_row"],
            min_x=box.min_x,
            min_y=box.min_y,
            max_x=box.max_x,
            max_y=box.max_y,
            epsg=box.crs.epsg,
        )
        for info, box in zip(info_bboxes, crs_bboxes)
    ]

    return tiles
