from typing import List
from typing import Tuple
from typing import Union

import shapely
from satextractor.models import Tile
from sentinelhub import CRS
from sentinelhub import UtmZoneSplitter


def split_region_in_utm_tiles(
    region: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    crs: CRS = CRS.WGS84,
    bbox_size: Tuple[int, int] = (10000, 10000),
    **kwargs,
) -> List[Tile]:
    """Split a given geometry in squares measured in meters.
    It splits the region in utm grid and the convert back to given crs.

    Args:
        region (UnionList[shapely.geometry.Polygon, shapely.geometry.MultiPolygon]): The region to split from
        bbox_size (Tuple[int, int]): square bbox in meters

    Returns:
        [List[Tile]]: The Tiles representing each of the boxes
    """
    utm_zone_splitter = UtmZoneSplitter([region], crs, bbox_size)
    crs_bboxes = utm_zone_splitter.get_bbox_list()
    info_bboxes = utm_zone_splitter.get_info_list()

    tiles = [
        Tile(
            id="_".join(
                [str(box.crs.epsg), str(info["index_x"]), str(info["index_y"])],
            ),
            epsg=box.crs.epsg,
            bbox=(box.min_x, box.min_y, box.max_x, box.max_y),
        )
        for info, box in zip(info_bboxes, crs_bboxes)
    ]

    return tiles
