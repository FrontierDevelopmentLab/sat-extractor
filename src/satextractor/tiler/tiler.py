from typing import List
from typing import Tuple
from typing import Union

import shapely
from satextractor.models import Tile
from sentinelhub import CRS
from sentinelhub import UtmGridSplitter


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
    utm_splitter = UtmGridSplitter([region], crs, bbox_size)
    crs_bboxes = utm_splitter.get_bbox_list()
    info_bboxes = utm_splitter.get_info_list()

    assert (
        bbox_size[0] == bbox_size[1]
    ), "bbox_size sides should be equal size, i.e. square"
    size = bbox_size[0]

    tiles = []
    for info, box in zip(info_bboxes, crs_bboxes):
        # tile ids are globally unique and take the format shown below
        zone = info["utm_zone"]
        row = info["utm_row"]
        x, y = (int(v / size) for v in box.lower_left)
        tiles.append(
            Tile(
                id=f"{zone}_{row}_{size}_{x}_{y}",
                epsg=box.crs.epsg,
                bbox=(box.min_x, box.min_y, box.max_x, box.max_y),
            ),
        )

    return tiles
