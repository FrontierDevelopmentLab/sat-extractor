import datetime
from typing import List
from typing import Tuple

import attr
import pystac
from satextractor.utils import get_transform_function


@attr.s
class Tile:
    id: str = attr.ib()
    epsg: str = attr.ib()
    bbox: Tuple[float, float, float, float] = attr.ib()  # (xmin, ymin, xmax, ymax)

    def __attrs_post_init__(self):
        self.bbox_size = (
            self.bbox[2] - self.bbox[0],
            self.bbox[3] - self.bbox[1],
        )

    def contains(self, other):
        # type: (Tile)->bool
        return (
            self.epsg == other.epsg
            and self.bbox[0] <= other.bbox[0]
            and self.bbox[1] <= other.bbox[1]
            and self.bbox[2] >= other.bbox[2]
            and self.bbox[3] >= other.bbox[3]
        )

    @property
    def bbox_wgs84(self):
        reproj_src_wgs = get_transform_function(str(self.epsg), "WGS84")
        return (
            *reproj_src_wgs(self.bbox[0], self.bbox[1]),
            *reproj_src_wgs(self.bbox[2], self.bbox[3]),
        )


@attr.s
class ExtractionTask:
    """Extraction task class

    Args:
        task_id (str): the task id
        tiles (List[Tile]): the tiles to extract
        item_collection (pystac.ItemCollection): the item collection with the assets
        band (str): the band to extract
        constellation (str): the satellite constellation from which to extract
        sensing_time (datetime.datetime): the assets starting sensing_time
    """

    task_id: str = attr.ib()
    tiles: List[Tile] = attr.ib()
    item_collection: pystac.ItemCollection = attr.ib()
    band: str = attr.ib()
    constellation: str = attr.ib()
    sensing_time: datetime.datetime = attr.ib()

    def serialize(self):
        serialized_task = attr.asdict(self)
        serialized_task["item_collection"] = serialized_task[
            "item_collection"
        ].to_dict()
        return serialized_task
