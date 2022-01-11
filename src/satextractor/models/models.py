from __future__ import annotations

import datetime
from typing import List
from typing import Tuple

import attr
import pystac
from satextractor.utils import get_transform_function


@attr.s(frozen=True)
class Tile:
    zone: int = attr.ib()
    row: str = attr.ib()
    min_x: int = attr.ib()
    min_y: int = attr.ib()
    max_x: int = attr.ib()
    max_y: int = attr.ib()
    epsg: str = attr.ib()

    @property
    def id(self) -> str:
        return f"{self.zone}_{self.row}_{self.bbox_size_x}_{self.xloc}_{self.yloc}"

    @property
    def xloc(self) -> int:
        return int(self.min_x / self.bbox_size_x)

    @property
    def yloc(self) -> int:
        return int(self.min_y / self.bbox_size_y)

    @property
    def bbox(self) -> Tuple[float, float, float, float]:
        return (self.min_x, self.min_y, self.max_x, self.max_y)

    @property
    def bbox_wgs84(self):
        reproj_src_wgs = get_transform_function(str(self.epsg), "WGS84")
        return (
            *reproj_src_wgs(self.min_x, self.min_y),
            *reproj_src_wgs(self.max_x, self.max_y),
        )

    @property
    def bbox_size_x(self) -> int:  # in metres
        return int(self.max_x - self.min_x)

    @property
    def bbox_size_y(self) -> int:  # in metres
        return int(self.max_y - self.min_y)

    @property
    def bbox_size(self) -> Tuple[int, int]:  # in metres
        return (self.bbox_size_x, self.bbox_size_y)

    def contains(self, other: Tile) -> bool:
        return (
            self.epsg == other.epsg
            and self.bbox[0] <= other.bbox[0]
            and self.bbox[1] <= other.bbox[1]
            and self.bbox[2] >= other.bbox[2]
            and self.bbox[3] >= other.bbox[3]
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
