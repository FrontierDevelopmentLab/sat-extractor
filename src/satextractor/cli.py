"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -msatextractor` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``satextractor.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``satextractor.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import os
import pickle

import geopandas as gpd
import hydra
from loguru import logger
from omegaconf import DictConfig


def build(cfg):
    logger.info(f"using {cfg.builder._target_} builder")

    hydra.utils.call(cfg.builder, cfg)


def stac(cfg):
    logger.info(f"using {cfg.stac._target_} stac creator.")

    if os.path.exists(cfg.item_collection):
        logger.info(
            f"stac item_collection already exists in {cfg.item_collection}. Skipping.",
        )
        return

    gdf = gpd.read_file(cfg.gpd_input)
    shp = gdf.unary_union

    item_collection = hydra.utils.call(
        cfg.stac,
        credentials=cfg.credentials,
        region=shp,
        start_date=cfg.start_date,
        end_date=cfg.end_date,
        constellations=cfg.constellations,
    )
    item_collection.save_object(cfg.item_collection)


def tiler(cfg):

    logger.info(f"using {cfg.tiler._target_} tiler")

    if os.path.exists(cfg.tiles):
        logger.info(f"Tilers already exists in {cfg.tiles}. Skipping.")
        return

    logger.info(f"loading vector file {cfg.gpd_input} and reducing geometries")
    gdf = gpd.read_file(cfg.gpd_input)
    shp = gdf.unary_union

    logger.info(cfg.tiler)

    tiles = hydra.utils.instantiate(cfg.tiler, shp)

    logger.info(f"Generated tile patches: {len(tiles)}")

    with open(cfg.tiles, "wb") as f:
        pickle.dump(tiles, f)


def scheduler(cfg):

    logger.info(f"using {cfg.scheduler._target_} scheduler")

    if os.path.exists(cfg.extraction_tasks):
        logger.info(
            f"Extraction tasks already exists in {cfg.extraction_tasks}. Skipping.",
        )
        return

    logger.info("Loading tiles and generating tasks")
    with open(cfg.tiles, "rb") as f:
        tiles = pickle.load(f)

    # Get the schedule function from a function dict
    # We have to do it this way, because Hydra converts dataclasses and attr classes to configs
    # And cannot be passed as arguments as-is
    schedule_f = hydra.utils.call(cfg.scheduler)
    extraction_tasks = schedule_f(
        tiles=tiles,
        item_collection=cfg.item_collection,
        constellations=cfg.constellations,
        **cfg.scheduler,
    )

    logger.info(f"Generated Extraction Tasks: {len(extraction_tasks)}")

    with open(cfg.extraction_tasks, "wb") as f:
        pickle.dump(extraction_tasks, f)


def preparer(cfg):

    logger.info(f"using {cfg.preparer._target_} to prepare zarr archives")

    extraction_tasks = pickle.load(open(cfg.extraction_tasks, "rb"))
    tiles = pickle.load(open(cfg.tiles, "rb"))

    hydra.utils.call(
        cfg.preparer,
        cfg.credentials,
        extraction_tasks,
        tiles,
        cfg.constellations,
        f"{cfg.cloud.storage_prefix}/{cfg.cloud.storage_root}/{cfg.dataset_name}",
        cfg.tiler.bbox_size,
    )


def deployer(cfg):
    logger.info(f"using {cfg.deployer._target_} deployer")

    extraction_tasks = pickle.load(open(cfg.extraction_tasks, "rb"))

    topic = f"projects/{cfg.cloud.project}/topics/{'-'.join([cfg.cloud.user_id, 'stacextractor'])}"

    hydra.utils.call(
        cfg.deployer,
        cfg.credentials,
        extraction_tasks,
        f"{cfg.cloud.storage_prefix}/{cfg.cloud.storage_root}/{cfg.dataset_name}",
        cfg.preparer.chunk_size,
        topic,
    )

    # extraction_tasks_path = os.path.join(
    #     ".", cfg.dataset_name + "_extraction_tasks.pkl",
    # )

    # logger.info(f"deploying on {cfg.deployer._target_} to {cfg.cloud.storage_root}",)

    # extraction_tasks = pickle.load(open(extraction_tasks_path, "rb"))

    # # check tiles meet spec
    # for t in extraction_tasks:
    #     assert isinstance(t, ExtractionTask), "Task does not match ExtractionTask spec"

    # hydra.utils.instantiate(cfg.deployer, extraction_tasks)


@hydra.main(config_path="./../../conf", config_name="config")
def main(cfg: DictConfig):
    """
    Args:
        cfg: a dict config object

    Returns:
        int: A return code

    Does stuff.
    """

    for t in cfg.tasks:
        assert t in [
            "build",
            "stac",
            "tile",
            "schedule",
            "prepare",
            "deploy",
        ], "valid tasks are [build, stac, tile, schedule, prepare, deploy]"

    logger.info(f"Running tasks {cfg.tasks}")

    if "build" in cfg.tasks:
        build(cfg)

    if "stac" in cfg.tasks:
        stac(cfg)

    if "tile" in cfg.tasks:
        tiler(cfg)

    if "schedule" in cfg.tasks:
        scheduler(cfg)

    if "prepare" in cfg.tasks:
        preparer(cfg)

    if "deploy" in cfg.tasks:
        deployer(cfg)

    return 0


if __name__ == "__main__":
    main()
