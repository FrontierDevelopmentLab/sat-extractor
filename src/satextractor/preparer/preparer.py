import datetime

import numpy as np
import zarr
from loguru import logger
from zarr.errors import ArrayNotFoundError
from zarr.errors import ContainsArrayError
from zarr.errors import ContainsGroupError
from zarr.errors import PathNotFoundError


def create_zarr_patch_structure(
    fs_mapper,
    storage_path,
    tile_id,
    patch_size,
    chunk_size,
    sensing_times,
    constellation,
    bands,
    overwrite,
):
    if not sensing_times.size == 0:
        patch_size_pixels = patch_size // min(b["gsd"] for _, b in bands.items())

        patch_constellation_path = f"{storage_path}/{tile_id}/{constellation}"
        zarr.open(
            fs_mapper(patch_constellation_path),
            mode="a",
        )  # make sure the path exists

        patch_path = f"{patch_constellation_path}/data"

        if overwrite:
            zarr.open_array(
                fs_mapper(patch_path),
                "w",
                shape=(
                    len(sensing_times),
                    len(bands),
                    int(patch_size_pixels),
                    int(patch_size_pixels),
                ),
                chunks=(1, 1, int(chunk_size), int(chunk_size)),
                dtype=np.uint16,
            )

            # Create timestamps array
            timestamps_path = f"{patch_constellation_path}/timestamps"
            z_dates = zarr.open_array(
                fs_mapper(f"{timestamps_path}"),
                mode="w",
                shape=(len(sensing_times)),
                chunks=(len(sensing_times)),
                dtype="<U27",
            )
            z_dates[:] = sensing_times

        else:

            # read current timestamps
            timestamps_path = f"{patch_constellation_path}/timestamps"

            try:
                existing_timestamps = zarr.open_array(fs_mapper(timestamps_path), "r")[
                    :
                ]
                existing_timestamps = np.array(
                    [
                        np.datetime64(datetime.datetime.fromisoformat(el))
                        for el in existing_timestamps
                    ],
                )

                max_existing = max(existing_timestamps)
                new_timesteps = np.array(sensing_times)[sensing_times > max_existing]

                if new_timesteps.size != sensing_times.size:
                    logger.warning(
                        f"""
                        Sat-Extractor can only append more recent data or overwrite existing data.
                        Maximum existing date for {timestamps_path} is {max_existing}
                        and minimum new_timestep is {min(sensing_times)}.
                        """,
                    )

                # get union of sensing times
                timestamps_union = np.union1d(existing_timestamps, sensing_times)

            except (PathNotFoundError, ContainsGroupError, ArrayNotFoundError):
                timestamps_union = sensing_times
            except Exception as e:
                raise e

            # write sensing times fresh
            z_dates = zarr.open_array(
                fs_mapper(f"{timestamps_path}"),
                mode="w",
                shape=(len(timestamps_union)),
                chunks=(len(timestamps_union)),
                dtype="<U27",
            )
            z_dates[:] = timestamps_union

            # resize any existing array based thereon

            # data
            try:
                # if it doesn't exist, create it.
                z_data = zarr.open_array(
                    fs_mapper(patch_path),
                    "w-",
                    shape=(
                        len(timestamps_union),
                        len(bands),
                        int(patch_size_pixels),
                        int(patch_size_pixels),
                    ),
                    chunks=(1, 1, int(chunk_size), int(chunk_size)),
                    dtype=np.uint16,
                )
            except ContainsArrayError:
                z_data = zarr.open_array(fs_mapper(patch_path), "r+")

                data_shape = z_data.shape
                z_data.resize(len(timestamps_union), *data_shape[1:])

            except Exception as e:
                raise e

            # masks
            mask_root_path = f"{patch_constellation_path}/mask"
            z_mask_dir = zarr.open_group(
                fs_mapper(mask_root_path),
                "a",
            )

            for mask_key in z_mask_dir.keys():

                mask_path = f"{mask_root_path}/{mask_key}"

                z_mask = zarr.open_array(
                    fs_mapper(mask_path),
                    "r+",
                )

                mask_shape = z_mask.shape

                z_mask.resize(len(timestamps_union), *mask_shape[1:])
