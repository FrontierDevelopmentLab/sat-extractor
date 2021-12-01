import numpy as np
import zarr


def create_zarr_patch_structure(
    fs_mapper,
    storage_path,
    tile_id,
    patch_size,
    chunk_size,
    sensing_times,
    constellation,
    bands,
):
    if not sensing_times.size == 0:
        patch_size_pixels = patch_size // min(b["gsd"] for _, b in bands.items())

        patch_constellation_path = f"{storage_path}/{tile_id}/{constellation}"
        zarr.open(fs_mapper(patch_constellation_path), mode="a")

        patch_path = f"{patch_constellation_path}/data"
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
