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
    overwrite,
):
    if not sensing_times.size == 0:
        patch_size_pixels = patch_size // min(b["gsd"] for _, b in bands.items())

        patch_constellation_path = f"{storage_path}/{tile_id}/{constellation}"
        zarr.open(fs_mapper(patch_constellation_path), mode="a") # make sure the path exists

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
            existing_timestamps = zarr.open(fs_mapper(timestamps_path))[:]
            
            assert max(existing_timestamps)<=min(sensing_times), 'Sat-Extractor can only append more recent data. '
            
            # get union of sensing times
            timestamps_union = np.union1d(existing_timestamps,sensing_times)

            # write sensing times fresh
            z_dates = zarr.open_array(
                fs_mapper(f"{timestamps_path}"),
                mode="w",
                shape=(len(sensing_times)),
                chunks=(len(sensing_times)),
                dtype="<U27",
            )
            z_dates[:] = sensing_times

            # resize any existing array based thereon
            
            # data
            z_data = zarr.open_array(
                fs_mapper(patch_path),
                "a",
            )
            z_data.resize(
                len(timestamps_union),
                len(bands),
                int(patch_size_pixels),
                int(patch_size_pixels),
            )
            
            # masks
            mask_root_path = f"{patch_constellation_path}/mask"
            z_mask_dir = zarr.open(
                fs_mapper(patch_path),
                "a",
            )
            
            for mask_key in z_mask_dir.keys():
                
                mask_path = f"{mask_root_path}/{mask_key}"
                
                z_mask = zarr.open_array(
                    fs_mapper(mask_path),
                    "a",
                )   
                
                mask_shape = z_mask.shape
                
                z_mask.resize(len(timestamps_union), *mask_shape[1:])
                
                
            
