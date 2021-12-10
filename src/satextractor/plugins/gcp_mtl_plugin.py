from typing import List

from gcsfs import GCSFileSystem
from joblib import delayed
from joblib import Parallel
from satextractor.models import ExtractionTask
from satextractor.utils import tqdm_joblib
from tqdm import tqdm


def copy_if_exists(fs, src, dst):
    if fs.exists(src):
        fs.copy(src, dst)


def copy_mtl_files(
    credentials: str,
    tasks: List[ExtractionTask],
    storage_path: str,
    n_jobs: int = -1,
) -> bool:
    fs = GCSFileSystem(token=credentials)
    mtl_files_info = set(
        [
            (
                task.constellation,
                tile.id,
                str(task.sensing_time)[:10],
                task.item_collection[0].assets["B1"].href.replace("B1.TIF", "MTL.txt"),
            )
            for task in tasks
            for tile in task.tiles
        ],
    )

    to_copy = []
    for mtl_info in mtl_files_info:
        src = mtl_info[3]
        dst = (
            f"{storage_path}/{mtl_info[1]}/{mtl_info[0]}/metadata/{mtl_info[2]}_MTL.txt"
        )
        to_copy.append((src, dst))

    with tqdm_joblib(
        tqdm(
            desc=f"parallel copying MTL files on {storage_path}",
            total=len(to_copy),
        ),
    ):
        Parallel(n_jobs=n_jobs, verbose=0, prefer="threads")(
            [delayed(copy_if_exists)(fs, src, dst) for src, dst in to_copy],
        )

    return True
