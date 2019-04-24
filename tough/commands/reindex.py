from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import glob
import json
import multiprocessing as mp
import os

from tqdm import tqdm

from .. import indexes
from ..commands.search import searcher
from ..config import INDEX_DIR, NUM_WORKERS
from ..eol_mapper import chunkify, eol_map
from ..utils import nginx_get_datetime


def run_reindex(index_name):
    index_eol_map(index_name)
    index_datetime(index_name)


def index_eol_map(index_name):
    selected_indexes = indexes.items()
    if index_name:
        selected_indexes = [(index_name, indexes[index_name])]

    paths = []
    for index_name, index_conf in selected_indexes:
        paths.extend(
            glob.glob(os.path.join(index_conf["base_dir"], index_conf["pattern"]))
        )

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for _ in tqdm(executor.map(eol_map, paths), total=len(paths)):
            pass


def index_datetime(index_name):
    selected_indexes = indexes.items()
    if index_name:
        selected_indexes = [(index_name, indexes[index_name])]

    for index_name, index_conf in selected_indexes:
        paths = [
            (x, None)
            for x in glob.glob(
                os.path.join(index_conf["base_dir"], index_conf["pattern"])
            )
        ]

        func = partial(
            searcher, regex=None, substring=b"", postprocess=nginx_get_datetime
        )
        chunks = list(chunkify(paths))
        results = defaultdict(lambda: defaultdict(list))
        with mp.Pool(NUM_WORKERS) as pool:
            for path, result in tqdm(
                pool.imap_unordered(func, chunks), total=len(chunks)
            ):
                filename = path.replace(index_conf["base_dir"], "")
                for lineno, date in result:
                    if len(results[date][filename]) < 2:
                        results[date][filename].append(lineno)
                    else:
                        results[date][filename][0] = min(
                            results[date][filename][0], lineno
                        )
                        results[date][filename][1] = max(
                            results[date][filename][1], lineno
                        )

        json.dump(results, open(os.path.join(INDEX_DIR, index_name), "w"))
