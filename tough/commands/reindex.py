from collections import defaultdict
from functools import partial
import glob
import json
import multiprocessing as mp
import os

from tqdm import tqdm

from .. import indexes
from ..commands.search import searcher
from ..config import INDEX_DIR, NUM_WORKERS
from ..eol_mapper import EOLMapper, chunkify
from ..utils import ensure_index_dir, get_datetime


def run_reindex(index_name):
    ensure_index_dir()

    pool = mp.Pool(NUM_WORKERS)

    index_eol_map(index_name, pool)
    index_datetime(index_name, pool)

    pool.close()
    pool.join()


def index_eol_map(index_name, pool):
    selected_indexes = indexes.items()
    if index_name:
        selected_indexes = [(index_name, indexes[index_name])]

    paths = []
    for _, index_conf in selected_indexes:
        paths.extend(
            glob.glob(os.path.join(index_conf["base_dir"], index_conf["pattern"]))
        )

    for _ in tqdm(pool.map(EOLMapper.map, paths), total=len(paths)):
        pass


def index_datetime(index_name, pool):
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

        postprocess = partial(get_datetime, index_name=index_name)
        func = partial(searcher, regex=None, substring=b"", postprocess=postprocess)
        chunks = list(chunkify(paths))
        results = defaultdict(lambda: defaultdict(list))

        for path, result in tqdm(pool.imap_unordered(func, chunks), total=len(chunks)):
            filename = path.replace(index_conf["base_dir"], "")
            for lineno, date in result:
                if len(results[date][filename]) < 2:
                    results[date][filename].append(lineno)
                else:
                    results[date][filename][0] = min(results[date][filename][0], lineno)
                    results[date][filename][1] = max(results[date][filename][1], lineno)

        json.dump(results, open(os.path.join(INDEX_DIR, index_name), "w"))
