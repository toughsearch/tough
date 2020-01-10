from functools import partial
import glob
import json
import multiprocessing as mp
import os
import re
import sys
from typing import Callable, List, Match, Optional, Tuple, Union

from tqdm import tqdm

from .. import get_indexes
from ..config import DATE_INDEX_NAME, INDEX_DIR, NUM_WORKERS
from ..eol_mapper import EOLMapper, chunkify
from ..opener import fopen
from ..utils import date_range


def searcher(
    chunk: Tuple[str, int, int, int],
    regex: bool,
    substring: bytes,
    index_name: str,
) -> Tuple[str, List[Tuple[int, bytes]]]:
    path, line_start, length, line_end = chunk
    mapper = EOLMapper(path, index_name)
    results = []

    check: Union[
        Callable[[bytes], bool],
        Callable[[bytes, int, int], Optional[Match[bytes]]],
    ]
    check = lambda x: substring in x  # noqa
    if regex:
        check = re.compile(substring).search

    with fopen(path, index_name) as f:
        m = mapper.read(line_start)
        if not m:
            raise IOError
        f.seek(m.offset)
        chunk_line_end = line_start + length
        if chunk_line_end > line_end:
            chunk_line_end = line_end + 1
        for lineno in range(line_start, chunk_line_end):
            line = f.readline()
            if chunk_line_end == line_end + 1 and not line:
                break

            if not check(line):
                continue

            result_line = line.strip()
            results.append((lineno, result_line))

    return path, results


def run_search(
    substring: str,
    regex: bool,
    index: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> None:
    if not substring:
        sys.stderr.write("Please provide substring\n")
        return

    indexes = get_indexes()
    index_conf = indexes[index]
    index_data = json.load(
        open(os.path.join(INDEX_DIR, index, DATE_INDEX_NAME))
    )

    to_search: List[Tuple[str, Optional[Tuple]]] = []
    if not date_from or not date_to:
        to_search = [
            (x, None)
            for x in glob.glob(
                os.path.join(index_conf["base_dir"], index_conf["pattern"])
            )
        ]

    else:
        for d in date_range(date_from, date_to):
            if d not in index_data:
                continue
            for filename, lines_range in index_data[d].items():
                to_search.append(
                    (
                        os.path.join(
                            index_conf["base_dir"], filename.strip("/")
                        ),
                        lines_range,
                    )
                )

    func = partial(
        searcher, substring=substring.encode(), regex=regex, index_name=index
    )
    chunks = list(chunkify(to_search, index))
    pool = mp.Pool(NUM_WORKERS)

    try:
        for _, result in tqdm(pool.imap(func, chunks), total=len(chunks)):
            sys.stdout.write("\n".join(x[1].decode() for x in result) + "\n")
    finally:
        pool.close()
        pool.join()
