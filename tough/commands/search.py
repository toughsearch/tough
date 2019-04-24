from concurrent.futures import ProcessPoolExecutor
from functools import partial
import glob
import json
import os
import re
import sys

from tqdm import tqdm

from .. import indexes
from ..config import INDEX_DIR, NUM_WORKERS
from ..eol_mapper import EOLMapper, chunkify
from ..utils import date_range, fopen


def searcher(chunk, regex, substring, postprocess):
    path, line_start, length, line_end = chunk
    mapper = EOLMapper(path)
    results = []

    check = lambda x: substring in x
    if regex is not None:
        check = regex.search

    with fopen(path) as f:
        m = mapper.read(line_start)
        f.seek(m.offset)
        line_end = min(line_start + length, line_end)
        for lineno in range(line_start, line_end):
            line = f.readline()
            if check(line):
                result_line = line.strip()
                if postprocess:
                    try:
                        result_line = postprocess(result_line)
                    except Exception as e:
                        print("%r" % e)
                        raise
                results.append((lineno, result_line))

    return path, results


def run_search(
    substring, regex, index_name, date_from=None, date_to=None, postprocess=None
):
    if not substring and not regex:
        sys.stderr.write("Please provide substring or --regex (-e) parameter\n")
        return

    index_conf = indexes[index_name]
    index_data = json.load(open(os.path.join(INDEX_DIR, index_name)))

    to_search = []
    if not date_from and not date_to:
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
                        os.path.join(index_conf["base_dir"], filename.strip("/")),
                        lines_range,
                    )
                )

    func = partial(
        searcher,
        regex=re.compile(regex.encode()) if regex else None,
        substring=substring.encode(),
        postprocess=postprocess,
    )
    chunks = list(chunkify(to_search))
    with ProcessPoolExecutor(NUM_WORKERS) as executor:
        for path, result in tqdm(executor.map(func, chunks), total=len(chunks)):
            sys.stdout.write("\n".join(x[1].decode() for x in result))
