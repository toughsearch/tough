import argparse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from functools import partial
import glob
import json
import multiprocessing as mp
import os
import re
import sys

from tqdm import tqdm
import yaml

from tough.config import NUM_WORKERS, MIN_CHUNK_LENGTH, CONF_NAME
from . import EOLMapper
from .config import INDEX_DIR
from .indexer import eol_map
from .utils import date_range, fopen, ensure_index_dir

try:
    indexes = yaml.load(open(CONF_NAME), Loader=yaml.FullLoader)
except FileNotFoundError:
    indexes = {}


def nginx_get_datetime(row):
    fmt = "%d/%b/%Y:%H:%M:%S %z"
    raw_datetime = row.split(b"[", maxsplit=1)[1].split(b"]", maxsplit=1)[0]
    dt = datetime.strptime(raw_datetime.decode(), fmt)
    return str(dt.astimezone(timezone.utc).date())


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


def chunkify(to_search, min_chunk_length=MIN_CHUNK_LENGTH):
    for path, lines_range in to_search:
        lines_from = 0
        lines_to = EOLMapper(path).count_lines()
        if lines_range is not None:
            # TODO: Single range case
            lines_from, lines_to = lines_range
        lines = lines_to - lines_from
        length = max(round(lines / (NUM_WORKERS * 4)), min_chunk_length)
        for line_start in range(lines_from, lines_to, length):
            yield path, line_start, length, lines_to


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


def run_reindex(index_name):
    index_eol_map(index_name)
    index_datetime(index_name)


def run():
    ensure_index_dir()
    main_parser = argparse.ArgumentParser(description="ToughSearch")

    subparsers = main_parser.add_subparsers(dest="command")

    index_parser = subparsers.add_parser("reindex", help="Reindex using conf.yaml")
    index_parser.add_argument(
        "index_name", help="Index name to reindex", default="", nargs="?"
    )

    search_parser = subparsers.add_parser("search", help="Searcher")
    search_parser.add_argument(
        "substring", help="Substring to search", default="", nargs="?"
    )
    search_parser.add_argument("index_name", help="Index to search")
    search_parser.add_argument("-e", "--regex", help="Regex pattern")
    search_parser.add_argument("-df", "--date-from")
    search_parser.add_argument("-dt", "--date-to")

    args = main_parser.parse_args()
    dict_args = args.__dict__

    commands = {"search": run_search, "reindex": run_reindex}

    commands[dict_args.pop("command")](**dict_args)


if __name__ == "__main__":
    run()
