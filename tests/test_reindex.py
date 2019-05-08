import json
import os

from tough.commands.reindex import run_reindex
from tough.config import DATE_INDEX_NAME, INDEX_DIR

INDEX_NAME = "access_log"
DATA_DIR = "tests/data"

expected_index = {
    "2019-02-20": {"access_log.2.gz": [0, 9]},
    "2019-02-21": {"access_log.2.gz": [10, 109], "access_log.1": [0, 9]},
    "2019-02-22": {"access_log.1": [10, 109], "access_log": [0, 9]},
    "2019-02-23": {"access_log": [10, 109]},
}


def test_reindex_empty():
    run_reindex(INDEX_NAME)
    assert not os.path.isfile(os.path.join(INDEX_DIR, INDEX_NAME, DATE_INDEX_NAME))


def test_reindex_data(provide_data):
    run_reindex(INDEX_NAME)
    actual_index = json.load(open(os.path.join(INDEX_DIR, INDEX_NAME, DATE_INDEX_NAME)))
    assert actual_index == expected_index


def test_reindex_all(provide_data):
    run_reindex("")
    actual_index = json.load(open(os.path.join(INDEX_DIR, INDEX_NAME, DATE_INDEX_NAME)))
    assert actual_index == expected_index


def test_reindex_twice(provide_data):
    run_reindex("")
    run_reindex("")
    actual_index = json.load(open(os.path.join(INDEX_DIR, INDEX_NAME, DATE_INDEX_NAME)))
    assert actual_index == expected_index
