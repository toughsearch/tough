import json

from tough.commands.reindex import run_reindex
from tough.config import DATE_INDEX_NAME, INDEX_DIR

expected_index = {
    "2019-02-20": {"access_log.2.gz": [0, 9]},
    "2019-02-21": {"access_log.2.gz": [10, 109], "access_log.1": [0, 9]},
    "2019-02-22": {"access_log.1": [10, 109], "access_log": [0, 9]},
    "2019-02-23": {"access_log": [10, 109]},
}


def test_reindex_empty(index_name):
    run_reindex(index_name)
    assert not (INDEX_DIR / index_name / DATE_INDEX_NAME).is_file()


def test_reindex_data(provide_data, index_name):
    run_reindex(index_name)
    actual_index = json.load(open(INDEX_DIR / index_name / DATE_INDEX_NAME))
    assert actual_index == expected_index


def test_reindex_all(provide_data, index_name):
    run_reindex("")
    actual_index = json.load(open(INDEX_DIR / index_name / DATE_INDEX_NAME))
    assert actual_index == expected_index


def test_reindex_twice(provide_data, index_name):
    run_reindex("")
    run_reindex("")
    actual_index = json.load(open(INDEX_DIR / index_name / DATE_INDEX_NAME))
    assert actual_index == expected_index
