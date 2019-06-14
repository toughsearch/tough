import json

from tough.commands import reindex
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


def test_reindex_only_changed_file(provide_data, index_name, data_dir, monkeypatch):
    run_reindex(index_name)

    fmt = (
        '127.0.0.1 - - [{date}:12:34:56 +0000] "GET /{url} HTTP/1.1" 404 233 "-" "-"\n'
    )

    files_dir = data_dir / index_name
    with open(files_dir / index_name, "a") as f:
        f.write(fmt.format(date="25/Feb/2019", url="foo"))

    real_add_to_index = reindex.add_to_index
    add_to_index_paths = []

    def patched_add_to_index(path, index_name, *, pool):
        add_to_index_paths.append(path)
        return real_add_to_index(path, index_name, pool=pool)

    monkeypatch.setattr(reindex, "add_to_index", patched_add_to_index)

    run_reindex(index_name)
    assert len(add_to_index_paths) == 1
    assert add_to_index_paths[0] == "tests/data/access_log/access_log"

    actual_index = json.load(open(INDEX_DIR / index_name / DATE_INDEX_NAME))
    assert actual_index == {**expected_index, "2019-02-25": {"access_log": [110]}}
