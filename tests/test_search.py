import pytest

from tough.commands.reindex import run_reindex
from tough.commands.search import run_search

INDEX_NAME = "access_log"
DATA_DIR = "tests/data"


@pytest.mark.parametrize(
    ("substring", "date_from", "date_to", "count"),
    [
        ("HTTP/1.1", "2019-02-20", "2019-02-20", 10),
        ("HTTP/1.1", "2019-02-20", "2019-02-21", 120),
        ("HTTP/1.1", "2019-02-21", "2019-02-21", 110),
        ("HTTP/1.1", "2019-02-21", "2019-02-22", 220),
        ("HTTP/1.1", "2019-02-22", "2019-02-22", 110),
        ("HTTP/1.1", "2019-02-22", "2019-02-23", 210),
        ("HTTP/1.1", "2019-02-23", "2019-02-23", 100),
        ("HTTP/1.1", "2019-02-24", "2019-02-25", 0),
        ("boom", "2019-02-20", "2019-02-24", 0),
    ],
)
def test_search_date_range(provide_data, capsys, substring, date_from, date_to, count):
    run_reindex(INDEX_NAME)
    run_search(substring, None, INDEX_NAME, date_from, date_to)
    captured = capsys.readouterr()
    assert len([*filter(None, captured.out.split("\n"))]) == count


def test_search_all(provide_data, capsys):
    run_reindex(INDEX_NAME)
    run_search("HTTP/1.1", None, INDEX_NAME)
    captured = capsys.readouterr()
    assert len([*filter(None, captured.out.split("\n"))]) == 330


def test_search_regex(provide_data, capsys):
    run_reindex(INDEX_NAME)
    run_search("", r"HTTP.*", INDEX_NAME, "2019-02-20", "2019-02-20")
    captured = capsys.readouterr()
    assert len([*filter(None, captured.out.split("\n"))]) == 10
