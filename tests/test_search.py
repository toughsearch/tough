import datetime

import pytest

from tough.commands.reindex import run_reindex
from tough.commands.search import run_search


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
def test_search_date_range(
    provide_data, capsys, substring, date_from, date_to, count, index_name
):
    run_reindex(index_name)
    run_search(substring, False, index_name, date_from, date_to)
    captured = capsys.readouterr()
    assert len([*filter(None, captured.out.split("\n"))]) == count


def test_search_all(provide_data, capsys, index_name):
    run_reindex(index_name)
    run_search("HTTP/1.1", False, index_name)
    captured = capsys.readouterr()
    assert len([*filter(None, captured.out.split("\n"))]) == 330


def test_search_regex(provide_data, capsys, index_name):
    run_reindex(index_name)
    run_search(r"HTTP.*", True, index_name, "2019-02-20", "2019-02-20")
    captured = capsys.readouterr()
    assert len([*filter(None, captured.out.split("\n"))]) == 10


@pytest.mark.parametrize("search_date", ["2019-02-20", "2019-02-21"])
def test_search_wrong_index(create_data_file, capsys, index_name, search_date):
    create_data_file(
        index_name,
        ((datetime.date(2019, 2, 20), 1), (datetime.date(2019, 2, 21), 1)),
    )
    run_reindex(index_name)
    run_search(r"HTTP.*", True, index_name, search_date, search_date)
    captured = capsys.readouterr()
    assert len([*filter(None, captured.out.split("\n"))]) == 1


def test_search_fail(capsys):
    run_search("", True, "")
    captured = capsys.readouterr()
    assert "Please" in captured.err
