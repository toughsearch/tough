import pytest

from tough.utils import date_range, dotify, get_datetime

from .const import INDEX_NAME


def test_dotify():
    assert dotify("foo.bar") == ".foo.bar"


def test_date_range():
    r = date_range("2017-12-31", "2018-01-02")
    assert list(r) == ["2017-12-31", "2018-01-01", "2018-01-02"]


def test_get_datetime():
    s = b'127.0.0.1 - - [20/Feb/2019:23:03:24 +0000] "GET / HTTP/1.1"'
    assert get_datetime(s, INDEX_NAME) == "2019-02-20"

    s = b'127.0.0.1 - - [2019-02-20 23:03:24 +0000] "GET / HTTP/1.1"'
    with pytest.raises(ValueError):
        get_datetime(s, INDEX_NAME)

    s = b""
    with pytest.raises(ValueError):
        get_datetime(s, INDEX_NAME)
