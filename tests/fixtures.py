import datetime
import glob
import gzip
import os
import shutil
import uuid

import pytest

from tough.config import INDEX_DIR


@pytest.fixture
def data_dir():
    return "tests/data"


@pytest.fixture
def index_name():
    return "access_log"


@pytest.fixture
def get_row():
    fmt = (
        '127.0.0.1 - - [{date}:12:34:56 +0000] "GET /{url} HTTP/1.1" 404 233 "-" "-"\n'
    )

    def _get_row(date):
        str_date = date.strftime("%d/%b/%Y")
        url = uuid.uuid4()
        return fmt.format(date=str_date, url=url)

    return _get_row


@pytest.fixture
def create_data_file(get_row, data_dir, index_name):
    files_dir = os.path.join(data_dir, index_name)

    def _create_data_file(name, dates):
        opener = open
        mode = "w"
        if name.endswith(".gz"):
            opener = gzip.open
            mode = "wt"

        with opener(os.path.join(files_dir, name), mode) as f:
            for date, number in dates:
                lines = "".join(get_row(date) for _ in range(number))
                f.writelines(lines)

    return _create_data_file


@pytest.fixture
def provide_data(create_data_file, index_name):
    """
    Create files access_log.2.gz, access_log.1, access_log with some data
    """
    dates = [datetime.date(2019, 2, day) for day in range(20, 24)]
    create_data_file(f"{index_name}.2.gz", ((dates[0], 10), (dates[1], 100)))
    create_data_file(f"{index_name}.1", ((dates[1], 10), (dates[2], 100)))
    create_data_file(index_name, ((dates[2], 10), (dates[3], 100)))


@pytest.fixture(autouse=True)
def clean(data_dir, index_name):
    yield
    for f in glob.glob(os.path.join(data_dir, index_name, "*")):
        os.remove(f)
    shutil.rmtree(os.path.join(INDEX_DIR), ignore_errors=True)
