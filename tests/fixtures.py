import glob
import gzip
import os
import uuid

import pytest

from tests.const import DATA_DIR, INDEX_NAME
from tough.config import INDEX_DIR


@pytest.fixture
def provide_data():
    """
    Create files access_log.2.gz, access_log.1, access_log with some data
    """

    def _provide(newline="\n"):
        fmt = '127.0.0.1 - - [{date}:12:34:56 +0000] "GET /{url} HTTP/1.1" 404 233 "-" "-"'
        files_dir = os.path.join(DATA_DIR, INDEX_NAME)

        file_1 = gzip.open(os.path.join(files_dir, f"{INDEX_NAME}.2.gz"), "wt")
        file_2 = open(os.path.join(files_dir, f"{INDEX_NAME}.1"), "w")
        file_3 = open(os.path.join(files_dir, f"{INDEX_NAME}"), "w")

        to_write = [
            (file_1, (("20/Feb/2019", 10), ("21/Feb/2019", 100))),
            (file_2, (("21/Feb/2019", 10), ("22/Feb/2019", 100))),
            (file_3, (("22/Feb/2019", 10), ("23/Feb/2019", 100))),
        ]

        for file, data in to_write:
            for date, number in data:
                for _ in range(number):
                    file.write(fmt.format(date=date, url=uuid.uuid4()) + newline)
            file.close()

    return _provide


@pytest.fixture
def provide_data_lf(provide_data):
    provide_data()


@pytest.fixture(autouse=True)
def clean():
    yield
    for f in glob.glob(os.path.join(DATA_DIR, INDEX_NAME, "access_log*")):
        os.remove(f)
    for f in glob.glob(os.path.join(DATA_DIR, INDEX_NAME, ".access_log*")):
        os.remove(f)
    for f in glob.glob(os.path.join(INDEX_DIR, "*")):
        os.remove(f)
