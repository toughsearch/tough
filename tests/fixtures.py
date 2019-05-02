import glob
import gzip
import os
import shutil
import uuid

import pytest

from tests.const import DATA_DIR, INDEX_NAME
from tough.config import INDEX_DIR


@pytest.fixture
def provide_data():
    """
    Create files access_log.2.gz, access_log.1, access_log with some data
    """
    fmt = (
        '127.0.0.1 - - [{date}:12:34:56 +0000] "GET /{url} HTTP/1.1" 404 233 "-" "-"\n'
    )
    date_1 = "20/Feb/2019"
    date_2 = "21/Feb/2019"
    date_3 = "22/Feb/2019"
    date_4 = "23/Feb/2019"

    files_dir = os.path.join(DATA_DIR, INDEX_NAME)

    file_1 = gzip.open(os.path.join(files_dir, f"{INDEX_NAME}.2.gz"), "wt")
    file_2 = open(os.path.join(files_dir, f"{INDEX_NAME}.1"), "w")
    file_3 = open(os.path.join(files_dir, f"{INDEX_NAME}"), "w")

    to_write = [
        (file_1, ((date_1, 10), (date_2, 100))),
        (file_2, ((date_2, 10), (date_3, 100))),
        (file_3, ((date_3, 10), (date_4, 100))),
    ]

    for file, data in to_write:
        for date, number in data:
            for _ in range(number):
                file.write(fmt.format(date=date, url=uuid.uuid4()))
        file.close()

    yield


@pytest.fixture(autouse=True)
def clean():
    yield
    for f in glob.glob(os.path.join(DATA_DIR, INDEX_NAME, "*")):
        os.remove(f)
    shutil.rmtree(os.path.join(INDEX_DIR), ignore_errors=True)
