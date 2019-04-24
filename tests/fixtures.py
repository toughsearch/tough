import glob
import gzip
import os
import uuid

import pytest
from tests.test_search import DATA_DIR, INDEX_NAME


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

    file_1 = os.path.join(DATA_DIR, INDEX_NAME, f"{INDEX_NAME}.2.gz")
    file_2 = os.path.join(DATA_DIR, INDEX_NAME, f"{INDEX_NAME}.1")
    file_3 = os.path.join(DATA_DIR, INDEX_NAME, f"{INDEX_NAME}")

    with gzip.open(file_1, "wt") as f:
        for _ in range(10):
            f.write(fmt.format(date=date_1, url=uuid.uuid4()))

        for _ in range(100):
            f.write(fmt.format(date=date_2, url=uuid.uuid4()))

    with open(file_2, "w") as f:
        for _ in range(10):
            f.write(fmt.format(date=date_2, url=uuid.uuid4()))

        for _ in range(100):
            f.write(fmt.format(date=date_3, url=uuid.uuid4()))

    with open(file_3, "w") as f:
        for _ in range(10):
            f.write(fmt.format(date=date_3, url=uuid.uuid4()))

        for _ in range(100):
            f.write(fmt.format(date=date_4, url=uuid.uuid4()))

    yield

    os.remove(file_1)
    os.remove(file_2)
    os.remove(file_3)
    for f in glob.glob(os.path.join(DATA_DIR, INDEX_NAME, ".access_log*")):
        os.remove(f)
