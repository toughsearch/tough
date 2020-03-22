from typing import BinaryIO, Union

from indexed_gzip import IndexedGzipFile

FileLike = Union[IndexedGzipFile, BinaryIO]
