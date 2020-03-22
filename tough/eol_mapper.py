from collections import namedtuple
import os
from typing import Optional

from .config import INDEX_DIR
from .types import FileLike

LEN_OFFSET = 5
OK = b"OK"
BUF_SIZE = 2 * 1024 * 1024
BYTE_ORDER = "little"

MapLine = namedtuple("MapLine", ["lineno", "offset", "length"])


class EOLMapper:
    """
    File EOL mapper.
    """

    f: Optional[FileLike]

    def __init__(self, fname, index_name) -> None:
        basename = os.path.basename(fname)
        self.map_fname = INDEX_DIR / index_name / f"{basename}.map"
        self.map_fname.touch()

        self.f = None
        self.f_read = open(self.map_fname, "rb")

    def open(self) -> None:
        self.f = open(self.map_fname, "r+b")

    def close(self) -> None:
        if not self.f:
            raise IOError

        self.f.close()

    def read(self, lineno) -> Optional[MapLine]:
        if lineno >= self.count_lines() or lineno < 0:
            return None

        f = self.f_read
        f.seek(LEN_OFFSET * max(lineno - 1, 0))

        if lineno > 0:
            record = f.read(LEN_OFFSET * 2)
            offset_start = int.from_bytes(record[:LEN_OFFSET], BYTE_ORDER)
            offset_end = int.from_bytes(record[LEN_OFFSET:], BYTE_ORDER)
        else:
            record = f.read(LEN_OFFSET)
            offset_start = 0
            offset_end = int.from_bytes(record[:LEN_OFFSET], BYTE_ORDER)

        length = offset_end - offset_start - 1

        return MapLine(lineno, offset_start, length)

    def write(self, lineno, offset) -> None:
        if not self.f:
            raise IOError

        record = offset.to_bytes(LEN_OFFSET, BYTE_ORDER)
        self.f.seek(LEN_OFFSET * lineno)
        self.f.write(record)

    def count_lines(self) -> int:
        return self.map_fname.stat().st_size // LEN_OFFSET

    def mark_ok(self) -> None:
        if not self.f:
            raise IOError
        self.f.seek(0, 2)
        self.f.write(OK)
