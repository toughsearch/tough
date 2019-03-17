from collections import namedtuple
import os

from .utils import dotify


LEN_OFFSET = 5
OK = b"OK"

MapLine = namedtuple("MapLine", ["lineno", "offset", "length"])


class EOLMapper:
    def __init__(self, fname):
        self.src_fname = fname
        self.map_fname = dotify(f"{fname}.map")

        if not os.path.isfile(self.map_fname):
            open(self.map_fname, "w").close()

        self.f_read = open(self.map_fname, "rb")

    def open(self):
        self.f = open(self.map_fname, "r+b")

    def close(self):
        self.f.close()

    def read(self, lineno):
        f = self.f_read
        f.seek(LEN_OFFSET * max(lineno - 1, 0))
        if lineno > 0:
            record = f.read(LEN_OFFSET * 2)
        else:
            record = f.read(LEN_OFFSET)

        if lineno > 0:
            offset_start = int.from_bytes(record[:LEN_OFFSET], "little")
            offset_end = int.from_bytes(record[LEN_OFFSET:], "little")
        else:
            offset_start = 0
            offset_end = int.from_bytes(record[:LEN_OFFSET], "little")

        length = offset_end - offset_start - 1

        return MapLine(lineno, offset_start, length)

    def write(self, lineno, offset):
        record = offset.to_bytes(LEN_OFFSET, "little")
        self.f.seek(LEN_OFFSET * lineno)
        self.f.write(record)

    def count_lines(self):
        return os.path.getsize(self.map_fname) // LEN_OFFSET

    def needs_remapping(self):
        try:
            self.f.seek(-2, 2)
            return self.f.read(2) != OK
        except OSError:
            return True

    def mark_ok(self):
        self.f.seek(0, 2)
        self.f.write(OK)
