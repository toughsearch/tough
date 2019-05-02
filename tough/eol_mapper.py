from collections import namedtuple
import os

from .config import MIN_CHUNK_LENGTH, NUM_WORKERS
from .opener import fopen
from .utils import dotify

LEN_OFFSET = 5
OK = b"OK"
BUF_SIZE = 2 * 1024 * 1024

MapLine = namedtuple("MapLine", ["lineno", "offset", "length"])


class EOLMapper:
    """
    File EOL mapper.
    """

    def __init__(self, fname):
        self.src_fname = fname
        self.map_fname = dotify(f"{fname}.map")

        if not os.path.isfile(self.map_fname):
            # Create empty map file
            open(self.map_fname, "w").close()

        self.f = None
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

    @classmethod
    def map(cls, fname):
        with fopen(fname) as f:
            lmap = cls(fname)
            lmap.open()
            if not lmap.needs_remapping():
                return

            lineno = 0

            while True:
                cur_pos = f.tell()
                buf = f.read(BUF_SIZE)
                if not buf:
                    break

                indexes = get_newlines(buf, cur_pos)
                for index in indexes:
                    lmap.write(lineno, index + 1)
                    lineno += 1

                if len(buf) < BUF_SIZE:
                    break
            lmap.mark_ok()
            lmap.close()


def chunkify(to_search, min_chunk_length=MIN_CHUNK_LENGTH):
    for path, lines_range in to_search:
        lines_from = 0
        lines_to = EOLMapper(path).count_lines()
        if lines_range is not None:
            # TODO: Single range case
            lines_from, lines_to = lines_range
        lines = lines_to - lines_from
        length = max(round(lines / (NUM_WORKERS * 4)), min_chunk_length)
        for line_start in range(lines_from, lines_to, length):
            yield path, line_start, length, lines_to


def get_newlines(haystack, add_offset=0):
    indexes = []
    index = find_newline(haystack)

    while index > -1:
        indexes.append(index + add_offset)
        index = find_newline(haystack, index + 1)

    return indexes


def find_newline(s, start=0):
    for newline in (b"\r\n", b"\n", b"\r"):
        index = s.find(newline, start)
        if index > -1:
            return index

    return -1
