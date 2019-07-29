from collections import namedtuple
import os

from .config import INDEX_DIR, MIN_CHUNK_LENGTH, NUM_WORKERS

LEN_OFFSET = 5
OK = b"OK"
BUF_SIZE = 2 * 1024 * 1024
BYTE_ORDER = "little"

MapLine = namedtuple("MapLine", ["lineno", "offset", "length"])


class EOLMapper:
    """
    File EOL mapper.
    """

    def __init__(self, fname, index_name):
        basename = os.path.basename(fname)
        self.map_fname = os.path.join(INDEX_DIR, index_name, f"{basename}.map")

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

    def write(self, lineno, offset):
        record = offset.to_bytes(LEN_OFFSET, BYTE_ORDER)
        self.f.seek(LEN_OFFSET * lineno)
        self.f.write(record)

    def count_lines(self):
        return os.path.getsize(self.map_fname) // LEN_OFFSET

    def mark_ok(self):
        self.f.seek(0, 2)
        self.f.write(OK)


def chunkify(to_search, index_name, min_chunk_length=MIN_CHUNK_LENGTH):
    for path, lines_range in to_search:
        lines_from = 0
        lines_to = EOLMapper(path, index_name).count_lines()
        length = min_chunk_length
        if lines_range is not None:
            if len(lines_range) == 1:
                lines_from = lines_range[0]
                lines_to = lines_from + 1
                length = 1

            elif len(lines_range) == 2:
                lines_from, lines_to = lines_range
                lines = lines_to - lines_from
                length = max(round(lines / (NUM_WORKERS * 4)), min_chunk_length)

            else:
                raise ValueError("Wrong date index")

        for line_start in range(lines_from, lines_to, length):
            yield path, line_start, length, lines_to
