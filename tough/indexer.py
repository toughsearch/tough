from . import EOLMapper
from .utils import fopen

BUF_SIZE = 2 * 1024 * 1024


def get_indexes(haystack, needle, add_offset=0):
    indexes = []
    index = haystack.find(needle)

    while index > -1:
        indexes.append(index + add_offset)
        index = haystack.find(needle, index + 1)

    return indexes


def eol_map(fname):
    with fopen(fname) as f:
        lmap = EOLMapper(fname)
        lmap.open()
        if not lmap.needs_remapping():
            return

        lineno = 0

        while True:
            cur_pos = f.tell()
            buf = f.read(BUF_SIZE)
            if not buf:
                break

            indexes = get_indexes(buf, b"\n", cur_pos)
            for index in indexes:
                lmap.write(lineno, index + 1)
                lineno += 1

            if len(buf) < BUF_SIZE:
                break
        lmap.mark_ok()
        lmap.close()
