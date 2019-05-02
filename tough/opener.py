import os

import indexed_gzip as igzip

from .config import INDEX_DIR


class Opener:
    def __init__(self, name, index_name):
        self.name = name
        self.index_name = index_name
        self.file = None

    def __enter__(self):
        self.file = self.open()
        return self.file

    def __exit__(self, *args):
        self.file.close()

    def open(self):
        raise NotImplementedError


class TextFileOpener(Opener):
    def open(self):
        return open(self.name, "r+b")


class GzipFileOpener(Opener):
    def open(self):
        f: igzip._IndexedGzipFile = igzip.IndexedGzipFile(self.name)
        basename = os.path.basename(self.name)
        gzindex_name = os.path.join(INDEX_DIR, self.index_name, f"{basename}.gzindex")
        if os.path.isfile(gzindex_name):
            f.import_index(gzindex_name)
        else:
            # TODO: Do not build index separately
            f.build_full_index()
            f.export_index(gzindex_name)
        return f


def fopen(name, index_name):
    opener = TextFileOpener
    if name.endswith(".gz"):
        opener = GzipFileOpener

    return opener(name, index_name)
