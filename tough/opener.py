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

    def open(self):  # pragma: no cover
        raise NotImplementedError

    def export_index(self):
        pass


class TextFileOpener(Opener):
    def open(self):
        return open(self.name, "r+b")


class GzipFileOpener(Opener):
    def open(self):
        f: igzip._IndexedGzipFile = igzip.IndexedGzipFile(self.name)
        basename = os.path.basename(self.name)
        gzindex_name = os.path.join(
            INDEX_DIR, self.index_name, f"{basename}.gzindex"
        )
        if os.path.isfile(gzindex_name):
            f.import_index(gzindex_name)

        return f

    def export_index(self):
        basename = os.path.basename(self.name)
        gzindex_name = os.path.join(
            INDEX_DIR, self.index_name, f"{basename}.gzindex"
        )
        self.file.seek(self.file.tell() - 1)
        self.file.export_index(gzindex_name)


def fopen(name, index_name):
    opener = TextFileOpener
    if name.endswith(".gz"):
        opener = GzipFileOpener

    return opener(name, index_name)
