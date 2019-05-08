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
        self.close()

    def open(self):
        raise NotImplementedError

    def close(self):
        self.file.close()


class TextFileOpener(Opener):
    def open(self):
        return open(self.name, "r+b")


class GzipFileOpener(Opener):
    def __init__(self, name, index_name):
        super().__init__(name, index_name)
        self.has_index = False
        basename = os.path.basename(self.name)
        self.gzindex_name = os.path.join(
            INDEX_DIR, self.index_name, f"{basename}.gzindex"
        )

    def open(self):
        file: igzip._IndexedGzipFile = igzip.IndexedGzipFile(self.name)

        if os.path.isfile(self.gzindex_name):
            file.import_index(self.gzindex_name)
            self.has_index = True
        return file

    def close(self):
        if not self.has_index:
            self.file.export_index(self.gzindex_name)
        super().close()


def fopen(name, index_name):
    opener = TextFileOpener
    if name.endswith(".gz"):
        opener = GzipFileOpener

    return opener(name, index_name)
