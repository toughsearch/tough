import os

import indexed_gzip as igzip

from .utils import dotify


class Opener:
    def __init__(self, name):
        self.name = name
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
        gzindex_name = dotify(self.name + ".gzindex")
        if os.path.isfile(gzindex_name):
            f.import_index(gzindex_name)
        else:
            # TODO: Do not build index separately
            f.build_full_index()
            f.export_index(gzindex_name)
        return f


def fopen(name):
    opener = TextFileOpener
    if name.endswith(".gz"):
        opener = GzipFileOpener

    return opener(name)
