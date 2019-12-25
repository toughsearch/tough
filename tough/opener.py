import os
from typing import BinaryIO, ContextManager, Optional, Type, Union

import indexed_gzip as igzip

from .config import INDEX_DIR


class Opener(ContextManager):
    file: Optional[Union[BinaryIO, igzip._IndexedGzipFile]]

    def __init__(self, name: str, index_name: str) -> None:
        self.name = name
        self.index_name = index_name
        self.file = None

    def __enter__(self) -> BinaryIO:
        self.file = self.open()
        return self.file

    def __exit__(self, *args) -> None:
        if not self.file:
            raise IOError

        self.file.close()

    def open(self) -> BinaryIO:  # pragma: no cover
        raise NotImplementedError

    def export_index(self) -> None:
        pass


class TextFileOpener(Opener):
    def open(self) -> BinaryIO:
        return open(self.name, "r+b")


class GzipFileOpener(Opener):
    def open(self) -> BinaryIO:
        f: igzip._IndexedGzipFile = igzip.IndexedGzipFile(self.name)
        basename = os.path.basename(self.name)
        gzindex_name = os.path.join(
            INDEX_DIR, self.index_name, f"{basename}.gzindex"
        )
        if os.path.isfile(gzindex_name):
            f.import_index(gzindex_name)

        return f

    def export_index(self):
        if not self.file:
            raise IOError

        basename = os.path.basename(self.name)
        gzindex_name = os.path.join(
            INDEX_DIR, self.index_name, f"{basename}.gzindex"
        )
        self.file.seek(self.file.tell() - 1)
        self.file.export_index(gzindex_name)


def fopen(name, index_name) -> Opener:
    opener: Type[Opener] = TextFileOpener
    if name.endswith(".gz"):
        opener = GzipFileOpener

    return opener(name, index_name)
