import os
from types import TracebackType
from typing import BinaryIO, ContextManager, Optional, Type

import indexed_gzip as igzip

from .config import INDEX_DIR
from .types import FileLike


class Opener(ContextManager):
    file: Optional[FileLike]

    def __init__(self, name: str, index_name: str) -> None:
        self.name = name
        self.index_name = index_name
        self.file = None

    def __enter__(self) -> FileLike:
        self.file = self.open()
        return self.file

    def __exit__(
        self,
        __exc_type: Optional[Type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> None:
        if not self.file:
            raise IOError

        self.file.close()

    def open(self) -> FileLike:  # pragma: no cover
        raise NotImplementedError

    def export_index(self) -> None:
        pass


class TextFileOpener(Opener):
    def open(self) -> BinaryIO:
        return open(self.name, "r+b")


class GzipFileOpener(Opener):
    def open(self) -> FileLike:
        f = igzip.IndexedGzipFile(self.name)
        basename = os.path.basename(self.name)
        gzindex_name = INDEX_DIR / self.index_name / f"{basename}.gzindex"
        if gzindex_name.is_file():
            f.import_index(gzindex_name)

        return f

    def export_index(self):
        if not self.file:
            raise IOError

        basename = os.path.basename(self.name)
        self.file.seek(self.file.tell() - 1)
        gzindex_name = INDEX_DIR / self.index_name / f"{basename}.gzindex"
        self.file.export_index(gzindex_name)


def fopen(name, index_name) -> Opener:
    opener: Type[Opener] = TextFileOpener
    if name.endswith(".gz"):
        opener = GzipFileOpener

    return opener(name, index_name)
