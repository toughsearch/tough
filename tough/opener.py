import gzip


class Opener:
    def __init__(self, name):
        self.name = name
        self.file = None

    def __enter__(self):
        self.file = self.open()
        return self.file

    def __exit__(self, *args):
        self.file.close()

    def open(self):  # pragma: no cover
        raise NotImplementedError


class TextFileOpener(Opener):
    def open(self):
        return open(self.name, "r+b")


class GzipFileOpener(Opener):
    def open(self):
        return gzip.open(self.name, "r+b")


def fopen(name):
    opener = TextFileOpener
    if name.endswith(".gz"):
        opener = GzipFileOpener

    return opener(name)
