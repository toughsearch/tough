import pytest

from tough.command import ensure_index_dir
from tough.eol_mapper import EOLMapper


@pytest.fixture(autouse=True)
def setup():
    ensure_index_dir()


@pytest.fixture
def create_source_file(index_name, create_data_file, data_dir):
    def _create_source_file(contents):
        filename = data_dir / index_name / index_name
        filename.write_text(contents)
        return filename

    return _create_source_file


@pytest.fixture
def create_eol_mapper(create_source_file, index_name):
    def _create_eol_mapper(contents):
        file_path = create_source_file(contents)

        eol_mapper = EOLMapper(file_path, index_name)
        eol_mapper.open()
        line_length = 0
        with open(file_path, "r") as f:
            lineno = 0
            while True:
                line = f.readline()
                if not line:
                    break

                if not line_length:
                    line_length = len(line) - 1

                eol_mapper.write(lineno, f.tell())
                lineno += 1

        eol_mapper.mark_ok()
        eol_mapper.close()

        return eol_mapper

    return _create_eol_mapper
