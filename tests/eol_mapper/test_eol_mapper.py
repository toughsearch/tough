import pytest

from tough.tough import MapLine


@pytest.mark.parametrize("contents", ["a\n"])
@pytest.mark.parametrize(
    ("lineno", "expected_result"), [(-1, None), (1, None), (1, None)]
)
def test_eol_map_boundaries(
    create_eol_mapper, contents, lineno, expected_result
):
    eol_mapper = create_eol_mapper(contents)
    assert eol_mapper.read(lineno) == expected_result


@pytest.mark.parametrize(
    "contents",
    [
        "a\n"  # offset=0, line_length=1
        "aa\n"  # offset=2, line_length=2
        "aaa\n"  # offset=5, line_length=3
    ],
)
@pytest.mark.parametrize(
    ("lineno", "expected_result"),
    [
        (0, MapLine(0, 0, 1)),
        (1, MapLine(1, 2, 2)),
        (2, MapLine(2, 5, 3)),
        (3, None),
    ],
)
def test_eol_map_1(create_eol_mapper, contents, lineno, expected_result):
    eol_mapper = create_eol_mapper(contents)
    assert eol_mapper.read(lineno) == expected_result


@pytest.mark.parametrize(
    "contents",
    [
        "\n"  # offset=0, line_length=0
        "a\n"  # offset=1, line_length=1
        "aa\n"  # offset=3, line_length=2
    ],
)
@pytest.mark.parametrize(
    ("lineno", "expected_result"),
    [
        (0, MapLine(0, 0, 0)),
        (1, MapLine(1, 1, 1)),
        (2, MapLine(2, 3, 2)),
        (3, None),
    ],
)
def test_eol_map_2(create_eol_mapper, contents, lineno, expected_result):
    eol_mapper = create_eol_mapper(contents)
    assert eol_mapper.read(lineno) == expected_result


@pytest.mark.parametrize("contents", ["\n"])
@pytest.mark.parametrize(
    ("lineno", "expected_result"), [(0, MapLine(0, 0, 0)), (1, None)]
)
def test_eol_map_3(create_eol_mapper, contents, lineno, expected_result):
    eol_mapper = create_eol_mapper(contents)
    assert eol_mapper.read(lineno) == expected_result


@pytest.mark.parametrize("contents", [""])
@pytest.mark.parametrize(("lineno", "expected_result"), [(0, None)])
def test_eol_map_4(create_eol_mapper, contents, lineno, expected_result):
    eol_mapper = create_eol_mapper(contents)
    assert eol_mapper.read(lineno) == expected_result


@pytest.mark.parametrize(
    ("contents", "expected"),
    [
        ("", 0),
        ("\n", 1),
        ("a\n", 1),
        ("a\na", 2),
        ("a\na\n", 2),
        ("a\na\n\n", 3),
    ],
)
def test_eol_map_count_lines(create_eol_mapper, contents, expected):
    eol_mapper = create_eol_mapper(contents)
    assert eol_mapper.count_lines() == expected
