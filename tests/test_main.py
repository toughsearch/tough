import pytest


def test_main():
    import sys

    sys.argv.append("--help")
    with pytest.raises(SystemExit):
        from tough import __main__

        _ = __main__
