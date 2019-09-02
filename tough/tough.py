import argparse
from datetime import date, datetime

from .search import run_search


def type_date(string):
    return datetime.strptime(string, "%Y-%m-%d").date()


def run():
    parser = argparse.ArgumentParser(description="ToughSearch")
    parser.add_argument(
        "substring", help="Substring to search", default="", nargs="?"
    )
    parser.add_argument("index", help="Index to search")
    parser.add_argument("-e", "--regex", help="Regex pattern")
    parser.add_argument("-df", "--date-from", type=type_date, default=date.min)
    parser.add_argument("-dt", "--date-to", type=type_date, default=date.max)

    args = parser.parse_args()
    dict_args = args.__dict__
    run_search(**dict_args)


if __name__ == "__main__":  # pragma: no cover
    run()
