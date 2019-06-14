import argparse

from .commands.reindex import run_reindex
from .commands.search import run_search


def run():
    main_parser = argparse.ArgumentParser(description="ToughSearch")

    subparsers = main_parser.add_subparsers(dest="command")

    index_parser = subparsers.add_parser(
        "reindex", help="Reindex using conf.yaml"
    )
    index_parser.add_argument(
        "index", help="Index name to reindex", default="", nargs="?"
    )

    search_parser = subparsers.add_parser("search", help="Searcher")
    search_parser.add_argument(
        "substring", help="Substring to search", default="", nargs="?"
    )
    search_parser.add_argument("index", help="Index to search")
    search_parser.add_argument("-e", "--regex", help="Regex pattern")
    search_parser.add_argument("-df", "--date-from")
    search_parser.add_argument("-dt", "--date-to")

    args = main_parser.parse_args()
    dict_args = args.__dict__

    commands = {"search": run_search, "reindex": run_reindex}

    commands[dict_args.pop("command")](**dict_args)


if __name__ == "__main__":  # pragma: no cover
    run()
