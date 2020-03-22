import argparse
from typing import Callable, Dict

from .command import Indexer, Searcher
from .errors import UnknownCommandError


def run() -> None:
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
    search_parser.add_argument(
        "-e",
        "--regex",
        help="Substring is a regex pattern",
        action="store_true",
    )
    search_parser.add_argument("-df", "--date-from")
    search_parser.add_argument("-dt", "--date-to")

    args = main_parser.parse_args()
    dict_args = args.__dict__

    commands: Dict[str, Callable] = {"search": Searcher, "reindex": Indexer}

    cmd_class = commands.get(dict_args.pop("command"))
    if not cmd_class:
        raise UnknownCommandError
    cmd = cmd_class(**dict_args)
    cmd.run()


if __name__ == "__main__":  # pragma: no cover
    run()
