import argparse
import sys
import os
from pathlib import Path

from derezz.catalog import load_catalog
from derezz.find import find, open_, ls
from derezz.index import index


def main():
    #
    # derezz index|find
    #

    parser = argparse.ArgumentParser(
        prog="derezz", description="Video search powered by Daft and S3 Tables."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    #
    # derezz index <path>
    #

    parser_index = subparsers.add_parser(
        "index", help="Upload this video to the search index."
    )
    parser_index.add_argument("path", type=str, help="Path to the video file to index.")

    #
    # derezz find <pattern>
    #

    parser_find = subparsers.add_parser(
        "find", help="Find videos with the specific items."
    )
    parser_find.add_argument("pattern", type=str, help="Search pattern.")

    #
    # derezz ls
    #
    parser_ls = subparsers.add_parser("ls", help="List indexed labels.")

    #
    # derezz open <pattern>
    #

    parser_open = subparsers.add_parser("open", help="Open the search hits as images.")
    parser_open.add_argument("pattern", type=str, help="Search pattern.")

    stage = os.environ.get("DEREZZ_STAGE", "dev")
    catalog = load_catalog(stage)

    args = parser.parse_args()
    if args.command == "index":
        index(catalog, Path(args.path))
    elif args.command == "ls":
        ls(catalog)
    elif args.command == "find":
        find(catalog, args.pattern)
    elif args.command == "open":
        open_(catalog, args.pattern)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
