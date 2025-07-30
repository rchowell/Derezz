import argparse
import sys
from pathlib import Path

from derezz.find import find
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
    # derezz find <term>
    #

    parser_find = subparsers.add_parser(
        "find", help="Find videos with the specific items."
    )
    parser_find.add_argument("term", type=str, help="Search term.")

    args = parser.parse_args()
    if args.command == "index":
        index(Path(args.path))
    elif args.command == "find":
        find(args.items)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
