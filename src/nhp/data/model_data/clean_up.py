"""Clean up spark files.

Removes any of the _SUCCESS, _committed_*, _started_* files that spark leaves."""

import os
import sys


def clean_up(path: str) -> None:
    """Clean up spark files from a given path.

    :param path: the path to clean up
    :type path: str
    """
    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            if (
                filename == "_SUCCESS"
                or filename.startswith("_committed_")
                or filename.startswith("_started_")
            ):
                full_path = os.path.join(dirpath, filename)
                os.remove(full_path)


def main() -> None:
    path = sys.argv[1]
    clean_up(path)
