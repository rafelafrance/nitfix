"""SQL functions."""

from os.path import exists
from pathlib import Path
import sqlite3
import lib.util as util

DB_NAME = 'nitfix.sqlite.db'


def connect(path=None):
    """Connect to the SQLite3 DB."""
    if not path:
        path = str(util.PROCESSED_DATA / DB_NAME)

    if not exists(path):
        path = str(Path('..') / util.PROCESSED_DATA / DB_NAME)

    cxn = sqlite3.connect(path)

    cxn.execute("PRAGMA page_size = {}".format(2**16))
    cxn.execute("PRAGMA busy_timeout = 10000")
    cxn.execute("PRAGMA journal_mode = WAL")

    cxn.create_function('IS_UUID', 1, util.is_uuid)
    return cxn
