"""SQL functions."""

from pathlib import Path
import sqlite3
import lib.util as util

DB_NAME = 'nitfix.sqlite.db'


def connect(path=None):
    """Connect to the SQLite3 DB."""
    if not path:
        path = str(Path('data') / 'processed' / DB_NAME)

    cxn = sqlite3.connect(path)

    cxn.execute("PRAGMA page_size = {}".format(2**16))
    cxn.execute("PRAGMA busy_timeout = 10000")
    cxn.execute("PRAGMA journal_mode = OFF")
    cxn.execute("PRAGMA synchronous = OFF")
    cxn.execute("PRAGMA optimize")

    cxn.create_function('IS_UUID', 1, util.is_uuid)
    return cxn


def connect_up():
    """Allow notebooks and other utilities to connect from local dirs."""
    path = str(Path('..') / 'data' / 'processed' / DB_NAME)
    return connect(path)
