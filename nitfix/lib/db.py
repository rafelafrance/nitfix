"""SQL functions."""

from os.path import exists
from pathlib import Path
import sqlite3
from .util import PROCESSED_DATA, is_uuid

DB_NAME = 'nitfix.sqlite.db'


def connect(path=None):
    """Connect to the SQLite3 DB."""
    if not path:
        path = PROCESSED_DATA

    if not exists(path):
        path = Path('..') / PROCESSED_DATA

    path = str(path / DB_NAME)

    cxn = sqlite3.connect(path)

    cxn.execute("PRAGMA page_size = {}".format(2**16))
    cxn.execute("PRAGMA busy_timeout = 10000")
    cxn.execute("PRAGMA journal_mode = WAL")

    cxn.create_function('IS_UUID', 1, is_uuid)
    return cxn


def get_columns(cxn, table):
    """Get a list of columns from a table"""
    sql = f'PRAGMA table_info({table});'
    cxn.row_factory = sqlite3.Row
    columns = [r[1] for r in cxn.execute(sql)]
    return columns
