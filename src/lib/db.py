"""SQL functions."""

from pathlib import Path
import sqlite3
import lib.util as util
from lib.dict_attr import DictAttrs


def attr_factory(cursor, row):
    """Turn the rows into pseudo-data classes.attr_factory."""
    data = DictAttrs({})
    for i, col in enumerate(cursor.description):
        data[col[0]] = row[i]
    return data


def connect(factory=None):
    """Connect to the SQLite3 DB."""
    if not factory:
        factory = sqlite3.Row

    db_path = str(Path('data') / 'processed' / 'nitfix.sqlite.db')
    cxn = sqlite3.connect(db_path)

    cxn.execute("PRAGMA page_size = {}".format(2**16))
    cxn.execute("PRAGMA busy_timeout = 10000")
    cxn.execute("PRAGMA journal_mode = OFF")
    cxn.execute("PRAGMA synchronous = OFF")
    cxn.execute("PRAGMA optimize")

    cxn.create_function('IS_UUID', 1, util.is_uuid)
    cxn.row_factory = factory
    return cxn
