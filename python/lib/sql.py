"""Utilities for SQL functions."""

from os.path import join
import sqlite3


def connect():
    """Connect to the SQLite3 DB."""
    db_path = join('data', 'nitfix.sqlite.db')
    return sqlite3.connect(db_path)
