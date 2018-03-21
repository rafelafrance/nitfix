"""Validate the data in the database."""

from os.path import join
import uuid
import sqlite3


def audit():
    """Check the database."""
    select = 'SELECT * FROM images'

    db_path = join('data', 'nitfix.sqlite.db')
    with sqlite3.connect(db_path) as cxn:
        cursor = cxn.cursor()
        cursor.execute(select)

        for row in cursor.fetchall():
            guid = row[0]
            try:
                uuid.UUID(guid)
            except ValueError:
                print(f'Bad UUID "{guid}"')  # noqa


if __name__ == '__main__':
    print('started')
    audit()
    print('finished')
