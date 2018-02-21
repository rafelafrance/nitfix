"""Load the generated UUIDs into the database."""

from os.path import join
from glob import glob
import lib.db as db


def load_uuids():
    """Load the generated UUIDs into the database."""
    pattern = join('data', 'uuids', 'uuids_batch_*.txt')
    with db.connect() as db_conn:
        db.create_uuids_table(db_conn)
        for path in glob(pattern):
            with open(path) as uuid_file:
                uuids = [(u.strip(), ) for u in uuid_file.readlines()]
            db.insert_uuid_batch(db_conn, uuids)


if __name__ == '__main__':
    load_uuids()
