"""Load the generated UUIDs into the database."""

from os.path import join
from glob import glob
import lib.db as db


def load_uuids():
    """Load the generated UUIDs into the database."""
    pattern = join('data', 'uuids', 'uuids_batch_*.txt')
    with db.connect() as cxn:
        db.create_uuids_table(cxn)
        for path in glob(pattern):
            with open(path) as uuid_file:
                uuids = [(u.strip(), ) for u in uuid_file.readlines()]
            db.insert_uuid_batch(cxn, uuids)


if __name__ == '__main__':
    load_uuids()
