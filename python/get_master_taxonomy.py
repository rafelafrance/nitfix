"""Get master taxonomy info from Google sheet and put it into the database."""

import csv
from tempfile import NamedTemporaryFile
import lib.db as db
import lib.google as google


def import_master_taxonomy():
    """Import sample plate data from the Google sheet."""
    with db.connect() as db_conn:
        db.create_taxonomies_table(db_conn)
        batch = []

        # with open('data/master_taxonomy.csv', 'wb') as temp_csv:
        with NamedTemporaryFile(delete=False) as temp_csv:
            google.export_sheet_csv('NitFixMasterTaxonomy', temp_csv)
            temp_csv.close()

            with open(temp_csv.name) as csv_file:
                reader = csv.reader(csv_file)
                next(reader)    # Skip header
                for row in reader:
                    batch.append(row)

        db.insert_taxonomy(db_conn, batch)
        db.create_taxonomies_indexes(db_conn)


if __name__ == '__main__':
    import_master_taxonomy()
