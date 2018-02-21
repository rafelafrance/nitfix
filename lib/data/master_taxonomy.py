"""Function for dealing with the master taxonomy Google sheet."""

import csv
from tempfile import NamedTemporaryFile
from ..data import db
from .. import google


def import_sheet(path):
    """Import sample plate data from the Google sheet as a CSV file."""
    with open(path, 'wb') as out_file:
        google.export_sheet_csv('NitFixMasterTaxonomy', out_file)


def export2db(path):
    """Take an imported CSV file and export it to the DB."""
    with db.connect() as db_conn:
        db.create_taxonomies_table(db_conn)
        batch = []

        with open(path) as csv_file:
            reader = csv.reader(csv_file)

            headers = next(reader)
            validate_headers(headers)

            for row in reader:
                row.append(row[2].split()[0])
                batch.append(row)

        db.insert_taxonomy(db_conn, batch)
        db.create_taxonomies_indexes(db_conn)


def validate_headers(headers):
    """Make sure we got a file with the required information."""
    print(headers)


def read_old():
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
                    row.append(row[2].split()[0])
                    batch.append(row)

        db.insert_taxonomy(db_conn, batch)
        db.create_taxonomies_indexes(db_conn)
