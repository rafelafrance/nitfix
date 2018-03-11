"""Get master taxonomy info from Google sheet and put it into the database."""

import csv
from pathlib import Path
import lib.db as db
import lib.google as google


def get_data():
    """Import sample plate data from the Google sheet."""
    with db.connect() as db_conn:
        db.create_taxons_table(db_conn)
        batch = []

        csv_path = Path('data') / 'interim' / 'taxons.csv'
        with open(csv_path, 'wb') as temp_csv:
            google.export_sheet_csv('NitFixMasterTaxonomy', temp_csv)
            temp_csv.close()

            with open(temp_csv.name) as csv_file:
                reader = csv.reader(csv_file)
                next(reader)    # Skip header
                for row in reader:
                    row.append(row[2].split()[0])
                    batch.append(row)

        db.insert_taxon_batch(db_conn, batch)


if __name__ == '__main__':
    get_data()
