"""Get the entered data from the sample_plates Google sheet."""

import csv
from tempfile import NamedTemporaryFile
from lib.dict_attr import DictAttrs
import lib.db as db
import lib.google as google

KEYS = ['plate_id', 'entry_date', 'local_id', 'protocol', 'notes']
ROWS_START = 4
ROWS_END = 13
COLS_END = 13


def get_data():
    """Import sample plate data from the Google sheet."""
    with db.connect() as db_conn:
        db.create_sample_plates_table(db_conn)
        batch = []

        with NamedTemporaryFile(delete=False) as temp_csv:
            google.export_sheet_csv('sample_plates', temp_csv)
            temp_csv.close()

            with open(temp_csv.name) as csv_file:
                reader = csv.reader(csv_file)
                rec = DictAttrs({})
                state = 0
                for row in reader:
                    if state == 0:
                        if db.is_uuid(row[0]):
                            rec[KEYS[state]] = row[0]
                            state += 1
                    elif state <= ROWS_START:
                        rec[KEYS[state]] = row[0]
                        state += 1
                    elif state < ROWS_END:
                        rec.plate_row = state - ROWS_START
                        for col in range(12):
                            if db.is_uuid(row[col + 1]):
                                rec.plate_col = 'ABCDEFGHIJKL'[col]
                                rec.sample_id = row[col + 1]
                                batch.append((
                                    rec.plate_id, rec.entry_date, rec.local_id,
                                    rec.protocol, rec.notes, rec.plate_row,
                                    rec.plate_col, rec.sample_id))
                        state += 1
                    else:
                        state = 0
                        rec = DictAttrs({})

        db.insert_sample_plates(db_conn, batch)


if __name__ == '__main__':
    get_data()
