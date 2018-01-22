"""Get the entered data from the sample_plates Google sheet."""

# pylint: disable=no-member

import csv
from tempfile import NamedTemporaryFile
from lib.dict_attr import DictAttrs
import lib.db as db
import lib.google_sheet as google_sheet

KEYS = ['plate_id', 'entry_date', 'local_id', 'protocol', 'notes']
ROWS_START = 4
ROWS_END = 13


def import_sample_plates():
    """Import sample plate data from the Google sheet."""
    db_conn = db.connect()
    db.create_sample_plates_table(db_conn)

    with NamedTemporaryFile(delete=False) as temp_csv:
        google_sheet.export_sheet_csv('sample_plates', temp_csv)
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
                            db.insert_sample_plate(db_conn, rec)
                    state += 1
                else:
                    state = 0
                    rec = DictAttrs({})


if __name__ == '__main__':
    import_sample_plates()
