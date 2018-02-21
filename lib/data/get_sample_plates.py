"""
Get the entered data from the sample_plates Google sheet.

There is a fixed format to the plates:
                            Plate column 1  ...     Plate column 12
    plate_id:UUID
    entry_date:ISO_Date
    local_id:Text
    protocol:Text
    notes:Text
    results:Text
    Plate row A                UUID?          ...     UUID?
        .                        .            ...       .
        .                        .            ...       .
        .                        .            ...       .
    Plate row H                UUID?          ...     UUID?
"""

import csv
from tempfile import NamedTemporaryFile
from lib.dict_attr import DictAttrs
import data.db as db
import google

KEYS = ['plate_id', 'entry_date', 'local_id', 'protocol', 'notes', 'results']
ROWS_START = 5
ROWS_END = 14
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
                        # Row label looks like "Plate row X". Get the X.
                        rec.plate_row = row[0][-1]  # Letter of row label
                        for col in range(1, COLS_END):
                            if db.is_uuid(row[col]):
                                rec.plate_col = col
                                rec.sample_id = row[col]
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
