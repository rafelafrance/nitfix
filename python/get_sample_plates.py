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
    with NamedTemporaryFile(delete=False) as temp_csv:
        google_sheet.export_sheet_csv('sample_plates', temp_csv)
        temp_csv.close()

        with open(temp_csv.name) as csv_file:
            reader = csv.reader(csv_file)
            fields = DictAttrs({})
            state = 0
            for row in reader:
                if state == 0:
                    if db.is_uuid(row[0]):
                        fields[KEYS[state]] = row[0]
                        state += 1
                elif state <= ROWS_START:
                    fields[KEYS[state]] = row[0]
                    state += 1
                elif state < ROWS_END:
                    fields.plate_row = state - ROWS_START
                    for col in range(12):
                        if db.is_uuid(row[col + 1]):
                            fields.plate_col = 'ABCDEFGHIJKL'[col]
                            fields.sample_id = row[col + 1]
                            print(fields)
                    state += 1
                else:
                    state = 0
                    fields = DictAttrs({})


if __name__ == '__main__':
    import_sample_plates()
