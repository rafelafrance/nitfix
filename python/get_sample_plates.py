"""Get the entered data from the sample_plates Google sheet."""

# pylint: disable=no-member

import csv
from tempfile import NamedTemporaryFile
# import lib.db as db
import lib.google_sheet as google_sheet


def import_sample_plates():
    """Import sample plate data from the Google sheet."""
    with NamedTemporaryFile(delete=False) as temp_csv:
        google_sheet.export_sheet_csv('sample_plates', temp_csv)
        temp_csv.close()

        with open(temp_csv.name) as csv_file:
            reader = csv.reader(csv_file)
            print(reader)
            for row in reader:
                print(row)


if __name__ == '__main__':
    import_sample_plates()
