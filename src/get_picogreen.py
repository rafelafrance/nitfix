"""Get Picogreen results from Google sheet and put it into the database."""

import re
import sys
import csv
from pathlib import Path
import lib.db as db
import lib.google as google


def get_sample_ids():
    """Get data from the sample plates table and organize it for linking."""
    local_id_regex = re.compile(r'^.+_(\d+)$')
    picogreen2sample_id = {}
    with db.connect(factory=db.attr_factory) as cxn:
        for well in db.select_plate_wells(cxn):
            match = local_id_regex.match(well.local_id)
            plate_no = match[1]
            well_no = 'ABCDEFGH'.find(well.plate_row) * 12 + well.plate_col
            picogreen_id = f'{plate_no}_{well_no:02d}'
            picogreen2sample_id[picogreen_id] = well.sample_id
    return picogreen2sample_id


def get_data(picogreen2sample_id):
    """Import picogreen data from the Google sheet."""
    with db.connect() as cxn:
        db.create_picogreen_table(cxn)
        batch = []
        values = []

        csv_path = Path('data') / 'interim' / 'picogreen.csv'
        with open(csv_path, 'wb') as temp_csv:
            google.export_sheet_csv('picogreen_2_14_2_15', temp_csv)
            temp_csv.close()

            with open(temp_csv.name) as csv_file:
                reader = csv.reader(csv_file)
                next(reader)    # Skip header
                for row in reader:
                    picogreen_id = row[0].strip()
                    sample_id = ''
                    if picogreen_id:
                        sample_id = picogreen2sample_id.get(row[0], 'x')
                        if row[-1] != sample_id:
                            print('Error: Plate IDs do not match')
                            sys.exit(1)
                    values.append([sample_id])
                    batch.append(row)

        db.insert_picogreen_batch(cxn, batch)

    range_ = f'H2:H{len(values) + 1}'
    google.update_sheet('picogreen', range_, values)


if __name__ == '__main__':
    PICOGREEN2SAMPLE_ID = get_sample_ids()
    get_data(PICOGREEN2SAMPLE_ID)
