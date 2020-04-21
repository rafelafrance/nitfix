"""Extract, transform, and load data related to the samples."""

import csv
from collections import namedtuple
import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


PlateShape = namedtuple('PlateShape', 'rows cols')
PlateRows = namedtuple(
    'PlateRows', ('plate_id entry_date local_id rapid_plates notes results '
                  'row_A row_B row_C row_D row_E row_F row_G row_H end'))
PLATE_SHAPE = PlateShape(14, 13)
PLATE_ROWS = PlateRows(*list(range(15)))

COL_END = 1 + 12 + 1    # 1 Label column  + 12 plate columns + 1


def ingest_samples():
    """
    Get the Sample plates from the Google sheet.

    There is a fixed format to the plates:
                           Plate column 1  ...     Plate column 12
    plate_id:    UUID
    entry_date:  ISO_Date
    local_id:    Text
    rapid_plates:Text
    notes:       Text
    results:     Text
    Plate row A                UUID?          ...     UUID?
        .                        .            ...       .
        .                        .            ...       .
        .                        .            ...       .
    Plate row H                UUID?          ...     UUID?

    Method:
    1) We look for something in the plate_data column that is a UUID.
    2) We then take that row and the next n rows.
    """
    csv_path = util.TEMP_DATA / 'sample_plates.csv'
    google.sheet_to_csv('sample_plates', csv_path)

    sample_wells = []

    with open(csv_path, newline='') as csv_file:
        reader = csv.reader(csv_file)
        for csv_row in reader:
            if util.is_uuid(csv_row[0]):
                header = build_headers(csv_row, reader)
                build_wells(header, reader, sample_wells)

    sample_wells = pd.DataFrame(sample_wells)

    write_to_db(sample_wells)


def build_headers(csv_row, reader):
    """Build the well record header data."""
    header = {'plate_id': csv_row[0]}

    csv_row = next(reader)
    header['entry_date'] = csv_row[0]

    csv_row = next(reader)
    header['local_id'] = csv_row[0]
    header['local_no'] = util.build_local_no(csv_row[0])

    csv_row = next(reader)
    header['rapid_plates'] = csv_row[0]

    csv_row = next(reader)
    header['notes'] = csv_row[0]

    csv_row = next(reader)
    header['results'] = csv_row[0]

    return header


def build_wells(header, reader, sample_wells):
    """Build a well record for each sample ID."""
    well_no = 0
    for row in 'ABCDEFGH':
        csv_row = next(reader)

        for col in range(1, COL_END):
            sample_id = csv_row[col]
            well_no += 1

            if util.is_uuid(sample_id):
                sample_well = {
                    'row': row,
                    'col': col,
                    'sample_id': sample_id,
                    'well': f'{row}{col:02d}',
                    'well_no': well_no,
                }
                sample_wells.append({**header, **sample_well})


def write_to_db(sample_wells):
    """Output the sample wells to the database."""
    with db.connect() as cxn:
        sample_wells.to_sql(
            'sample_wells', cxn, if_exists='replace', index=False)

        cxn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                sample_wells_plate_id_well ON sample_wells (plate_id, well);
            """)


if __name__ == '__main__':
    ingest_samples()
