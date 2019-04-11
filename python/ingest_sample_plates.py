"""Extract, transform, and load data related to the samples."""

import re
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
PLATE_ROWS = PlateRows(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
LOCAL_ID = re.compile(
    r'^.*? (nitfix|rosales|test) \D* (\d+) \D*$',
    re.IGNORECASE | re.VERBOSE)


def ingest_samples():
    """Ingest data related to the samples."""
    plate_groups = get_plate_groups()

    sample_rows = build_sample_rows(plate_groups)
    sample_wells = build_sample_wells(sample_rows)

    cxn = db.connect()
    create_sample_wells_table(cxn, sample_wells)


def create_sample_wells_table(cxn, sample_wells):
    """Create sample plate wells table table."""
    sample_wells.to_sql(
        'sample_wells', cxn, if_exists='replace', index=False)

    cxn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            sample_wells_plate_id_well ON sample_wells (plate_id, well);
        """)


def get_plate_groups():
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
    col_names = (['col_A']
                 + [f'col_{c:02d}' for c in range(1, PLATE_SHAPE.cols)])

    google.sheet_to_csv('sample_plates', csv_path)
    plates = pd.read_csv(csv_path, names=col_names, na_filter=False)

    # Find rows with a UUID in column A. This is the plate ID & starts a plate.
    plates['in_plate'] = plates['col_A'].apply(util.is_uuid)

    # Now find the next N rows (N = rows per plate)
    for shift in range(1, PLATE_SHAPE.rows):
        plates.in_plate = (plates.in_plate
                           | plates.col_A.shift(shift).apply(util.is_uuid))

    # Remove rows not in a plate & re-index so we can use modular arithmetic
    plates = plates.loc[plates.in_plate, :].reset_index(drop=True)
    plates = plates.drop('in_plate', axis=1)  # Cleanup

    # Group by plate which is just a group of N rows
    return plates.groupby(plates.index // PLATE_SHAPE.rows)


def build_sample_rows(plate_groups):
    """Build the sample rows from the plate groups."""
    sample_rows = []
    for _, group in plate_groups:
        rows = group.iloc[PLATE_ROWS.row_A:PLATE_ROWS.end, :].copy()
        rows['plate_id'] = group.iat[PLATE_ROWS.plate_id, 0]
        rows['entry_date'] = group.iat[PLATE_ROWS.entry_date, 0]
        rows['local_id'] = group.iat[PLATE_ROWS.local_id, 0]
        rows['rapid_plates'] = group.iat[PLATE_ROWS.rapid_plates, 0]
        rows['notes'] = group.iat[PLATE_ROWS.notes, 0]
        rows['results'] = group.iat[PLATE_ROWS.results, 0]
        rows['row'] = rows.loc[:, 'col_A'].str[-1]  # = row's letter
        sample_rows.append(rows)

    sample_rows = pd.concat(sample_rows).drop('col_A', axis='columns')

    sample_rows['local_no'] = sample_rows.local_id.apply(build_local_no)
    # sample_rows['local_no'] = (pd.to_numeric(
    #     sample_rows.local_id.str.replace(r'\D+', ''), errors='coerce'))

    return sample_rows


def build_local_no(local_id):
    """Convert the local_id into something we can sort on consistently."""
    match = LOCAL_ID.match(local_id)
    lab = match[1].title()
    number = match[2].zfill(4)
    return f'{lab}_{number}'


def build_sample_wells(sample_rows):
    """Build the sample wells from the sample rows."""
    sample_wells = sample_rows.melt(
        id_vars=(c for c in sample_rows if not c.startswith('col_')),
        value_vars=(c for c in sample_rows if c.startswith('col_')),
        value_name='sample_id',
        var_name='col')

    sample_wells['well'] = sample_wells['row'] + sample_wells['col'].str[-2:]
    sample_wells['col'] = sample_wells['col'].str[-2:].astype('int')

    rows = 'ABCDEFGH'
    wells_per_row = 12
    sample_wells['well_no'] = sample_wells.apply(
        lambda well: rows.find(well.row) * wells_per_row + well.col, axis=1)
    sample_wells.sample_id = sample_wells.sample_id.str.strip()

    return sample_wells


if __name__ == '__main__':
    ingest_samples()
