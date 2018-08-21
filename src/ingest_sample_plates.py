"""Extract, transform, and load data related to the samples."""

from collections import namedtuple
import pandas as pd
# import lib.db as db
import lib.google as google
import lib.util as util


PlateShape = namedtuple('PlateShape', 'rows cols')
PlateRows = namedtuple(
    'PlateRows', ('plate_id entry_date local_id rapid_plates notes results '
                  'row_A row_B row_C row_D row_E row_F row_G row_H end'))
PLATE_SHAPE = PlateShape(14, 13)
PLATE_ROWS = PlateRows(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)


def ingest_samples():
    """Ingest data related to the samples."""
    plate_groups = get_plate_groups()

    sample_plates = build_sample_plates(plate_groups)
    sample_rows = build_sample_rows(plate_groups)
    sample_wells = build_sample_wells(sample_rows)

    print(sample_plates.head())
    print()
    print(sample_rows.head(24))
    print()
    print(sample_wells.head())

    # cxn = db.connect()
    # wells = get_wells()
    # wells.to_sql('wells', cxn, if_exists='replace', index=False)


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
    csv_path = util.INTERIM_DATA / 'sample_plates.csv'
    col_names = (['col_A'] +
                 [f'col_{c:02d}' for c in range(1, PLATE_SHAPE.cols)])

    google.sheet_to_csv('sample_plates', csv_path)
    plates = pd.read_csv(csv_path, names=col_names, na_filter=False)

    # Find rows with a UUID in column A
    plates['in_plate'] = plates['col_A'].apply(util.is_uuid)

    # Now find the next N rows (N = rows per plate)
    for shift in range(1, PLATE_SHAPE.rows):
        plates.in_plate = (plates.in_plate |
                           plates.col_A.shift(shift).apply(util.is_uuid))

    # Remove rows not in a plate & re-index so we can use modular arithmetic
    plates = plates.loc[plates.in_plate, :].reset_index(drop=True)
    plates = plates.drop('in_plate', axis=1)  # Cleanup

    # Group by plate which is just a group of N rows
    return plates.groupby(plates.index // PLATE_SHAPE.rows)


def build_sample_plates(plate_groups):
    """Build the sample plates from the plate groups."""
    plates = []
    for idx, group in plate_groups:
        plates.append({
            'plate_id': group.iat[PLATE_ROWS.plate_id, 0],
            'entry_date': group.iat[PLATE_ROWS.entry_date, 0],
            'local_id': group.iat[PLATE_ROWS.local_id, 0],
            'rapid_plates': group.iat[PLATE_ROWS.rapid_plates, 0],
            'notes': group.iat[PLATE_ROWS.notes, 0],
            'results': group.iat[PLATE_ROWS.results, 0]})
    sample_plates = pd.DataFrame(plates)
    sample_plates['local_no'] = (pd.to_numeric(
        sample_plates.local_id.str.replace(r'\D+', ''), errors='coerce'))
    return sample_plates


def build_sample_rows(plate_groups):
    """Build the sample rows from the plate groups."""
    sample_rows = []
    for idx, group in plate_groups:
        rows = group.iloc[PLATE_ROWS.row_A:PLATE_ROWS.end, :].copy()
        rows['plate_id'] = group.iat[PLATE_ROWS.plate_id, 0]
        rows['row'] = rows.loc[:, 'col_A'].str[-1]
        sample_rows.append(rows)

    return pd.concat(sample_rows).drop('col_A', axis='columns')


def build_sample_wells(sample_rows):
    """Build the sample wells from the sample rows."""
    sample_wells = sample_rows.melt(
        id_vars=(c for c in sample_rows if not c.startswith('col_')),
        value_vars=(c for c in sample_rows if c.startswith('col_')))

    return sample_wells
    # plate_id sample_id row col well well_no


# # Get all of the per plate information into a data frame
# plates = []
# for i in range(6):
#     plate = sample_plates.iloc[i::rows_per_plate, [0]]
#     plate = plate.reset_index(drop=True)
#     plates.append(plate)
#
# plates = pd.concat(plates, axis=1, ignore_index=True)
#
# # Append per plate data to the per well data
# row_start = 6
# rows = 'ABCDEFGH'
# col_end = 13  # columns [1, 12] have data, i.e. [1, 13)
# wells = []
# for row in range(row_start, row_start + len(rows)):
#     for col in range(1, col_end):
#         well = pd.DataFrame(sample_plates.iloc[row::rows_per_plate, col])
#         well = well.reset_index(drop=True)
#         row_offset = row - row_start
#         well['row'] = rows[row_offset:row_offset + 1]
#         well['col'] = col
#         well = pd.concat([plates, well], axis=1, ignore_index=True)
#         wells.append(well)
#
# wells = (pd.concat(wells, axis=0, ignore_index=True)
#           .rename(columns={0: 'plate_id', 1: 'entry_date', 2: 'local_id',
#                             3: 'rapid_info', 4: 'notes', 5: 'results',
#                             6: 'sample_id', 7: 'row', 8: 'col'}))
# wells['well_no'] = wells.apply(
#   lambda well: 'ABCDEFGH'.find(well.row.upper()) * 12 + well.col, axis=1)
# wells['local_no'] = (pd.to_numeric(
#     wells.local_id.str.replace(r'\D+', ''), errors='coerce')
#     .fillna(0).astype('int'))
# wells['well'] = wells.apply(lambda w: f'{w.row}{w.col:02d}', axis=1)
#
# return wells


if __name__ == '__main__':
    ingest_samples()
