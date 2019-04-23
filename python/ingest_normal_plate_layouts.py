"""Extract, transform, and load samples sent to Rapid."""

import re
import pandas as pd
import lib.db as db
import lib.util as util
import lib.google as google
import lib.normal_plate_layout as normal_plate


TABLE = 'Normal_Plate_Layout'

GOOGLE_SHEETS = [
    'FMN_131001_Normal_Plate_Layout',
    'KIB_135801_Normal_Plate_Layout']


def ingest_normal_plate_layout(google_sheet):
    """Extract, transform, and load samples sent to Rapid."""
    cxn = db.connect()

    rapid_wells = get_rapid_wells(google_sheet)
    rapid_wells = normal_plate.assign_plate_ids(rapid_wells)

    rapid_wells.to_sql(google_sheet, cxn, if_exists='replace', index=False)


def get_rapid_wells(google_sheet):
    """Get data sent to Rapid from Google sheet."""
    csv_path = util.TEMP_DATA / f'{google_sheet}.csv'

    google.sheet_to_csv(google_sheet, csv_path)

    rapid_wells = pd.read_csv(
        csv_path,
        skiprows=1,
        header=0,
        names=['row_sort', 'col_sort', 'rapid_id', 'sample_id', 'old_conc',
               'volume', 'commentingest_rapid_qc_data.pys', 'concentration',
               'total_dna'])
    rapid_wells = rapid_wells.drop('old_conc', axis=1)

    source_plate = re.compile(r'^[A-Za-z]+_\d+_(P\d+)_W\w+$')
    rapid_wells['source_plate'] = rapid_wells.rapid_id.str.extract(
        source_plate, expand=False)

    source_well = re.compile(r'^[A-Za-z]+_\d+_P\d+_W(\w+)$')
    rapid_wells['source_well'] = rapid_wells.rapid_id.str.extract(
        source_well, expand=False)

    rapid_wells['source_row'] = rapid_wells['source_well'].str[0]
    rapid_wells['source_col'] = rapid_wells.source_well.str[1:].astype(int)

    rapid_wells['plate_id'] = ''
    rapid_wells['well'] = ''
    rapid_wells.sample_id = rapid_wells.sample_id.str.strip()

    return rapid_wells


def merge_normal_plate_layouts():
    """Combine the input sheets into one table."""
    cxn = db.connect()

    merged = None
    for sheet in GOOGLE_SHEETS:
        sheet = pd.read_sql(f'SELECT * from {sheet};', cxn)
        if merged is None:
            merged = sheet
        else:
            merged = merged.append(sheet, ignore_index=True)

    merged.to_sql(TABLE, cxn, if_exists='replace', index=False)


if __name__ == '__main__':
    for SHEET in GOOGLE_SHEETS:
        ingest_normal_plate_layout(SHEET)
        break
    merge_normal_plate_layouts()
