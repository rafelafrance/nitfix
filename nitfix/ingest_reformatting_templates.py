"""Extract, transform, & load Rapid reformatting templates."""

from os.path import splitext

import pandas as pd

import lib.db as db
import lib.google as google
import lib.util as util

NAMES = ['row_sort', 'source_plate', 'source_well', 'source_well_no',
         'dest_plate', 'dest_well', 'dest_well_no', 'volume', 'sample_id',
         'status']


def ingest_reformatting_template(sheet):
    """Ingest one reformatting template."""
    cxn = db.connect()
    wells = get_reformatted_wells(sheet, NAMES)
    wells.to_sql(sheet, cxn, if_exists='replace', index=False)


def get_reformatted_wells(sheet, names):
    """Get replated Rapid data from Google sheet."""
    table, _ = splitext(sheet)
    csv_path = util.TEMP_DATA / f'{table}.csv'

    google.sheet_to_csv(sheet, csv_path)

    wells = pd.read_csv(csv_path, header=0, na_filter=False, names=names)
    wells['sample_id'] = wells['sample_id'].str.lower()
    # wells = wells.drop_duplicates('sample_id', keep=False)

    return wells.loc[wells['source_plate'] != '', :].copy()


def merge_reformatting_templates():
    """Create rapid reformat data table table."""
    cxn = db.connect()

    merged = None
    for sheet in util.REFORMATTING_TEMPLATE_SHEETS:
        table, _ = splitext(sheet)
        sheet = pd.read_sql(f'SELECT * from {table};', cxn)
        if merged is None:
            merged = sheet
        else:
            merged = merged.append(sheet, ignore_index=True)

    merged['rapid_source'] = (
            merged['source_plate'] + '_' + merged['source_well'])

    merged['rapid_dest'] = merged['dest_plate'] + '_' + merged['dest_well']

    merged.to_sql(
        'reformatting_templates', cxn, if_exists='replace', index=False)


if __name__ == '__main__':
    for SHEET in util.REFORMATTING_TEMPLATE_SHEETS:
        ingest_reformatting_template(SHEET)
    merge_reformatting_templates()
