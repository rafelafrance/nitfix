"""Extract, transform, & load Rapid reformatting templates."""

from os.path import splitext
import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


GOOGLE_SHEETS = {
    'FMN_131001_Reformatting_Template': [
        'row_sort', 'source_project_id', 'source_plate',
        'source_well', 'source_well_no', 'dest_project_id',
        'dest_plate', 'dest_well', 'dest_well_no', 'volume',
        'sample_id'],
    'KIB_135802_Reformatting_template': [
        'row_sort', 'source_plate', 'source_well', 'source_well_no',
        'dest_plate', 'dest_well', 'dest_well_no', 'volume',
        'sample_id']}


def ingest_reformating_template(sheet, names):
    """Ingest one reformatting template."""
    cxn = db.connect()
    wells = get_reformatted_wells(sheet, names)
    wells.to_sql(sheet, cxn, if_exists='replace', index=False)


def get_reformatted_wells(sheet, names):
    """Get replated Rapid rata from Google sheet."""
    table, _ = splitext(sheet)
    csv_path = util.TEMP_DATA / f'{table}.csv'

    google.sheet_to_csv(sheet, csv_path)

    wells = pd.read_csv(csv_path, header=0, na_filter=False, names=names)

    # ########################################################################
    # The format of the sheets has changed asymmetrically

    if 'source_project_id' not in names:
        wells['source_project_id'] = (wells['source_plate'].str.split('_')
                                      .str[:2].str.join('_'))
        wells['source_plate'] = wells['source_plate'].str.split('_').str[2]

    if 'dest_project_id' not in names:
        wells['dest_project_id'] = (wells['dest_plate'].str.split('_')
                                    .str[:2].str.join('_'))
        wells['dest_plate'] = wells['dest_plate'].str.split('_').str[2]

    wells = wells.reindex(
        GOOGLE_SHEETS['FMN_131001_Reformatting_Template'], axis='columns')

    # ########################################################################

    return wells.loc[wells['source_plate'] != '', :].copy()


def merge_reformating_templates():
    """Create rapid reformat data table table."""
    cxn = db.connect()

    merged = None
    for sheet in GOOGLE_SHEETS:
        table, _ = splitext(sheet)
        sheet = pd.read_sql(f'SELECT * from {table};', cxn)
        if merged is None:
            merged = sheet
        else:
            merged = merged.append(sheet, ignore_index=True)

    merged.to_sql(
        'reformatting_templates', cxn, if_exists='replace', index=False)


if __name__ == '__main__':
    for SHEET, NAMES in GOOGLE_SHEETS.items():
        ingest_reformating_template(SHEET, NAMES)
    merge_reformating_templates()
