"""Extract, transform, & load Rapid reformatting templates."""

from os.path import splitext
import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


GOOGLE_SHEETS = [
    'FMN_131001_Reformatting_Template.xlsx',
    'KIB_135802_Reformatting_template']


def ingest_reformating_template(google_sheet):
    """Ingest one reformatting template."""
    cxn = db.connect()
    table, _ = splitext(google_sheet)
    reformat_wells = get_reformatted_wells(google_sheet)
    reformat_wells.to_sql(table, cxn, if_exists='replace', index=False)


def get_reformatted_wells(google_sheet):
    """Get replated Rapid rata from Google sheet."""
    table, _ = splitext(google_sheet)
    csv_path = util.TEMP_DATA / f'{table}.csv'

    google.sheet_to_csv(google_sheet, csv_path)

    reformat_wells = pd.read_csv(
        csv_path,
        header=0,
        na_filter=False,
        names=[
            'row_sort', 'source_plate', 'source_well', 'source_well_no',
            'dest_plate', 'dest_well', 'dest_well_no', 'volume', 'comments'])

    return reformat_wells.loc[reformat_wells['source_plate'] != '', :].copy()


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
    for SHEET in GOOGLE_SHEETS:
        ingest_reformating_template(SHEET)
    merge_reformating_templates()
