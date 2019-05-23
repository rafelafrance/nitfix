"""Extract, transform, & load Rapid sample sheets."""

import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


GOOGLE_SHEETS = [
    'FMN_131001_SampleSheet']
# 'KIB_135802_SampleSheet']


def ingest_sample_sheet(google_sheet):
    """Ingest one sample sheet."""
    cxn = db.connect()
    sample_sheet = get_sample_sheet(google_sheet)
    sample_sheet.to_sql(google_sheet, cxn, if_exists='replace', index=False)


def get_sample_sheet(google_sheet):
    """Get replated Rapid rata from Google sheet."""
    csv_path = util.TEMP_DATA / f'{google_sheet}.csv'

    google.sheet_to_csv(google_sheet, csv_path)

    sample_sheet = pd.read_csv(
        csv_path,
        header=0,
        na_filter=False,
        names=[
            'sample_code', 'sample_id', 'i5_barcode_seq', 'i7_barcode_seq',
            'seq_file', 'seq_cycle'])

    return sample_sheet.loc[sample_sheet['sample_code'] != '', :]


def merge_sample_sheets():
    """Create rapid sample sheet data table table."""
    cxn = db.connect()

    merged = None
    for sheet in GOOGLE_SHEETS:
        sheet = pd.read_sql(f'SELECT * from {sheet};', cxn)
        if merged is None:
            merged = sheet
        else:
            merged = merged.append(sheet, ignore_index=True)

    merged.to_sql('sample_sheets', cxn, if_exists='replace', index=False)


if __name__ == '__main__':
    for SHEET in GOOGLE_SHEETS:
        ingest_sample_sheet(SHEET)
    merge_sample_sheets()
