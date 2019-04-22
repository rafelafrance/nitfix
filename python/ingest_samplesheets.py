"""Extract, transform, & load RAPiD sample sheet."""

import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


def ingest_rapid_sample_sheet():
    """Ingest data related to the samples."""
    cxn = db.connect()
    sample_sheet = get_sample_sheet()
    create_rapid_sample_sheet_table(cxn, sample_sheet)


def create_rapid_sample_sheet_table(cxn, sample_sheet):
    """Create rapid sample sheet data table table."""
    sample_sheet.to_sql(
        'rapid_sample_sheet', cxn, if_exists='replace', index=False)

    cxn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            rapid_sample_sheet_sample_code
            ON rapid_sample_sheet (sample_code);
        """)


def get_sample_sheet():
    """Get replated Rapid rata from Google sheet."""
    csv_path = util.TEMP_DATA / 'rapid_sample_sheet.csv'

    google.sheet_to_csv('FMN_131001_SampleSheet', csv_path)

    sample_sheet = pd.read_csv(
        csv_path,
        header=0,
        na_filter=False,
        names=[
            'sample_code', 'sample_id', 'i5_barcode_seq', 'i7_barcode_seq',
            'seq_name', 'seq_cycle'])

    return sample_sheet.loc[sample_sheet.sample_code != '', :]


if __name__ == '__main__':
    ingest_rapid_sample_sheet()
