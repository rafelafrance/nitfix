"""Extract, transform, & load Rapid sequencing loci assembled sheet."""

import pandas as pd

import lib.db as db
import lib.google as google
import lib.util as util

GOOGLE_SHEETS = [
    'P002-P077_Phylo_loci_assembled']


def ingest_loci_sheet(google_sheet):
    """Ingest one sequencing metadata sheet."""
    cxn = db.connect()
    seq_sheet = get_sequencing_sheet(google_sheet)
    seq_sheet.to_sql('loci_assembled', cxn, if_exists='replace', index=False)


def get_sequencing_sheet(google_sheet):
    """Get Rapid loci data from Google sheet."""
    csv_path = util.TEMP_DATA / f'{google_sheet}.csv'

    google.sheet_to_csv(google_sheet, csv_path)

    seq_sheet = pd.read_csv(
        csv_path,
        header=0,
        na_filter=False,
        names=['rapid_dest', 'loci_assembled'])

    return seq_sheet


if __name__ == '__main__':
    for SHEET in GOOGLE_SHEETS:
        ingest_loci_sheet(SHEET)
