"""Extract, transform, & load Rapid sequencing metadata sheets."""

import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


GOOGLE_SHEETS = [
    'FMN_131001_Sequencing_Metadata']
# 'KIB_135802_Sequencing_Metadata']


def ingest_sequencing_sheet(google_sheet):
    """Ingest one sequencing metadata sheet."""
    cxn = db.connect()
    seq_sheet = get_sequencing_sheet(google_sheet)
    seq_sheet.to_sql(google_sheet, cxn, if_exists='replace', index=False)


def get_sequencing_sheet(google_sheet):
    """Get replated Rapid rata from Google sheet."""
    csv_path = util.TEMP_DATA / f'{google_sheet}.csv'

    google.sheet_to_csv(google_sheet, csv_path)

    seq_sheet = pd.read_csv(
        csv_path,
        header=0,
        na_filter=False,
        names=[
            'well', 'sample_id', 'local_plate', 'local_well', 'family',
            'genus', 'sci_name', 'total_dna', 'on_target_reads',
            'total_reads', 'on_target_percent', 'dedup_percent',
            'loci_assembled', 'hit_genus', 'hit_species', 'sample_genus',
            'sample_species', 'result', 'taxon_id_action'])

    return seq_sheet


def merge_sequencing_sheets():
    """Create rapid sample sheet data table table."""
    cxn = db.connect()

    merged = None
    for sheet in GOOGLE_SHEETS:
        sheet = pd.read_sql(f'SELECT * from {sheet};', cxn)
        if merged is None:
            merged = sheet
        else:
            merged = merged.append(sheet, ignore_index=True)

    merged.to_sql(
        'sequencing_metadata', cxn, if_exists='replace', index=False)


if __name__ == '__main__':
    for SHEET in GOOGLE_SHEETS:
        ingest_sequencing_sheet(SHEET)
    merge_sequencing_sheets()
