"""Extract, transform, and load data related to the samples."""

import re
import pandas as pd
import lib.db as db
import lib.util as util
import lib.google as google


def ingest_samples():
    """Ingest data related to the samples."""
    cxn = db.connect()
    samples = select_samples(cxn)
    rapid_input = get_rapid_input()
    rapid_input = assign_plate_ids(samples, rapid_input)
    rapid_input.to_sql('rapid_input', cxn, if_exists='replace', index=False)


def get_rapid_input():
    """Get data sent to Rapid from Google sheet."""
    csv_path = util.INTERIM_DATA / 'rapid_input.csv'

    google.sheet_to_csv(
        'FMN_131001_QC_Normal_Plate_Layout.xlsx', csv_path)

    rapid_input = pd.read_csv(
        csv_path,
        header=0,
        skiprows=1,
        names=[
            'row_sort', 'col_sort', 'rapid_id', 'sample_id', 'old_conc',
            'volume', 'comments', 'concentration', 'total_dna'])
    rapid_input = rapid_input.drop('old_conc', axis=1)

    source_plate = re.compile(r'^[A-Za-z]+_\d+_(P\d+)_W\w+$')
    rapid_input['source_plate'] = rapid_input.rapid_id.str.extract(
        source_plate, expand=False)

    source_well = re.compile(r'^[A-Za-z]+_\d+_P\d+_W(\w+)$')
    rapid_input['source_well'] = rapid_input.rapid_id.str.extract(
        source_well, expand=False)

    return rapid_input


def assign_plate_ids(samples, rapid_input):
    """
    Map Rapid source plate wells to sample plate and well.

    1) Find RAPiD plate's fingerprint using the RAPiD source_plate and
    source_row. {(source_well, source_row) -> fingerprint}

    2) Find the sample plate and row using the fingerprint.
    {fingerprint -> {source_row's plate_id, row, list of sample_ids in order}}

    3) Get the first sample_id in the row that matches the RAPiD sample_id. Use
       its index to get the column number.

    4) Blank out the sample_id in the list of sample_ids so the next one will
       be found when there are duplicate sample IDs in a row.

    NOTE: that rows can be permuted between the samples and what is sent to
    Rapid, so we need to sort the fingerprint of sample IDs.
    """
    rapid_input['source_row'] = rapid_input.source_well.str[0]
    rapid_input['source_col'] = rapid_input.source_well.str[1:].astype(int)

    sample_prints = _get_sample_fingerprints(samples)
    rapid_prints = _get_rapid_fingerprints(rapid_input)

    rapid_input['plate_id'] = ''
    rapid_input['well'] = ''

    # Vectorizing this loop is more trouble than it's worth
    for idx, rapid_well in rapid_input.iterrows():
        rapid_key = (rapid_well.source_plate, rapid_well.source_row)
        fingerprint = rapid_prints.get(rapid_key)

        if not fingerprint or not util.is_uuid(rapid_well.sample_id):
            continue

        sample_row = sample_prints[fingerprint]
        col = sample_row['sample_ids'].index(rapid_well.sample_id)

        sample_row['sample_ids'][col] = ''

        rapid_input.at[idx, 'plate_id'] = sample_row['plate_id']
        rapid_input.at[idx, 'well'] = f"{sample_row['row']}{(col + 1):02d}"

    return rapid_input


def _get_rapid_fingerprints(df):
    fingerprints = {}
    for _, row in df.iterrows():
        key = (row.source_plate, row.source_row)
        fingerprints.setdefault(key, [''] * 12)
        if util.is_uuid(row.sample_id):
            fingerprints[key][row.source_col - 1] = row.sample_id
    return {k: tuple(sorted(v)) for k, v in fingerprints.items() if any(v)}


def _get_sample_fingerprints(df):
    fingerprints = {}
    for _, row in df.iterrows():
        key = (row.plate_id, row.row)
        fingerprints.setdefault(key, [''] * 12)
        if util.is_uuid(row.sample_id):
            fingerprints[key][row.col - 1] = row.sample_id

    return {tuple(sorted(v)): {'plate_id': k[0], 'row': k[1], 'sample_ids': v}
            for k, v in fingerprints.items() if any(v)}


def select_samples(cxn):
    """Get samples from the database."""
    return pd.read_sql('SELECT * FROM samples', cxn)


if __name__ == '__main__':
    ingest_samples()
