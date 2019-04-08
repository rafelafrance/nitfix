"""Extract, transform, and load data related to the samples."""

import re
import pandas as pd
import lib.db as db
import lib.util as util
import lib.google as google


WELLS_PER_ROW = 12


def ingest_rapid_qc_wells():
    """Ingest data related to the samples."""
    cxn = db.connect()

    sample_wells = pd.read_sql('SELECT * FROM sample_wells;', cxn)
    rapid_wells = get_rapid_qc_wells()
    rapid_wells = assign_plate_ids(sample_wells, rapid_wells)

    create_rapid_qc_wells_table(cxn, rapid_wells)


def create_rapid_qc_wells_table(cxn, rapid_wells):
    """Create sample plate wells table table."""
    rapid_wells.to_sql(
        'rapid_qc_wells', cxn, if_exists='replace', index=False)

    cxn.executescript("""
        CREATE INDEX IF NOT EXISTS
            rapid_qc_wells_plate_id_well ON rapid_qc_wells (plate_id, well);
        CREATE INDEX IF NOT EXISTS
            rapid_qc_wells_sample_id ON rapid_qc_wells (sample_id);
        """)


def get_rapid_qc_wells():
    """Get data sent to Rapid from Google sheet."""
    csv_path = util.INTERIM_DATA / 'rapid_qc_data.csv'

    google.sheet_to_csv('FMN_131002_QC_Normal_Plate_Layout', csv_path)

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


def assign_plate_ids(sample_wells, rapid_wells):
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
    sample_prints = _get_sample_fingerprints(sample_wells)
    rapid_prints = _get_rapid_fingerprints(rapid_wells)

    # Vectorizing this loop is more trouble than it's worth
    for idx, rapid_well in rapid_wells.iterrows():
        rapid_key = (rapid_well.source_plate, rapid_well.source_row)
        fingerprint = rapid_prints.get(rapid_key)

        if not fingerprint or not util.is_uuid(rapid_well.sample_id):
            continue

        sample_row = sample_prints[fingerprint]
        col = sample_row['sample_ids'].index(rapid_well.sample_id)

        sample_row['sample_ids'][col] = ''

        rapid_wells.at[idx, 'plate_id'] = sample_row['plate_id']
        rapid_wells.at[idx, 'well'] = f"{sample_row['row']}{(col + 1):02d}"

    return rapid_wells


def _get_rapid_fingerprints(dfm):
    fingerprints = {}
    for _, well in dfm.iterrows():
        key = (well.source_plate, well.source_row)
        fingerprints.setdefault(key, [''] * 12)
        if util.is_uuid(well.sample_id):
            fingerprints[key][well.source_col - 1] = well.sample_id
    return {k: tuple(sorted(v)) for k, v in fingerprints.items() if any(v)}


def _get_sample_fingerprints(dfm):
    fingerprints = {}
    for _, well in dfm.iterrows():
        key = (well.plate_id, well.row)
        fingerprints.setdefault(key, [''] * 12)
        if util.is_uuid(well.sample_id):
            fingerprints[key][well.col - 1] = well.sample_id

    return {tuple(sorted(v)): {'plate_id': k[0], 'row': k[1], 'sample_ids': v}
            for k, v in fingerprints.items() if any(v)}


if __name__ == '__main__':
    ingest_rapid_qc_wells()
