"""Utilities used to ingest all 'normal plates' sent to Rapid."""

from collections import defaultdict
import pandas as pd
import lib.db as db
import lib.util as util


def assign_plate_ids(rapid_wells):
    """
    Map Rapid source plate wells to sample plate and wells.

    Sample IDs will only work if the physical sample has been plated once.
    If the sample has been plated more then once we need to figure out which
    sample plate well the Rapid well actually points too.
    """
    cxn = db.connect()
    sample_ids = defaultdict(list)

    sql = """
        SELECT sample_id, plate_id, well
          FROM sample_wells
         WHERE length(sample_id) = 36;"""

    for row in cxn.execute(sql):
        sample_id, plate_id, well = row
        sample_ids[sample_id].append((plate_id, well))

    sample_wells = pd.read_sql('SELECT * FROM sample_wells;', cxn)
    sample_prints = _get_sample_fingerprints(sample_wells)
    rapid_prints = _get_rapid_fingerprints(rapid_wells)

    # Vectorizing this loop is more trouble than it's worth
    for idx, rapid_well in rapid_wells.iterrows():
        locations = sample_ids[rapid_well['sample_id']]

        if len(locations) == 1:
            where = locations[0]
        else:
            where = plate_id_heuristics(
                rapid_well, rapid_prints, sample_prints, locations)

        if where:
            plate_id, well = where
            rapid_wells.at[idx, 'plate_id'] = plate_id
            rapid_wells.at[idx, 'well'] = well

    return rapid_wells


def plate_id_heuristics(rapid_well, rapid_prints, sample_prints, locations):
    """
    Use the plate fingerprints to find the plate ID.

    1) Find Rapid plate's fingerprint using the Rapid source_plate and
    source_row. {(source_well, source_row) -> fingerprint}

    2) Find the sample plate and row using the fingerprint.
    {fingerprint -> {source_row's plate_id, row, list of sample_ids in order}}

    3) Get the first sample_id in the row that matches the Rapid sample_id.
       Use its index to get the column number.

    4) Blank out the sample_id in the list of sample_ids so the next one will
       be found when there are duplicate sample IDs in a row.

    NOTE: that rows can be permuted between the samples and what is sent to
    Rapid, so we need to sort the fingerprints of sample IDs.
    """
    rapid_key = (rapid_well['source_plate'], rapid_well['source_row'])
    fingerprint = rapid_prints.get(rapid_key)

    if not fingerprint or not util.is_uuid(rapid_well['sample_id']):
        return None

    sample_row = sample_prints.get(fingerprint)
    if not sample_row:
        print(rapid_key)
        return None

    col = sample_row['sample_ids'].index(rapid_well['sample_id'])

    sample_row['sample_ids'][col] = ''
    well = f"{sample_row['row']}{(col + 1):02d}"
    return sample_row['plate_id'], well


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
