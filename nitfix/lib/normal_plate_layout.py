"""Utilities used to ingest all 'normal plates' sent to Rapid."""

import re
from collections import defaultdict
import pandas as pd
import lib.db as db
import lib.util as util
import lib.google as google


def ingest_normal_plate_layout(google_sheet):
    """Extract, transform, and load samples sent to Rapid."""
    print(google_sheet)

    cxn = db.connect()

    rapid_wells = get_rapid_wells(google_sheet)
    rapid_wells = assign_plate_ids(rapid_wells)

    rapid_wells.to_sql(google_sheet, cxn, if_exists='replace', index=False)


def get_rapid_wells(google_sheet):
    """Get data sent to Rapid from Google sheet."""
    csv_path = util.TEMP_DATA / f'{google_sheet}.csv'

    google.sheet_to_csv(google_sheet, csv_path)

    rapid_wells = pd.read_csv(
        csv_path,
        skiprows=1,
        header=0,
        names=['row_sort', 'col_sort', 'rapid_id', 'sample_id',
               'old_concentration', 'volume', 'comments', 'concentration',
               'total_dna'])

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


def merge_normal_plate_layouts(google_sheets, table):
    """Combine the input sheets into one table."""
    cxn = db.connect()

    merged = None
    for sheet in google_sheets:
        sheet = pd.read_sql(f'SELECT * from {sheet};', cxn)
        if merged is None:
            merged = sheet
        else:
            merged = merged.append(sheet, ignore_index=True)

    merged.to_sql(table, cxn, if_exists='replace', index=False)


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
                    rapid_well, rapid_prints, sample_prints)

        if where:
            plate_id, well = where
            rapid_wells.at[idx, 'plate_id'] = plate_id
            rapid_wells.at[idx, 'well'] = well

    return rapid_wells


def plate_id_heuristics(rapid_well, rapid_prints, sample_prints):
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
