"""Extract, transform, and load data related to the samples."""

import re
from pathlib import Path
import pandas as pd
import lib.db as db
import lib.google as google


INTERIM_DATA = Path('data') / 'interim'


def ingest_samples():
    """Ingest data related to the samples."""
    cxn = db.connect()

    wells = get_wells()
    picogreen = get_picogreen()
    rapid_input = get_rapid_input()
    rapid_wells = get_rapid_wells()

    wells.to_sql('wells', cxn, if_exists='replace', index=False)
    picogreen.to_sql('picogreen', cxn, if_exists='replace', index=False)
    rapid_input.to_sql('rapid_input', cxn, if_exists='replace', index=False)
    rapid_wells.to_sql('rapid_wells', cxn, if_exists='replace', index=False)


def get_wells():
    """
    Get the Sample plates from the Google sheet.

    Get the entered data from the sample_plates Google sheet.
    There is a fixed format to the plates:
                           Plate column 1  ...     Plate column 12
    plate_id:UUID
    entry_date:ISO_Date
    local_id:Text
    protocol:Text
    notes:Text
    results:Text
    Plate row A                UUID?          ...     UUID?
       .                        .            ...       .
       .                        .            ...       .
       .                        .            ...       .
    Plate row H                UUID?          ...     UUID?
    """
    step = 14
    csv_path = INTERIM_DATA / 'sample_plates.csv'

    google.sheet_to_csv('sample_plates', csv_path)

    sample_plates = pd.read_csv(csv_path)

    has_data = sample_plates['Plate ID'].notna()
    sample_plates = sample_plates[has_data]
    sample_plates = sample_plates.reset_index(drop=True)

    # Get all of the per plate information into a data frame
    plates = []
    for i in range(6):
        plate = sample_plates.iloc[i::step, [0]]
        plate = plate.reset_index(drop=True)
        plates.append(plate)

    plates = pd.concat(plates, axis=1, ignore_index=True)

    # Append per well information with the per plate information for each well
    row_start = 6
    rows = 'ABCDEFGH'
    wells = []
    for row in range(row_start, row_start + len(rows)):
        for col in range(1, 13):
            well = pd.DataFrame(sample_plates.iloc[row::step, col])
            well = well.reset_index(drop=True)
            row_offset = row - row_start
            well['row'] = rows[row_offset:row_offset + 1]
            well['col'] = col
            well = pd.concat([plates, well], axis=1, ignore_index=True)
            wells.append(well)

    wells = (pd.concat(wells, axis=0, ignore_index=True)
               .rename(columns={0: 'plate_id', 1: 'entry_date', 2: 'local_id',
                                3: 'protocol', 4: 'notes', 5: 'results',
                                6: 'sample_id', 7: 'row', 8: 'col'}))
    wells['well_no'] = wells.apply(
        lambda well: 'ABCDEFGH'.find(well.row.upper()) * 12 + well.col, axis=1)
    wells['local_no'] = wells.local_id.str.replace(r'\D+', '').astype('int')
    wells['well'] = wells.apply(lambda w: f'{w.row}{w.col:02d}', axis=1)
    wells['picogreen_id'] = wells.apply(
        lambda w: f'{w.local_no}_{w.well_no:02d}', axis=1)

    return wells


def get_picogreen():
    """Get the Picogreen data from the Google sheet."""
    csv_path = INTERIM_DATA / 'picogreen.csv'

    google.sheet_to_csv('picogreen_results', csv_path)

    picogreen = pd.read_csv(
        csv_path,
        header=0,
        names=[
            'picogreen_id', 'well', 'rfu', 'ng_microliter',
            'ng_microliter_mean', 'quant_method', 'quant_date', 'sample_id'])

    return picogreen


def get_rapid_input():
    """Get data sent to Rapid from Google sheet."""
    csv_path = INTERIM_DATA / 'rapid_input.csv'

    google.sheet_to_csv(
        'FMN_131001_QC_Normal_Plate_Layout.xlsx', csv_path)

    rapid_input = pd.read_csv(
        csv_path,
        header=0,
        skiprows=1,
        names=[
            'row_sort', 'col_sort', 'rapid_id', 'sample_id', 'concentration',
            'volume', 'comments', 'rapid_concentration', 'rapid_total_dna'])

    source_plate = re.compile(r'^[A-Za-z]+_\d+_(P\d+)_W\w+$')
    rapid_input['source_plate'] = rapid_input.rapid_id.str.extract(
        source_plate, expand=False)

    source_well = re.compile(r'^[A-Za-z]+_\d+_P\d+_W(\w+)$')
    rapid_input['source_well'] = rapid_input.rapid_id.str.extract(
        source_well, expand=False)

    return rapid_input


def get_rapid_wells():
    """Get replated Rapid rata from Google sheet."""
    csv_path = INTERIM_DATA / 'rapid_wells.csv'

    google.sheet_to_csv('Standard Reformatting Template.xlsx', csv_path)

    rapid_wells = pd.read_csv(
        csv_path,
        header=0,
        names=[
            'row_sort', 'source_plate', 'source_well', 'source_well_no',
            'dest_plate', 'dest_well', 'dest_well_no', 'volume', 'comments'])

    return rapid_wells


if __name__ == '__main__':
    ingest_samples()
