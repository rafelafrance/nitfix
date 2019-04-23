"""Extract, transform, and load samples QC'd by Rapid."""

import re
import pandas as pd
import lib.db as db
import lib.util as util
import lib.google as google
import lib.normal_plate_layout as normal_plate


def ingest_rapid_qc_wells():
    """Ingest data related to the samples."""
    cxn = db.connect()

    sample_wells = pd.read_sql('SELECT * FROM sample_wells;', cxn)
    rapid_wells = get_rapid_qc_wells()
    rapid_wells = normal_plate.assign_plate_ids(sample_wells, rapid_wells)

    # create_rapid_qc_wells_table(cxn, rapid_wells)


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
    csv_path = util.TEMP_DATA / 'rapid_qc_data.csv'

    google.sheet_to_csv('FMN_131002_QC_Normal_Plate_Layout', csv_path)

    rapid_wells = pd.read_csv(
        csv_path,
        skiprows=1,
        header=0,
        names=['row_sort', 'col_sort', 'rapid_id', 'sample_id', 'old_conc',
               'volume', 'comments', 'concentration', 'total_dna'])
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


if __name__ == '__main__':
    ingest_rapid_qc_wells()
