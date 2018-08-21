"""Extract, transform, and load data related to the samples."""

import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


def ingest_rapid_wells():
    """Ingest data related to the samples."""
    cxn = db.connect()
    rapid_wells = get_rapid_wells()
    rapid_wells.to_sql('rapid_wells', cxn, if_exists='replace', index=False)


def get_rapid_wells():
    """Get replated Rapid rata from Google sheet."""
    csv_path = util.INTERIM_DATA / 'rapid_wells.csv'

    google.sheet_to_csv('FMN_131001_Reformatting_Template.xlsx', csv_path)

    rapid_wells = pd.read_csv(
        csv_path,
        header=0,
        names=[
            'row_sort', 'source_plate', 'source_well', 'source_well_no',
            'dest_plate', 'dest_well', 'dest_well_no', 'volume', 'comments'])

    return rapid_wells


if __name__ == '__main__':
    ingest_rapid_wells()
