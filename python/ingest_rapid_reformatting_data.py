"""Extract, transform, & load RAPiD reformatting data."""

import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


def ingest_rapid_reformat_data():
    """Ingest data related to the samples."""
    cxn = db.connect()
    reformat_data = get_reformatted_wells()
    create_rapid_reformat_data_table(cxn, reformat_data)


def create_rapid_reformat_data_table(cxn, reformat_data):
    """Create rapid reformat data table table."""
    reformat_data.to_sql(
        'rapid_reformat_data', cxn, if_exists='replace', index=False)

    cxn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            rapid_reformat_data_row_sort
            ON rapid_reformat_data (row_sort);
        """)


def get_reformatted_wells():
    """Get replated Rapid rata from Google sheet."""
    csv_path = util.INTERIM_DATA / 'rapid_reformat_data.csv'

    google.sheet_to_csv('FMN_131001_Reformatting_Template.xlsx', csv_path)

    reformat_data = pd.read_csv(
        csv_path,
        header=0,
        na_filter=False,
        names=[
            'row_sort', 'source_plate', 'source_well', 'source_well_no',
            'dest_plate', 'dest_well', 'dest_well_no', 'volume', 'comments'])

    return reformat_data.loc[reformat_data.source_plate != '', :]


if __name__ == '__main__':
    ingest_rapid_reformat_data()
