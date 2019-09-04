"""Extract, transform, and load Sprent nodulation data."""

import pandas as pd
import lib.db as db
import lib.util as util


def ingest_sprent_data():
    """Read the Sprent nodulataion."""
    csv_path = util.RAW_DATA / 'sprent' / 'Nodulation_clade.csv'
    cxn = db.connect()

    data = pd.read_csv(
        csv_path,
        header=0,
        names=[
            'genus', 'species_count', 'nodulation_status',
            'nodulating_species', 'clade', 'legume_subfamily'])

    data.genus = data.genus.str.strip()

    create_sprent_table(cxn, data)


def create_sprent_table(cxn, data):
    """Create Genbank loci data table."""
    data.to_sql('sprent_data', cxn, if_exists='replace', index=False)

    cxn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            sprent_data_genus ON sprent_data (genus);
        """)


if __name__ == '__main__':
    ingest_sprent_data()
