"""Extract, transform, and load Sprent nodulation data."""

import pandas as pd
import lib.db as db
import lib.util as util


def ingest_sprent_data():
    """Read the Sprent nodulataion."""
    csv_path = util.SPRENT_DATA_CSV
    cxn = db.connect()

    data = pd.read_csv(
        csv_path,
        header=0,
        names=[
            'genus', 'species_count', 'nodulation_status',
            'nodulating_species', 'clade', 'legume_subfamily'])

    # Split this record into two
    idx = data['genus'] == 'Fillaeopsis_and_Lemurodendron'
    data.loc[idx, 'genus'] = 'Fillaeopsis'
    data.loc[idx, 'species_count'] = 1

    row = data.loc[idx, :].copy()
    row['genus'] = 'Lemurodendron'
    data = data.append(row, ignore_index=True)

    # Remove stray records
    data = data[data.genus != 'Piptadenia viridiflora']
    data = data[data.genus != 'Otion']

    # Cleanup genus
    data['genus'] = data.genus.str.split().str[0]

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
