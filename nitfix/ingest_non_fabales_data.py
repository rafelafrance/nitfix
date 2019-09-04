"""Extract, transform, and load non-Fabales nodulation data."""

import pandas as pd
import lib.db as db
import lib.util as util


def ingest_non_fabales_data():
    """Read the non-Fabales nodulataion."""
    csv_path = util.RAW_DATA / 'non-fabales_nodulation.csv'
    cxn = db.connect()

    data = pd.read_csv(
        csv_path,
        header=0,
        names=['genus', 'nodulation_status', 'nodulation_type'])

    data.genus = data.genus.str.strip()

    create_genbank_loci_table(cxn, data)


def create_genbank_loci_table(cxn, data):
    """Create Genbank loci data table."""
    data.to_sql('non_fabales_data', cxn, if_exists='replace', index=False)

    cxn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            non_fabales_data_genus ON non_fabales_data (genus);
        """)


if __name__ == '__main__':
    ingest_non_fabales_data()
