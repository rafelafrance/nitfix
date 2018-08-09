"""Extract, transform, and load data related to the taxonomy."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.google as google


INTERIM_DATA = Path('data') / 'interim'


def ingest_loci_data():
    """Read the Genbank loci Google sheet."""
    csv_path = INTERIM_DATA / 'loci.csv'
    cxn = db.connect()

    google.sheet_to_csv('genbank_loci', csv_path)

    loci = pd.read_csv(
        csv_path,
        header=0,
        names=['sci_name', 'its', 'atpb', 'matk', 'matr', 'rbcl'])

    loci.sci_name = loci.sci_name.str.split().str.join(' ')
    loci.to_sql('genbank_loci', cxn, if_exists='replace', index=False)


if __name__ == '__main__':
    ingest_loci_data()
