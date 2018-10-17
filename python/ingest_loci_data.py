"""Extract, transform, and load data related to genbank loci."""

import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


def ingest_loci_data():
    """Read the Genbank loci Google sheet."""
    csv_path = util.INTERIM_DATA / 'loci.csv'
    cxn = db.connect()

    google.sheet_to_csv('genbank_loci', csv_path)

    loci = pd.read_csv(
        csv_path,
        header=0,
        names=['sci_name', 'its', 'atpb', 'matk', 'matr', 'rbcl'])

    loci.sci_name = loci.sci_name.str.split().str.join(' ')

    create_genbank_loci_table(cxn, loci)


def create_genbank_loci_table(cxn, loci):
    """Create Genbank loci data table."""
    loci.to_sql('genbank_loci', cxn, if_exists='replace', index=False)

    sql = """CREATE UNIQUE INDEX IF NOT EXISTS
             genbank_loci_sci_name ON genbank_loci (sci_name)"""
    cxn.execute(sql)


if __name__ == '__main__':
    ingest_loci_data()
