"""Extract, transform, and load the priority taxa list."""

import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


def ingest_priority_taxa_list():
    """Read the priority taxa list Google sheet."""
    csv_path = util.INTERIM_DATA / 'priority_taxa.csv'
    cxn = db.connect()

    google.sheet_to_csv('Priority_Taxa_List', csv_path)

    taxa = pd.read_csv(
        csv_path,
        header=0,
        na_filter=False,
        names=['family', 'subclade', 'genus', 'position', 'priority'])

    create_priority_taxa_table(cxn, taxa)


def create_priority_taxa_table(cxn, taxa):
    """Create the priority taxa table."""
    taxa.to_sql('priority_taxa', cxn, if_exists='replace', index=False)

    sql = """CREATE UNIQUE INDEX IF NOT EXISTS
             priority_taxa_family_genus ON priority_taxa (family, genus)"""
    cxn.execute(sql)


if __name__ == '__main__':
    ingest_priority_taxa_list()
