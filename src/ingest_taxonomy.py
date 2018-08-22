"""Extract, transform, and load data related to the taxonomy."""

import pandas as pd
import lib.db as db
import lib.util as util
import lib.google as google


def ingest_taxonomy():
    """Ingest data related to the taxonomy."""
    cxn = db.connect()

    taxonomy = get_master_taxonomy()
    taxonomy = split_sample_ids(taxonomy)

    create_taxonomy_table(cxn, taxonomy)
    create_taxon_ids_table(cxn, taxonomy)


def create_taxonomy_table(cxn, taxonomy):
    """Create taxonomy table."""
    taxonomy.to_sql('taxonomy', cxn, if_exists='replace', index=False)

    sql = """CREATE UNIQUE INDEX IF NOT EXISTS
             taxonomy_sci_name ON taxonomy (sci_name)"""
    cxn.execute(sql)

    sql = """CREATE INDEX IF NOT EXISTS
             taxonomy_genus ON taxonomy (genus)"""
    cxn.execute(sql)

    sql = """CREATE INDEX IF NOT EXISTS
             taxonomy_family ON taxonomy (family)"""
    cxn.execute(sql)


def get_master_taxonomy():
    """Get the master taxonomy google sheet."""
    csv_path = util.INTERIM_DATA / 'taxonomy.csv'

    google.sheet_to_csv('NitFixMasterTaxonomy', csv_path)

    taxonomy = pd.read_csv(
        csv_path,
        header=0,
        names=[
            'column_a', 'family', 'sci_name', 'authority', 'synonyms',
            'sample_ids', 'provider_acronym', 'provider_id', 'quality_notes'])
    taxonomy = taxonomy[taxonomy.sci_name.notna()]

    taxonomy.sci_name = \
        taxonomy.sci_name.str.split().str.join(' ')
    taxonomy['genus'] = taxonomy.sci_name.str.split().str[0]

    return taxonomy


def split_sample_ids(taxonomy):
    """Split the sample IDs so each one in a row has its own column."""
    # Fix inconsistent sample IDs: Remove extra spaces and lower case them
    taxonomy.sample_ids = (taxonomy.sample_ids.str.lower()
                           .str.split().str.join(' '))

    split_ids = taxonomy.sample_ids.str.split(r'\s*[;,]\s*', expand=True)
    id_cols = {i: f'sample_id_{i + 1}' for i in split_ids.columns}
    split_ids = split_ids.rename(columns=id_cols)

    taxonomy = pd.concat([taxonomy, split_ids], axis=1)

    return taxonomy


def create_taxon_ids_table(cxn, taxonomy):
    """Create a way to link sample IDs to the master taxonomy record."""
    columns = [c for c in taxonomy.columns if c.startswith('sample_id_')]
    taxon_ids = taxonomy.melt(
        id_vars='sci_name', value_vars=columns, value_name='sample_id')
    taxon_ids = taxon_ids.loc[
        taxon_ids.sample_id.notna(), ['sci_name', 'sample_id']]
    taxon_ids.to_sql('taxon_ids', cxn, if_exists='replace', index=False)

    sql = """CREATE INDEX IF NOT EXISTS
             taxon_ids_sci_name ON taxon_ids (sci_name)"""
    cxn.execute(sql)

    sql = """CREATE INDEX IF NOT EXISTS
             taxon_ids_sample_id ON taxon_ids (sample_id)"""
    cxn.execute(sql)


if __name__ == '__main__':
    ingest_taxonomy()
