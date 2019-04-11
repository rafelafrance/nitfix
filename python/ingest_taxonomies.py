"""Extract, transform, and load data related to the taxonomy."""

import pandas as pd
import lib.db as db
import lib.util as util
import lib.google as google


def ingest_taxonomy(google_sheet):
    """Ingest data related to the taxonomy."""
    cxn = db.connect()

    taxonomy = get_taxonomy(google_sheet)
    taxonomy = split_sample_ids(taxonomy)

    taxonomy.to_sql(google_sheet, cxn, if_exists='replace', index=False)
    create_taxon_ids_table(cxn, google_sheet, taxonomy)


def get_taxonomy(google_sheet):
    """Get the master taxonomy google sheet."""
    csv_path = util.TEMP_DATA / f'{google_sheet}.csv'

    google.sheet_to_csv(google_sheet, csv_path)

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


def create_taxon_ids_table(cxn, table, taxonomy):
    """Create a way to link sample IDs to the master taxonomy record."""
    table += '_ids'

    columns = [c for c in taxonomy.columns if c.startswith('sample_id_')]

    taxonomy_ids = taxonomy.melt(
        id_vars='sci_name', value_vars=columns, value_name='sample_id')
    not_na = taxonomy_ids.sample_id.notna()
    not_blank = taxonomy_ids.sample_id != ''
    taxonomy_ids = taxonomy_ids.loc[
        not_na & not_blank, ['sci_name', 'sample_id']]

    taxonomy_ids.to_sql(table, cxn, if_exists='replace', index=False)


def merge_taxonomies():
    """
    Merge Ting Shuang taxonomy with the master taxonomy.

    Note: The Ting Shuang taxonomy currently has duplicate scientific names so
    we need to make sure we don't propagate them.
    """
    cxn = db.connect()

    taxonomy = pd.read_sql('SELECT * FROM NitFixMasterTaxonomy', cxn)
    taxonomy.to_sql('taxonomy', cxn, if_exists='replace', index=False)
    cxn.executescript("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            taxonomy_sci_name ON taxonomy (sci_name);

        CREATE INDEX IF NOT EXISTS
            taxonomy_genus ON taxonomy (genus);

        CREATE INDEX IF NOT EXISTS
            taxonomy_family ON taxonomy (family);
        """)

    for i in range(1, 6):
        cxn.execute(f"""
            CREATE INDEX IF NOT EXISTS
                taxonomy_sample_id_{i} ON taxonomy (sample_id_{i});
        """)

    taxonomy_ids = pd.read_sql('SELECT * FROM NitFixMasterTaxonomy_ids', cxn)
    taxonomy_ids.to_sql('taxonomy_ids', cxn, if_exists='replace', index=False)
    cxn.executescript(f"""
        CREATE INDEX IF NOT EXISTS
            taxonomy_ids_sci_name ON taxonomy_ids (sci_name);

        CREATE INDEX IF NOT EXISTS
            taxonomy_ids_sample_id ON taxonomy_ids (sample_id);
        """)

    sql = """
        INSERT INTO taxonomy
        SELECT *
          FROM Tingshuang_NitFixMasterTaxonomy
         WHERE sci_name NOT IN (SELECT sci_name FROM taxonomy)
           AND rowid IN (SELECT rowid
                           FROM Tingshuang_NitFixMasterTaxonomy
                       GROUP BY sci_name
                         HAVING MIN(rowid));

        INSERT INTO taxonomy_ids
        SELECT *
          FROM Tingshuang_NitFixMasterTaxonomy_ids
         WHERE sample_id NOT IN (SELECT sample_id FROM taxonomy_ids);
        """
    cxn.executescript(sql)
    cxn.commit()


if __name__ == '__main__':
    ingest_taxonomy('NitFixMasterTaxonomy')
    ingest_taxonomy('Tingshuang_NitFixMasterTaxonomy')
    merge_taxonomies()
