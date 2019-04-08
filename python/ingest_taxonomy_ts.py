"""Extract, transform, and load data related to the Ting Shuang taxonomy."""

import pandas as pd
import lib.db as db
import lib.util as util
import lib.google as google


def ingest_taxonomy_ts():
    """Ingest data related to the taxonomy."""
    cxn = db.connect()

    taxonomy_ts = get_taxonomy_ts()
    taxonomy_ts = split_sample_ids(taxonomy_ts)

    create_taxonomy_table(cxn, taxonomy_ts)
    create_taxon_ids_table(cxn, taxonomy_ts)
    merge_with_master_taxonomy(cxn)


def merge_with_master_taxonomy(cxn):
    """Merge Ting Shuang taxonomy into the master taxonomy."""
    sql = """
        INSERT INTO taxonomy
        SELECT *
          FROM taxonomy_ts
         WHERE sci_name NOT IN (SELECT sci_name FROM taxonomy)
           AND rowid IN (SELECT rowid
                           FROM taxonomy_ts
                       GROUP BY sci_name
                         HAVING MIN(rowid));
        INSERT INTO taxonomy_ids
        SELECT *
          FROM taxonomy_ids_ts
         WHERE sample_id NOT IN (SELECT sample_id FROM taxonomy_ids);
        """
    cxn.executescript(sql)
    cxn.commit()


def create_taxonomy_table(cxn, taxonomy_ts):
    """Create taxonomy_ts table."""
    taxonomy_ts.to_sql('taxonomy_ts', cxn, if_exists='replace', index=False)

    cxn.executescript("""
        CREATE INDEX IF NOT EXISTS
            taxonomy_ts_sci_name ON taxonomy_ts (sci_name);
        CREATE INDEX IF NOT EXISTS
            taxonomy_ts_genus ON taxonomy_ts (genus);
        CREATE INDEX IF NOT EXISTS
            taxonomy_ts_family ON taxonomy_ts (family);
        """)

    for i in range(1, 6):
        cxn.execute(f"""
            CREATE INDEX IF NOT EXISTS
                taxonomy_ts_sample_id_{i} ON taxonomy_ts (sample_id_{i});
            """)


def get_taxonomy_ts():
    """Get the Ting Shuang taxonomy google sheet."""
    csv_path = util.INTERIM_DATA / 'taxonomy_ts.csv'

    google.sheet_to_csv('Tingshuang_NitFixMasterTaxonomy', csv_path)

    taxonomy_ts = pd.read_csv(
        csv_path,
        header=0,
        names=[
            'column_a', 'family', 'sci_name', 'authority', 'synonyms',
            'sample_ids', 'provider_acronym', 'provider_id', 'quality_notes'])
    taxonomy_ts = taxonomy_ts[taxonomy_ts.sci_name.notna()]

    taxonomy_ts.sci_name = \
        taxonomy_ts.sci_name.str.split().str.join(' ')
    taxonomy_ts['genus'] = taxonomy_ts.sci_name.str.split().str[0]

    return taxonomy_ts


def split_sample_ids(taxonomy_ts):
    """Split the sample IDs so each one in a row has its own column."""
    # Fix inconsistent sample IDs: Remove extra spaces and lower case them
    taxonomy_ts.sample_ids = (taxonomy_ts.sample_ids.str.lower()
                              .str.split().str.join(' '))

    split_ids = taxonomy_ts.sample_ids.str.split(r'\s*[;,]\s*', expand=True)
    id_cols = {i: f'sample_id_{i + 1}' for i in split_ids.columns}
    split_ids = split_ids.rename(columns=id_cols)

    taxonomy_ts = pd.concat([taxonomy_ts, split_ids], axis=1)

    return taxonomy_ts


def create_taxon_ids_table(cxn, taxonomy_ts):
    """Create a way to link sample IDs to the taxonomy record."""
    columns = [c for c in taxonomy_ts.columns if c.startswith('sample_id_')]

    taxonomy_ids_ts = taxonomy_ts.melt(
        id_vars='sci_name', value_vars=columns, value_name='sample_id')
    not_na = taxonomy_ids_ts.sample_id.notna()
    not_blank = taxonomy_ids_ts.sample_id != ''
    taxonomy_ids_ts = taxonomy_ids_ts.loc[
        not_na & not_blank, ['sci_name', 'sample_id']]

    taxonomy_ids_ts.to_sql(
        'taxonomy_ids_ts', cxn, if_exists='replace', index=False)

    cxn.executescript("""
        CREATE INDEX IF NOT EXISTS
            taxon_ids_ts_sci_name ON taxonomy_ids_ts (sci_name);
        CREATE INDEX IF NOT EXISTS
            taxon_ids_ts_sample_id ON taxonomy_ids_ts (sample_id);
        """)


if __name__ == '__main__':
    ingest_taxonomy_ts()
