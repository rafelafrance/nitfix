"""Extract, transform, and load data related to the Werner data."""

import pandas as pd
import lib.db as db
import lib.util as util


def ingest_werner_data():
    """Ingest the Werner Excel sheet stored on Google drive."""
    cxn = db.connect()

    werner = read_werner_data()
    taxonomy = read_taxon_data(cxn)

    sci_names = taxonomy.sci_name.tolist()
    is_sci_name = werner.sci_name.isin(sci_names)

    synonyms = get_synonyms(taxonomy)
    is_synonym = werner.sci_name.isin(synonyms)

    update_it = ~is_sci_name & is_synonym

    werner.loc[update_it, 'sci_name'] = \
        werner.loc[update_it, 'sci_name'].apply(lambda x: synonyms[x])

    dups = werner.sci_name.duplicated()
    werner = werner.loc[~dups, :]

    create_werner_data_table(cxn, werner)


def create_werner_data_table(cxn, werner):
    """Create Werner data table."""
    werner.to_sql('werner_data', cxn, if_exists='replace', index=False)

    sql = """CREATE UNIQUE INDEX IF NOT EXISTS
             werner_data_sci_name ON werner_data (sci_name)"""
    cxn.execute(sql)


def read_werner_data():
    """Read the Werner Excel spreadsheet data."""
    excel_path = util.RAW_DATA / 'werner' / 'NitFixWernerEtAl2014.xlsx'

    drops = """Legume Likelihood_non-precursor
        Likelihood_precursor Likelihood_fixer Likelihood_stable_fixer
        Most_likely_state Corrected_lik_precursor
        Corrected_lik_stable_fixer""".split()

    werner = pd.read_excel(excel_path).drop(drops, axis=1).rename(columns={
        'NFC': 'nfc',
        'Species': 'sci_name',
        'Family': 'family_w',
        'Order': 'order'})

    is_nfc = werner.nfc == 'Yes'
    return werner[is_nfc]


def read_taxon_data(cxn):
    """Get the taxon data we need for making the synonyms."""
    sql = "SELECT sci_name, synonyms FROM taxonomy"
    return pd.read_sql(sql, cxn)


def get_synonyms(taxonomy):
    """Extract synonyms from the Master Taxonomy."""
    synonyms = taxonomy.synonyms.str.split(r'\s*[;,]\s*', expand=True)
    taxons = taxonomy[['sci_name', 'synonyms']]

    taxons = pd.concat([taxons, synonyms], axis=1)
    synonyms = taxons.melt(
        id_vars=['sci_name'],
        value_vars=synonyms.columns,
        value_name='synonym')

    synonyms = synonyms[synonyms.synonym.notna()].drop('variable', axis=1)
    synonyms = synonyms.set_index('synonym').sci_name.to_dict()

    return synonyms


if __name__ == '__main__':
    ingest_werner_data()