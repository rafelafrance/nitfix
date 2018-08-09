"""Extract, transform, and load data related to the taxonomy."""

import os
import re
from pathlib import Path
import numpy as np
import pandas as pd
from pandas.api.types import is_number
import lib.db as db
import lib.util as util
import lib.google as google


INTERIM_DATA = Path('data') / 'interim'
EXTERNAL_DATA = Path('data') / 'external'
EXPEDITION_DATA = Path('data') / 'raw' / 'expeditions'


def ingest_taxonomy():
    """Ingest data related to the taxonomy."""
    cxn = db.connect()

    taxonomy, split_ids = get_master_taxonomy()
    taxon_ids = link_images_to_taxonomy(cxn, taxonomy, split_ids)
    taxonomy = merge_taxons_and_ids(taxonomy, taxon_ids)
    taxonomy = merge_genbank_loci_data(taxonomy)
    taxonomy = merge_werner_data(taxonomy)
    taxon_ids, expeditions = get_expedition_data(taxon_ids)

    taxonomy.to_sql('taxonomy', cxn, if_exists='replace', index=False)
    taxon_ids.to_sql('taxon_ids', cxn, if_exists='replace', index=False)
    expeditions.to_sql('expeditions', cxn, if_exists='replace', index=False)


def get_master_taxonomy():
    """Get the master taxonomy google sheet."""
    csv_path = INTERIM_DATA / 'taxonomy.csv'

    google.sheet_to_csv('NitFixMasterTaxonomy', csv_path)

    taxonomy = pd.read_csv(
        csv_path,
        header=0,
        names=['column_a', 'family', 'scientific_name', 'authority',
               'synonyms', 'ids', 'provider_acronym', 'provider_id',
               'quality_notes'])
    taxonomy = taxonomy[taxonomy.scientific_name.notna()]

    taxonomy.scientific_name = \
        taxonomy.scientific_name.str.split().str.join(' ')
    taxonomy.ids = taxonomy.ids.str.lower().str.split().str.join(' ')
    taxonomy['genus'] = taxonomy.scientific_name.str.split().str[0]

    split_ids = taxonomy.ids.str.split(r'\s*[;,]\s*', expand=True)
    id_cols = {i: f'id_{i + 1}' for i in split_ids.columns}
    split_ids = split_ids.rename(columns=id_cols)

    taxonomy = pd.concat([taxonomy, split_ids], axis=1)

    return taxonomy, split_ids


def link_images_to_taxonomy(cxn, taxonomy, split_ids):
    """Link Images to Taxonomy IDs."""
    images = pd.read_sql('SELECT * FROM images', cxn)

    taxon_ids = taxonomy.melt(
        id_vars='scientific_name',
        value_vars=split_ids.columns,
        value_name='sample_id').drop('variable', axis=1)

    taxon_ids.sample_id = taxon_ids.sample_id.str.split().str.join(' ')
    has_id = taxon_ids.sample_id.apply(lambda x: util.is_uuid(x))
    taxon_ids = taxon_ids[has_id]

    taxon_ids = taxon_ids.merge(right=images, how='left', on='sample_id')

    return taxon_ids


def merge_taxons_and_ids(taxonomy, taxon_ids):
    """Merge per ID data into the taxonomy data."""
    groups = taxon_ids.groupby('scientific_name').aggregate({
        'sample_id': join_aggregate, 'image_file': join_aggregate})

    taxonomy = taxonomy.merge(
        right=groups, how='left', left_on='scientific_name', right_index=True)

    return taxonomy


def merge_genbank_loci_data(taxonomy):
    """Read the Genbank loci Google sheet."""
    csv_path = INTERIM_DATA / 'loci.csv'

    google.sheet_to_csv('genbank_loci', csv_path)

    loci = pd.read_csv(
        csv_path,
        header=0,
        names=['scientific_name', 'its', 'atpb', 'matk', 'matr', 'rbcl'])
    loci.scientific_name = loci.scientific_name.str.split().str.join(' ')

    taxonomy = taxonomy.merge(right=loci, how='left', on='scientific_name')

    return taxonomy


def merge_werner_data(taxonomy):
    """Read the Werner Excel sheet stored on Google drive."""
    sci_names = taxonomy.scientific_name.tolist()
    synonyms = get_synonyms(taxonomy)
    werner = read_werner_data()

    is_sci_name = werner.scientific_name.isin(sci_names)
    is_synonym = werner.scientific_name.isin(synonyms)
    update_it = ~is_sci_name & is_synonym
    werner.loc[update_it, 'scientific_name'] = \
        werner.loc[update_it, 'scientific_name'].apply(lambda x: synonyms[x])

    taxonomy = taxonomy.merge(right=werner, how='left', on='scientific_name')
    return taxonomy


def read_werner_data():
    """Read the Werner Excel spreadsheet data."""
    excel_path = EXTERNAL_DATA / 'NitFixWernerEtAl2014.xlsx'
    werner = pd.read_excel(excel_path)
    drops = """Legume Likelihood_non-precursor
        Likelihood_precursor Likelihood_fixer Likelihood_stable_fixer
        Most_likely_state Corrected_lik_precursor
        Corrected_lik_stable_fixer""".split()
    werner = werner.drop(drops, axis=1).rename(columns={
        'NFC': 'nfc',
        'Species': 'scientific_name',
        'Family': 'family_w',
        'Order': 'order'})
    is_nfc = werner.nfc == 'Yes'
    return werner[is_nfc]


def get_synonyms(taxonomy):
    """Extract synonyms from the Master Taxonomy."""
    synonyms = taxonomy.synonyms.str.split(r'\s*[;,]\s*', expand=True)
    taxons = taxonomy[['scientific_name', 'synonyms']]

    taxons = pd.concat([taxons, synonyms], axis=1)
    synonyms = taxons.melt(
        id_vars=['scientific_name'],
        value_vars=synonyms.columns,
        value_name='synonym')

    synonyms = synonyms[synonyms.synonym.notna()].drop('variable', axis=1)
    synonyms = synonyms.set_index('synonym').scientific_name.to_dict()

    return synonyms


def get_expedition_data(taxon_ids):
    """Get NitFix 1 expedition data."""
    csv_path = os.fspath(EXPEDITION_DATA / '5657_Nit_Fix_I.reconcile.4.2.csv')

    expedition_01 = pd.read_csv(csv_path)
    columns = {}
    for old in expedition_01.columns:
        new = old.lower()
        new = new.replace('⁰', 'deg')
        new = new.replace("''", 'sec')
        new = new.replace("'", 'min')
        new = re.sub(r'[^a-z0-9_]+', '_', new)
        new = re.sub(r'^_|_$', '', new)
        columns[old] = new
    columns['subject_qr_code'] = 'sample_id'

    expeditions = expedition_01.rename(columns=columns)

    taxon_ids = taxon_ids.merge(right=expeditions, how='left', on='sample_id')

    return taxon_ids, expeditions


def join_columns(columns, row):
    """Combine data from multiple columns into a single value."""
    value = ','.join([row[c] for c in columns if not is_number(row[c])])
    return value if value else np.nan


def join_aggregate(values):
    """Combine a group of values into a single value."""
    value = ','.join([v for v in values if not is_number(v)])
    return value if value else np.nan


if __name__ == '__main__':
    ingest_taxonomy()
