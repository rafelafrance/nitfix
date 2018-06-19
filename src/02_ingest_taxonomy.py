"""Extract, transform, and load data related to the taxonomy."""

import os
import re
from pathlib import Path
from functools import partial
import numpy as np
import pandas as pd
from pandas.api.types import is_number
import dropbox
from dotenv import load_dotenv, find_dotenv
import lib.db as db
import lib.google as google


load_dotenv(find_dotenv())
INTERIM_DATA = Path('data') / 'interim'
EXTERNAL_DATA = Path('..') / 'data' / 'external'
ORDERS = ['Cucurbitales', 'Fagales', 'Fabales', 'Rosales']


def ingest_taxonomy():
    """Ingest data related to the taxonomy."""
    cxn = db.connect()

    my_dropbox = os.getenv('DROPBOX')
    dbx = dropbox.Dropbox(my_dropbox)

    taxonomy, split_ids = get_master_taxonomy()
    taxon_ids = link_images_to_taxonomy(cxn, taxonomy, split_ids)
    taxon_ids = merge_pilot_data(cxn, taxon_ids)
    taxon_ids = merge_corrales_data(cxn, taxon_ids)
    taxonomy = rollup_id_data(taxonomy, taxon_ids)
    taxonomy = merge_genbank_loci_data(taxonomy)
    taxonomy = merge_werner_data(taxonomy)
    taxon_ids, expeditions = get_expedition_data(dbx, taxon_ids)

    taxonomy.to_sql('taxonomy', cxn, if_exists='replace', index=False)
    taxon_ids.to_sql('taxon_ids', cxn, if_exists='replace', index=False)
    expeditions.to_sql('expeditions', cxn, if_exists='replace', index=False)


def join_columns(columns, row):
    """Combine data from multiple columns into a single value."""
    value = ','.join([row[c] for c in columns if not is_number(row[c])])
    return value if value else np.nan


def join_aggregate(values):
    """Combine a group of values into a single value."""
    value = ','.join([v for v in values if not is_number(v)])
    return value if value else np.nan


def get_master_taxonomy():
    """Get the master taxonomy google sheet."""
    csv_path = INTERIM_DATA / 'taxonomy.csv'

    google.sheet_to_csv('NitFixMasterTaxonomy', csv_path)

    taxonomy = pd.read_csv(
        csv_path,
        header=0,
        names=[
            'column_a',
            'family',
            'scientific_name',
            'authority',
            'synonyms',
            'ids',
            'provider_acronym',
            'provider_id',
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

    taxon_ids = (taxonomy.melt(id_vars=['scientific_name'],
                               value_vars=split_ids.columns)
                 .rename(columns={'value': 'id'}))

    has_id = taxon_ids.id.str.len() > 4
    taxon_ids = taxon_ids[has_id]

    taxon_ids.id = taxon_ids.id.str.split().str.join(' ')

    taxon_ids = taxon_ids.merge(
        right=images, how='left', left_on='id', right_on='sample_id')

    return taxon_ids


def merge_pilot_data(cxn, taxon_ids):
    """Link pilot data to taxonomy IDs."""
    pilot = pd.read_sql('SELECT * FROM raw_pilot', cxn)

    taxon_ids = taxon_ids.merge(
        right=pilot, how='left', left_on='id', right_on='pilot_id')

    columns = ['image_file_x', 'image_file_y']
    taxon_ids['image_file'] = taxon_ids.apply(
        partial(join_columns, columns), axis=1)
    taxon_ids = taxon_ids.drop(columns, axis=1)

    columns = ['sample_id_x', 'sample_id_y']
    taxon_ids['sample_id'] = taxon_ids.apply(
        partial(join_columns, columns), axis=1)
    taxon_ids = taxon_ids.drop(columns, axis=1)

    return taxon_ids


def merge_corrales_data(cxn, taxon_ids):
    """Link Corrales data to taxonomy IDs."""
    corrales = pd.read_sql('SELECT * FROM raw_corrales', cxn)

    taxon_ids = taxon_ids.merge(
        right=corrales, how='left', left_on='id', right_on='corrales_id')

    columns = ['image_file_x', 'image_file_y']
    taxon_ids['image_files'] = taxon_ids.apply(
        partial(join_columns, columns), axis=1)
    taxon_ids = taxon_ids.drop(columns, axis=1)

    columns = ['sample_id_x', 'sample_id_y']
    taxon_ids['sample_ids'] = taxon_ids.apply(
        partial(join_columns, columns), axis=1)
    taxon_ids = taxon_ids.drop(columns, axis=1)

    not_an_id = taxon_ids.id.str.contains('corrales: corrales no voucher')
    taxon_ids = taxon_ids[~not_an_id]

    return taxon_ids


def rollup_id_data(taxonomy, taxon_ids):
    """Merge per ID data into the taxonomy data."""
    groups = taxon_ids.groupby('scientific_name').aggregate({
        'id': join_aggregate,
        'pilot_id': join_aggregate,
        'sample_ids': join_aggregate,
        'image_files': join_aggregate})

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
    drops = """NFC Legume Likelihood_non-precursor
        Likelihood_precursor Likelihood_fixer Likelihood_stable_fixer
        Most_likely_state Corrected_lik_precursor
        Corrected_lik_stable_fixer""".split()
    werner = werner.drop(drops, axis=1).rename(columns={
        'Species': 'scientific_name',
        'Family': 'family',
        'Order': 'order'})
    in_orders = werner.order.isin(ORDERS)
    return werner[in_orders]


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


def get_expedition_data(dbx, taxon_ids):
    """Get NitFix 1 expedition data."""
    csv_path = os.fspath(INTERIM_DATA / 'expedition_01.csv')
    dbx_path = 'id:zSBrtnqOfSAAAAAAAAAAKw/5657_Nit_Fix_I.reconcile.4.2.csv'

    dbx.files_download_to_file(csv_path, dbx_path)

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

    taxon_ids = taxon_ids.merge(
        right=expeditions, how='left', left_on='id', right_on='sample_id')

    return taxon_ids, expeditions


if __name__ == '__main__':
    ingest_taxonomy()
