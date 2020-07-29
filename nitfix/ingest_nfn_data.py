"""Extract, transform, and load data related to Notes from Nature data."""

import os
import re
import string

import pandas as pd

import lib.db as db
import lib.util as util

EXPEDITIONS = [
    '5657_Nit_Fix_I.reconcile.4.3.csv',
    '5857_Nit_Fix_II.reconcile.0.4.4.csv',
    '6415_Nit_Fix_III.reconcile.4.3.csv',
    '6779_Nit_Fix_IV.reconcile.0.4.4.csv',
    '6801_nitrogen_fixing_plants_v_east_coast.reconcile.0.4.4.csv',
    '12077_nitfix-the-return.reconciled.0.4.7.csv',

    ('10651_understanding-a-critical-symbiosis-nitrogen-fixing-in-'
     'plants-missouri-botanical-gardens.reconciled.0.4.5.csv'),
]


def ingest_nfn_data():
    """Ingest data related to the taxonomy."""
    cxn = db.connect()

    exps = [get_expedition(e) for e in EXPEDITIONS]
    nfn = pd.concat(exps, ignore_index=True).fillna('')
    nfn = fixup_data(nfn)
    nfn = update_collector_data(nfn)

    create_nfn_table(cxn, nfn)


def get_expedition(file_name):
    """Get NitFix expedition data."""
    csv_path = os.fspath(util.EXPEDITION_DATA / file_name)
    nfn = pd.read_csv(csv_path, dtype=str).fillna('')
    nfn['workflow_id'] = file_name[:4]

    columns = {}
    for old in nfn.columns:
        new = old.lower()
        new = new.replace('⁰', 'deg')
        new = new.replace("''", 'sec')
        new = new.replace("'", 'min')
        new = re.sub(r'\W+', '_', new)
        new = re.sub(r'^_|_$', '', new)
        columns[old] = new
    if 'subject_qr_code' in columns:
        columns['subject_qr_code'] = 'sample_id'
    if 'subject_sample_id' in columns:
        columns['subject_sample_id'] = 'sample_id'
    nfn.rename(columns=columns, copy=False, inplace=True)

    return nfn


def fixup_data(nfn):
    """Merge duplicate sample IDs into one record."""
    aggs = {c: agg_concat for c in nfn.columns}
    dup_ids = nfn.sample_id.duplicated(keep=False)
    dups = nfn.loc[dup_ids].groupby('sample_id').agg(aggs)

    nfn = pd.concat([nfn[~dup_ids], dups])
    nfn = nfn.set_index('sample_id')
    return nfn


def agg_concat(group):
    """Concatenate the group into a string of unique values."""
    group = [g for g in group if g]
    return '|'.join(set(group))


def update_collector_data(nfn):
    """Normalize the collector data as much as possible."""
    nfn['collection_no'] = nfn.apply(get_collection_no, axis=1)
    nfn['collected_by'] = nfn.apply(
        lambda x: x.collected_by_first_collector_last_name_only
        or x.primary_collector_last_first_middle, axis=1)
    nfn['last_name'] = nfn.collected_by.apply(get_last_name)
    return nfn


def get_last_name(collected_by):
    """Extract the last name from the collected by field."""
    if not collected_by:
        return ''

    collected_by = collected_by.split(',')
    if not collected_by or not collected_by[0]:
        return ''

    last_name = collected_by[0]

    while (last_name[-1] in string.punctuation
           or (len(last_name) > 4 and last_name[-2] == ' ')):
        if last_name[-1] in string.punctuation:
            last_name = last_name[:-1]
        if len(last_name) > 4 and last_name[-2] == ' ':
            last_name = last_name[:-2]

    return last_name


def get_collection_no(row):
    """Get the collection number from an expedition row."""
    if row.get('collector_number'):
        return row.collector_number
    num = row.get('collector_number_numeric_only', '')
    verb = row.get('collector_number_verbatim', '')
    if verb and len(num) < 2:
        return row.collector_number_verbatim
    return row.collector_number_numeric_only


def create_nfn_table(cxn, nfn):
    """Create Notes from Nature data table."""
    nfn.to_sql('nfn_data', cxn, if_exists='replace')

    cxn.execute("""
        CREATE UNIQUE INDEX nfn_data_sample_id ON nfn_data (sample_id);
        """)


if __name__ == '__main__':
    ingest_nfn_data()
