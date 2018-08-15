"""Extract, transform, and load data related to Notes from Nature data."""

import os
import re
import pandas as pd
import lib.db as db
import lib.util as util


EXPEDITIONS = [
    '5657_Nit_Fix_I.reconcile.0.4.4.csv',
    '5857_Nit_Fix_II.reconcile.0.4.4.csv',
    '6415_Nit_Fix_III.reconcile.0.4.4.csv',
    '6779_Nit_Fix_IV.reconcile.0.4.4.csv']


def ingest_nfn_data():
    """Ingest data related to the taxonomy."""
    cxn = db.connect()

    exps = [get_expedition(e) for e in EXPEDITIONS]
    nfn = pd.concat(exps, ignore_index=True, sort=False).fillna('')
    nfn = fixup_data(nfn)

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
    return ', '.join(set(group))


def create_nfn_table(cxn, nfn):
    """Create Notes from Nature data table."""
    nfn.to_sql('nfn_data', cxn, if_exists='replace')

    sql = """CREATE UNIQUE INDEX IF NOT EXISTS
             nfn_data_sample_id ON nfn_data (sample_id)"""
    cxn.execute(sql)


if __name__ == '__main__':
    ingest_nfn_data()
