"""Extract, transform, and load data related to Notes from Nature data."""

import os
import re
import numpy as np
import pandas as pd
from pandas.api.types import is_number
import lib.db as db
import lib.util as util


def ingest_nfn_data():
    """Ingest data related to the taxonomy."""
    # cxn = db.connect()

    # taxon_ids, expeditions = get_expedition_data(taxon_ids)
    #
    # taxonomy.to_sql('taxonomy', cxn, if_exists='replace', index=False)
    # taxon_ids.to_sql('taxon_ids', cxn, if_exists='replace', index=False)
    # expeditions.to_sql('expeditions', cxn, if_exists='replace', index=False)


def get_expedition_data(taxon_ids):
    """Get NitFix 1 expedition data."""
    csv_path = os.fspath(
        util.EXPEDITION_DATA / '5657_Nit_Fix_I.reconcile.4.2.csv')

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
    ingest_nfn_data()
