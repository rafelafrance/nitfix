"""Audit problem taxonomy records in the database."""

import pandas as pd
import lib.db as db


def clean_taxonomy():
    """Audit problem taxonomy records in the database."""
    cxn = db.connect()
    taxonomy_ids = pd.read_sql('SELECT * FROM taxonomy_ids;', cxn)
    errors = get_duplicate_sample_ids(taxonomy_ids)
    create_taxonomy_errors_table(cxn, errors)


def get_duplicate_sample_ids(taxonomy_ids):
    """Get duplicate sample IDs from the taxonomy table."""
    taxonomy_ids['times'] = 0
    errors = taxonomy_ids.groupby('sample_id').agg(
        {'times': 'count', 'sci_name': lambda x: ', '.join(x)})
    errors = errors.loc[errors.times > 1, :].drop(['times'], axis='columns')

    sci_names = errors.sci_name.str.split(r'\s*[;,]\s*', expand=True)
    id_cols = {i: f'sci_name_{i + 1}' for i in sci_names.columns}
    sci_names = sci_names.rename(columns=id_cols)

    errors = pd.concat([errors, sci_names], axis='columns').drop(
        ['sci_name'], axis=1)

    return errors


def create_taxonomy_errors_table(cxn, errors):
    """Create taxonomy table."""
    errors.to_sql('taxonomy_errors', cxn, if_exists='replace')

    cxn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            taxonomy_errors_sample_id ON taxonomy_errors (sample_id);
        """)


if __name__ == '__main__':
    clean_taxonomy()
