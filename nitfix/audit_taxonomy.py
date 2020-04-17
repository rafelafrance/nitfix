"""Audit problem taxonomy records in the database.

Some simple sanity checks on the taxonomy table. Remove taxonomy records that
will cause problems later in the analysis. As always, record any destructive
action taken in an errors table for amy data that may be recovered later.
However, pure garbage is excised without an error record.
"""

import pandas as pd

import lib.db as db


def clean_taxonomy():
    """Audit problem taxonomy records in the database."""
    cxn = db.connect()
    taxonomy_ids = pd.read_sql('SELECT * FROM taxonomy_ids;', cxn)
    errors = get_duplicate_sample_ids(taxonomy_ids)
    create_taxonomy_errors_table(cxn, errors)

    drop_bad_genera(cxn)
    drop_errors(cxn)


def drop_errors(cxn):
    """Remove duplicate taxonomy ids.

    The problem taxonomy IDs are already in the taxonomy error table, we can
    drop hem from the taxonomy table itself.
    """
    cxn.execute("""
        DELETE FROM taxonomy_ids
         WHERE sample_id IN (SELECT sample_id FROM taxonomy_errors) ;""")
    cxn.commit()


def drop_bad_genera(cxn):
    """Remove taxonomy records with a bad genus.

    A bad genus name is completely useless for any further analysis.
    """
    cxn.execute("""DELETE FROM taxonomy WHERE genus LIKE '%.%';""")
    cxn.commit()


def get_duplicate_sample_ids(taxonomy_ids):
    """Get duplicate sample IDs from the taxonomy table.

    It happens that some sample IDs are associated with more than taxon. Which
    means that the same sample is two different species. This is a data entry
    error and should be removed. Conversely, having more than one sample for
    a taxon is fine; it's just oversampling and will be handled later.
    """
    taxonomy_ids['times'] = 0
    errors = taxonomy_ids.groupby('sample_id').agg(
        {'times': 'count', 'sci_name': ', '.join})
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
