"""Ingest data crawled from the Tropicos search portal."""

import pandas as pd
import lib.db as db
import lib.util as util


def ingest_tropicos_data():
    """Ingest data crawled from the Tropicos search portal."""
    # csv_path = util.RAW_DATA / 'tropicos' / 'tropicos.csv'
    # cxn = db.connect()
    #
    # tropicos = pd.read_csv(csv_path)
    #
    # create_tropicos_table(cxn, tropicos)


def create_tropicos_table(cxn, tropicos):
    """Create Tropicos data table."""


if __name__ == '__main__':
    ingest_tropicos_data()
