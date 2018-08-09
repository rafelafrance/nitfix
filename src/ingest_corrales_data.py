"""Extract, transform, and load the Corrales data."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.google as google


INTERIM_DATA = Path('.') / 'data' / 'interim'


def ingest_corrales_data():
    """Process the Corrales data."""
    csv_path = INTERIM_DATA / 'corrales.csv'
    cxn = db.connect()

    google.sheet_to_csv('corrales_data', csv_path)
    corrales = pd.read_csv(csv_path)
    corrales.corrales_id = corrales.corrales_id.str.lower()

    corrales.to_sql('corrales_data', cxn, if_exists='replace', index=False)

    # already_in = corrales.sample_id.isin(images.sample_id)
    # corrales = corrales[~already_in]
    #
    # corrales = corrales.drop('corrales_id', axis=1)
    # return pd.concat([images, corrales], ignore_index=True, sort=True)


if __name__ == '__main__':
    ingest_corrales_data()
