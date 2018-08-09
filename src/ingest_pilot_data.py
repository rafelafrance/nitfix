"""Extract, transform, and load pilot data."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.google as google


INTERIM_DATA = Path('.') / 'data' / 'interim'


def ingest_pilot_data(cxn, images):
    """Process the pilot data."""
    csv_path = INTERIM_DATA / 'pilot.csv'
    cxn = db.connect()

    google.sheet_to_csv('UFBI_identifiers_photos', csv_path)
    pilot = pd.read_csv(csv_path)

    # Create a fake path for the file name
    pilot['image_file'] = pilot['File'].apply(
        lambda x: f'UFBI_sample_photos/{x}')

    pilot = (pilot.drop(['File'], axis=1)
                  .rename(columns={'Identifier': 'pilot_id'}))
    pilot.pilot_id = pilot.pilot_id.str.lower().str.split().str.join(' ')

    pilot.to_sql('pilot_data', cxn, if_exists='replace', index=False)

    # already_in = pilot.sample_id.isin(images.sample_id)
    # pilot = pilot[~already_in]
    #
    # pilot = pilot.drop('pilot_id', axis=1)
    # return pd.concat([images, pilot], ignore_index=True, sort=True)


if __name__ == '__main__':
    ingest_pilot_data()
