"""Extract, transform, and load pilot data."""

import pandas as pd
import lib.db as db
import lib.google as google
import lib.util as util


def ingest_pilot_data():
    """Process the pilot data."""
    csv_path = util.TEMP_DATA / 'pilot.csv'
    cxn = db.connect()

    google.sheet_to_csv('UFBI_identifiers_photos', csv_path)
    pilot = pd.read_csv(csv_path)

    # Create a fake path for the file name
    pilot['image_file'] = pilot['File'].apply(
        lambda x: f'UFBI_sample_photos/{x}')

    pilot = (pilot.drop(['File'], axis=1)
             .rename(columns={'Identifier': 'pilot_id'}))
    pilot.pilot_id = pilot.pilot_id.str.lower().str.split().str.join(' ')

    create_pilot_data_table(cxn, pilot)

    merge_into_images(cxn)


def create_pilot_data_table(cxn, pilot):
    """Create pilot data table."""
    pilot.to_sql('pilot_data', cxn, if_exists='replace', index=False)

    cxn.executescript("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            pilot_data_pilot_id ON pilot_data (pilot_id);
        CREATE UNIQUE INDEX IF NOT EXISTS
            pilot_data_sample_id ON pilot_data (sample_id);
        CREATE UNIQUE INDEX IF NOT EXISTS
            pilot_data_image_file ON pilot_data (image_file);
        """)


def merge_into_images(cxn):
    """Merge the data into the images table."""
    cxn.execute("""
        INSERT OR REPLACE INTO images (sample_id, image_file)
            SELECT sample_id, image_file FROM pilot_data;
        """)
    cxn.commit()


if __name__ == '__main__':
    ingest_pilot_data()
