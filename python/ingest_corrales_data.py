"""Extract, transform, and load the Corrales data."""

import pandas as pd
import lib.db as db
import lib.util as util
import lib.google as google


def ingest_corrales_data():
    """Process the Corrales data."""
    csv_path = util.INTERIM_DATA / 'corrales.csv'
    cxn = db.connect()

    google.sheet_to_csv('corrales_data', csv_path)
    corrales = pd.read_csv(csv_path)
    corrales.corrales_id = corrales.corrales_id.str.lower()
    corrales.image_file = corrales.image_file.apply(util.normalize_file_name)

    create_corrales_data_table(cxn, corrales)

    merge_into_images(cxn)


def create_corrales_data_table(cxn, corrales):
    """Create corrales data table."""
    corrales.to_sql('corrales_data', cxn, if_exists='replace', index=False)

    cxn.executescript("""
        CREATE INDEX IF NOT EXISTS
            corrales_data_corrales_id ON corrales_data (corrales_id);
        CREATE UNIQUE INDEX IF NOT EXISTS
            corrales_data_sample_id ON corrales_data (sample_id);
        CREATE UNIQUE INDEX IF NOT EXISTS
            corrales_data_image_file ON corrales_data (image_file);
    """)


def merge_into_images(cxn):
    """Merge the data into the images table."""
    cxn.execute("""
        INSERT OR REPLACE INTO images (sample_id, image_file)
            SELECT sample_id, image_file FROM corrales_data;""")
    cxn.commit()


if __name__ == '__main__':
    ingest_corrales_data()
