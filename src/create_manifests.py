"""Create manifests."""

from os.path import basename
from pathlib import Path
import pandas as pd
import lib.db as db
import lib.util as util


CXN = db.connect()
INTERIM_DATA = Path('data') / 'interim'


def nybg234():
    """Make a manifest."""
    sql = """
        SELECT image_file, images.sample_id, scientific_name
          FROM images
          JOIN taxon_ids ON (taxon_ids.id = images.sample_id)
         WHERE image_file LIKE 'NY_visit_2/%'
            OR image_file LIKE 'NY_DOE-nitfix_visit3/%'
            OR image_file LIKE 'NY_DOE-nitfix_visit4/%'
        """
    images = pd.read_sql(sql, CXN)

    sql = """
        SELECT image_file FROM image_errors
         WHERE image_file LIKE 'NY_visit_2/%'
            OR image_file LIKE 'NY_DOE-nitfix_visit3/%'
            OR image_file LIKE 'NY_DOE-nitfix_visit4/%'
        """
    errors = pd.read_sql(sql, CXN)

    images.to_csv(INTERIM_DATA / 'nybg_manifest.csv', index=False)
    errors.to_csv(INTERIM_DATA / 'nybg_manifest_missing.csv', index=False)


def CalAcademy():
    """Make a manifest."""
    sql = """
        SELECT image_file, images.sample_id, scientific_name
          FROM images
          JOIN taxon_ids ON (taxon_ids.id = images.sample_id)
         WHERE image_file LIKE '%/CAS-DOE-nitfix_specimen_photos/%'
      ORDER BY image_file
    """
    images = pd.read_sql(sql, CXN)

    images.image_file = images.image_file.str.extract(r'.*/(.*)', expand=False)

    print(len(images))
    images.head()

    sql = """
        SELECT image_file FROM image_errors
         WHERE image_file LIKE 'CAS-DOE-nitfix_specimen_photos/%'
        """
    errors = pd.read_sql(sql, CXN)
    errors.image_file = errors.image_file.str.extract(r'.*/(.*)', expand=False)

    print(len(errors))
    errors

    images.to_csv(INTERIM_DATA / 'cas_manifest.csv', index=False)
    errors.to_csv(INTERIM_DATA / 'cas_manifest_missing.csv', index=False)


def mobot():
    """Make a manifest."""
    taxonomy = pd.read_sql('SELECT * FROM taxons', CXN)

    sql = """SELECT *
               FROM images
              WHERE file_name LIKE 'MO-DOE-nitfix_specimen_photos/%'"""

    images = pd.read_sql(sql, CXN)

    taxons = {}
    for key, taxon in taxonomy.iterrows():
        guids = util.split_uuids(taxon.sample_ids)
        for guid in guids:
            taxons[guid] = taxon.scientific_name

    for key, image in images.iterrows():
        images.loc[key, 'resolved_name'] = taxons.get(image.sample_id)

    images.head()

    images.file_name = images.file_name.apply(basename)
    print(len(images))
    images.head()

    csv_path = INTERIM_DATA / 'mobot_manifest.csv'
    images.to_csv(csv_path, index=False)

    missing = images.resolved_name.isna()
    missing_images = images[missing]
    len(missing_images)


if __name__ == '__main__':
    nybg234()
