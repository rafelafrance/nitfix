"""Create manifests."""

import os
from os.path import dirname
import pandas as pd
from PIL import Image
import lib.db as db
import lib.util as util


CXN = db.connect()


def remaining_1_of_3():
    """Make a manifest and zip images for all remaining images."""
    sql = """
        SELECT *
          FROM images
         WHERE sample_id NOT IN (SELECT sample_id FROM nfn_data)
           AND image_file NOT LIKE 'missing_photos%'
      ORDER BY image_file
         LIMIT 2500;
        """
    images = pd.read_sql(sql, CXN)
    images['manifest_file'] = images.image_file.str.replace('/', '_')
    images.to_csv(util.TEMP_DATA / 'nitfix_remaining_1_of_3.csv', index=False)
    zip_images(images, 'nitfix_remaining_1_of_3')


def remaining_2_of_3():
    """Make a manifest and zip images for all remaining images."""
    sql = """
        SELECT *
          FROM images
         WHERE sample_id NOT IN (SELECT sample_id FROM nfn_data)
           AND image_file NOT LIKE 'missing_photos%'
      ORDER BY image_file
         LIMIT 2500 OFFSET 2500;
    """
    images = pd.read_sql(sql, CXN)
    images['manifest_file'] = images.image_file.str.replace('/', '_')
    images.to_csv(util.TEMP_DATA / 'nitfix_remaining_2_of_3.csv', index=False)
    zip_images(images, 'nitfix_remaining_2_of_3')


def remaining_3_of_3():
    """Make a manifest and zip images for all remaining images."""
    sql = """
        SELECT *
          FROM images
         WHERE sample_id NOT IN (SELECT sample_id FROM nfn_data)
           AND image_file NOT LIKE 'missing_photos%'
      ORDER BY image_file
           LIMIT 2500 OFFSET 5000;
      """
    images = pd.read_sql(sql, CXN)
    images['manifest_file'] = images.image_file.str.replace('/', '_')
    images.to_csv(util.TEMP_DATA / 'nitfix_remaining_3_of_3.csv', index=False)
    zip_images(images, 'nitfix_remaining_3_of_3')


def mobot_all():
    """Make manifest and zip images."""
    sql = """
        SELECT image_file, sample_id
          FROM images
         WHERE image_file LIKE 'MO-DOE-nitfix_specimen_photos/%'
            OR image_file LIKE 'MO-DOE-nitfix_visit2/%'
            OR image_file LIKE 'MO-DOE-nitfix_visit3/%'
            OR image_file LIKE 'Tingshuang_MO_nitfix_photos/%';
        """
    images = pd.read_sql(sql, CXN)
    images['manifest_file'] = images.image_file.str.replace('/', '_')
    images.to_csv(util.TEMP_DATA / 'mobot_all_manifest.csv', index=False)

    zip_images(images, 'mobot_all')


def zip_images(images, image_dir):
    """Shrink and rotate images and then put them into a zip file."""
    image_zip_dir = util.TEMP_DATA / image_dir
    os.makedirs(image_zip_dir, exist_ok=True)

    for _, image_file in images.image_file.iteritems():
        print(image_file)
        src = util.PHOTOS / image_file
        dst = image_zip_dir / image_file.replace('/', '_')
        original = Image.open(src)
        transformed = original.resize((
            int(original.size[0] * 0.75),
            int(original.size[1] * 0.75)))
        dir_name = dirname(image_file)
        if original.size[0] > original.size[1]:
            if (dir_name.startswith('Tingshuang')
                and dir_name != 'Tingshuang_US_nitfix_photos') \
               or dir_name in (
                    'MO-DOE-nitfix_visit3', 'NY_DOE-nitfix_visit3',
                    'NY_DOE-nitfix_visit4', 'NY_DOE-nitfix_visit5'):
                transformed = transformed.transpose(Image.ROTATE_90)
            else:
                transformed = transformed.transpose(Image.ROTATE_270)
        transformed.save(dst)


def nybg234():
    """Make a manifest."""
    sql = """
        SELECT image_file, images.sample_id, sci_name
          FROM images
          JOIN taxonomy_ids USING (sample_id)
         WHERE image_file LIKE 'NY_visit_2/%'
            OR image_file LIKE 'NY_DOE-nitfix_visit3/%'
            OR image_file LIKE 'NY_DOE-nitfix_visit4/%';
        """
    images = pd.read_sql(sql, CXN)

    sql = """
        SELECT image_file FROM image_errors
         WHERE image_file LIKE 'NY_visit_2/%'
            OR image_file LIKE 'NY_DOE-nitfix_visit3/%'
            OR image_file LIKE 'NY_DOE-nitfix_visit4/%';
        """
    errors = pd.read_sql(sql, CXN)

    images.to_csv(util.TEMP_DATA / 'nybg_manifest.csv', index=False)
    errors.to_csv(util.TEMP_DATA / 'nybg_manifest_missing.csv', index=False)


def cal_academy():
    """Make a manifest."""
    sql = """
        SELECT image_file, images.sample_id, sci_name
          FROM images
          JOIN taxonomy_ids USING (sample_id)
         WHERE image_file LIKE '%/CAS-DOE-nitfix_specimen_photos/%'
      ORDER BY image_file;
    """
    images = pd.read_sql(sql, CXN)

    images.image_file = images.image_file.str.extract(r'.*/(.*)', expand=False)

    print(len(images))
    images.head()

    sql = """
        SELECT image_file FROM image_errors
         WHERE image_file LIKE 'CAS-DOE-nitfix_specimen_photos/%';
        """
    errors = pd.read_sql(sql, CXN)
    errors.image_file = errors.image_file.str.extract(
        r'.*/(.*)', expand=False)

    images.to_csv(util.TEMP_DATA / 'cas_manifest.csv', index=False)
    errors.to_csv(util.TEMP_DATA / 'cas_manifest_missing.csv', index=False)


# def mobot():
#     """Make a manifest."""
#     taxonomy = pd.read_sql('SELECT * FROM taxa;', CXN)
#
#     sql = """
#         SELECT *
#           FROM images
#          WHERE file_name LIKE 'MO-DOE-nitfix_specimen_photos/%';
#         """
#
#     images = pd.read_sql(sql, CXN)
#
#     taxa = {}
#     for key, taxon in taxonomy.iterrows():
#         guids = util.split_uuids(taxon.sample_ids)
#         for guid in guids:
#             taxa[guid] = taxon.sci_name
#
#     for key, image in images.iterrows():
#         images.loc[key, 'resolved_name'] = taxa.get(image.sample_id)
#
#     images.head()
#
#     images.file_name = images.file_name.apply(basename)
#     print(len(images))
#     images.head()
#
#     csv_path = util.TEMP_DATA / 'mobot_manifest.csv'
#     images.to_csv(csv_path, index=False)
#
#     missing = images.resolved_name.isna()
#     missing_images = images[missing]
#     len(missing_images)


if __name__ == '__main__':
    remaining_1_of_3()
    remaining_2_of_3()
    remaining_3_of_3()
