"""Create manifests.

The photos are handed off to citizen science projects (expeditions) in
Zooniverse, Notes from Nature, for gathering information about what's on the
museum label or the condition of the sample itself. This script creates
zip files of the photos and manifests of what's in them for these projects.

These expeditions tend to be ad hoc.
"""

import os
import random
import re
import sqlite3
import zipfile
from os.path import basename, dirname

import pandas as pd
from PIL import Image

import lib.db as db
import lib.util as util

CXN = db.connect()
CXN.row_factory = sqlite3.Row

# A simple regex for checking if a string is a valid UUID
IS_UUID = re.compile(
    r""" \b [0-9a-f]{8} - [0-9a-f]{4} - [1-5][0-9a-f]{3}
        - [89ab][0-9a-f]{3} - [0-9a-f]{12} \b """,
    flags=re.IGNORECASE | re.VERBOSE)

# Some samples are missing a photo, flag those with this value.
MISSING = '<missing/>'


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


def nfn_submitted():
    """Create a table of samples submitted to NfN."""
    sql = """
        SELECT *
          FROM images
         WHERE sample_id IN (SELECT sample_id FROM nfn_data)
           AND image_file NOT LIKE 'missing_photos%'
      ORDER BY image_file;
      """

    df1 = pd.read_csv(util.INTERIM_DATA / 'nitfix_remaining_1_of_3.csv')
    df2 = pd.read_csv(util.INTERIM_DATA / 'nitfix_remaining_2_of_3.csv')
    df3 = pd.read_csv(util.INTERIM_DATA / 'nitfix_remaining_3_of_3.csv')
    df4 = pd.read_sql(sql, CXN)
    df4['manifest_file'] = df4.image_file.str.replace('/', '_')
    all_ = pd.concat([df1, df2, df3, df4])
    all_.to_sql('nfn_submitted', CXN, if_exists='replace', index=False)


def missing_location():
    """Create expeditions of images returned from NfN without a location."""
    size = 2500
    sql = """
        SELECT sample_id, i.image_file, subject_id
          FROM images AS i
          JOIN nfn_data AS n USING (sample_id)
         WHERE sample_id IN (SELECT sample_id FROM nfn_data
                              WHERE location = '')
      ORDER BY image_file;
        """
    df = pd.read_sql(sql, CXN)
    df['manifest_file'] = df.image_file.str.replace('/', '_')
    steps = list(range(0, df.shape[0], size))
    splits = [df.iloc[i:i + size, :] for i in steps]
    for i, split in enumerate(splits, 1):
        name = f'nitfix_missing_location_{i}_of_{len(splits)}'
        split.to_csv(util.TEMP_DATA / (name + '.csv'), index=False)
        zip_images(split, name)


def missing_country():
    """Create expeditions of images returned from NfN without a country."""
    size = 2500
    sql = """
        SELECT sample_id, i.image_file, subject_id
          FROM images AS i
          JOIN nfn_data AS n USING (sample_id)
         WHERE sample_id IN (SELECT sample_id FROM nfn_data
                              WHERE country = '')
      ORDER BY image_file;
        """
    df = pd.read_sql(sql, CXN)
    df['manifest_file'] = df.image_file.str.replace('/', '_')
    steps = list(range(0, df.shape[0], size))
    splits = [df.iloc[i:i + size, :] for i in steps]
    for i, split in enumerate(splits, 1):
        name = f'nitfix_missing_country_{i}_of_{len(splits)}'
        split.to_csv(util.TEMP_DATA / (name + '.csv'), index=False)
        zip_images(split, name)


def random_subset():
    """Create a table of samples submitted to NfN.

    This isn't an actual expedition, but it is using the same general logic.
    """
    sample_size = 2400
    sql = """
        SELECT *
          FROM images
         WHERE image_file NOT LIKE 'missing_photos%'
      ORDER BY image_file;
      """
    rows = list(CXN.execute(sql))
    rows = random.sample(rows, sample_size)
    rows = [{'image_file': r[0], 'sample_id': r[1],
             'manifest_file': r[0].replace('/', '_')} for r in rows]

    name = 'nitfix_sample_2020-04-07a'
    df = pd.DataFrame(rows)
    df.to_csv(util.TEMP_DATA / (name + '.csv'), index=False)
    zip_images(df, name)


def get_a_genus(genus):
    """Create a zip file of images from one genus."""
    mask = genus.title() + '%'
    sql = """
        select *
          from taxonomy_ids
          join images using (sample_id)
          left join nfn_data using (sample_id)
         where sci_name like ?
        """
    cursor = CXN.execute(sql, (mask, ))
    row = cursor.fetchone()
    columns = row.keys()

    rows = list(CXN.execute(sql, (mask, )))
    name = f'genus_{genus}_2020-06-16b'

    df = pd.DataFrame(rows, columns=columns)
    df['manifest_file'] = df['image_file'].str.replace('/', '_')

    df.to_csv(util.TEMP_DATA / (name + '.csv'), index=False)
    zip_images(df, name)


def image_zip():
    """Get images from a list and zip them.

    There was s request to get the images for a specific set of samples. This
    isn't an actual expedition.
    """
    cxn = db.connect()

    sample_ids = _get_sample_ids()
    images = []

    for sample_id in sample_ids:
        if IS_UUID.search(sample_id):
            sql = """SELECT image_file FROM images WHERE sample_id = ?"""
        else:
            sql = """SELECT image_file FROM pilot_data WHERE pilot_id = ?"""
        cur = cxn.cursor()
        cur.execute(sql, (sample_id,))
        row = cur.fetchone()
        images.append(row[0] if row else MISSING)

    csv_file = util.TEMP_DATA / 'Images_2020-03-05a.csv'
    csv_data = pd.DataFrame({'sample_id': sample_ids, 'image_file': images})
    csv_data.to_csv(csv_file, index=False)

    zip_file = util.TEMP_DATA / 'Images_list_2020-03-05a.zip'
    with zipfile.ZipFile(zip_file, mode='w') as zippy:
        zippy.write(csv_file,
                    arcname=basename(csv_file),
                    compress_type=zipfile.ZIP_DEFLATED)
        for image in images:
            if image == MISSING:
                continue
            path = util.PHOTOS / image
            zippy.write(
                path, arcname=image, compress_type=zipfile.ZIP_DEFLATED)


def _get_sample_ids():
    """Return a hardcoded list of sample IDs that will be zipped.

    This list was given to me by the researchers.
    """
    return [
        'ny: telles 7394',
        'ny: lewis 1723',
        'ny: carvalho 5909',
        '3aa815bd-d4cf-458b-822c-a97e0f88d50d',
        '3ac5f8c7-fef3-41cd-88ab-31f1caf5484a',
        '3acd1733-57aa-424c-a311-864b978635aa',
        'b9b1583f-0a1d-4dd2-bd74-bae4d636181e',
        '0e9ccc62-f031-4d92-8a0b-527cec45bb0d',
        'ny: beaman 11372',
        'a3267333-837d-4875-adad-9da7270d75e6',
        'a1b87db5-e73d-4b31-a212-8e3e40ac1ec7',
        'tex: church 729',
        '1350a3ff-6512-42f2-8128-d5dcb2097172',
        '253a5672-5f75-4fa1-94d0-26507768dcce',
        'b91cd5af-b17f-4138-9798-78d308f2ee31',
        'tex: cuevas 78',
        'tex: ballesteros 344',
        '8252b59d-0a19-49ab-a62e-420d20a37e36',
        'tex: henrickson 8292',
        'tex: webster 33479',
        '3f068026-62bb-45d5-99d1-a703b088f33a',
        '3f05a8e4-12a2-4c70-9628-13d7796bb8dc',
        'tex: webster 4926',
        'tex: correll 42435',
        '3ef8577e-67f2-4813-9f3b-332cdf53f95c',
        '088b524c-b577-4d8f-8a4c-10404d60bd12',
        '0ff29671-cac5-4aec-a001-82c1ea01d707',
        '0fe6fec2-5bc1-484d-9ebb-eea94f17ea93',
        '9fad1c62-2240-4a74-9b11-5796693b4049',
        '3ef7b767-60e1-4401-b82b-c3a78d5855e7',
        '0fe6987a-c259-4edd-918c-4d2e063c6372',
        '0fded544-6fc7-44b2-99e3-337b76a2b16e',
        '0fd1f6c3-fe08-413f-a365-db6aab8bb418',
        'tex: howard 246',
        '108a5119-3f74-447e-9cd6-95e878f562f8',
        '3c9812d4-762a-4ae1-b49d-2552aaf4dbe6',
        '3c8438c9-f6ca-4f1c-8da2-fedc94876df7',
        '8b3dece5-0b80-40cf-a7aa-6a2c96be3ad6',
        '3c823c0d-2a6c-43e2-bd1e-7626efa8c35b',
        '0d5b4d06-5b74-4eda-b795-ac4aa2c0191e',
        '0d644710-2d49-41ba-a2ff-50887c28f240',
        'tex: phillipson 3166',
        'tex: contreras 8742',
        '0de59b5a-2df0-4cb3-9295-eebb664ffc02',
        '3beee92d-04a4-4d6d-9f5d-6416ac843c86',
        '3bdaecde-b52b-49b1-b580-c2bbcaa7487d',
        '0ca7f306-b0fd-49eb-b242-d4a0999a1576',
        '3f2ee9bf-ad3b-4deb-9424-1135314794d0',
        'tex: seigler ds-14450',
        'tex: pereira 2263',
        '3bda1fa1-dec3-47ce-aade-84ca875ceaaf',
        '3bdfc29f-2309-4e8d-9510-57d803ce5879',
        '3be63f7f-6f4f-4a70-8df8-4d45d0ea5b5f',
        '40a378ac-e289-4c0e-aa21-8b8248c52c59',
        '3aa0ebcb-2137-4e9f-9092-936dff10971a',
        '3aa0b8e9-1fe2-4875-8f70-1e1f30e3a000',
        '0b513105-f917-43b9-88bd-00939471d92e',
        'tex: turner 5255',
        '40462f12-a4b9-4a3b-93bb-c9dbe82a3b25',
        'tex: donner 10079',
        'tex: panero 7161',
        'tex: panero 6950',
        'a23ab012-7658-4e8b-97c3-705db38a0639',
        '09b6e692-1c6b-4d33-89a3-ef68c847d380',
        'tex: santos-guerra sgrjh 085',
        '09b936c2-9a8b-4736-bfb5-f3129ddfc013',
        '41daf6aa-1284-4fdd-95dc-ed4e625de6ff',
        'c31e5dd6-bb66-4f9c-a922-3ae282ca8d79',
        'tex: campos v4458',
        '12605d32-7db7-4ebe-b424-239f0f2ca70b',
        '39620ad5-ac9d-4abe-a184-8c2a0bb08bd0',
        'tex: vlastimil 3320',
        '3a5b4c2e-398d-49bc-91b9-35157da4325c',
        '3c374396-b060-4d95-86e4-58ad1f0db2b1',
        'tex: lewis 881795',
        'tex: tupayachi 810',
        '3a5cc0f5-e13f-4d2c-ab8e-3114e6d98e2c',
        '39523c91-f722-4855-842a-16dea817dbf9',
        '3a5f3373-6148-4937-bbad-cbab7f38e99d',
        '97963add-32c3-4936-9877-0d56af3efdd3',
        '3a5f33b7-59e7-448b-ba7f-2f4b4cf6f5a2',
        '3a66feae-9ed4-47e6-876a-95ffc08b7610',
        'tex: henrickson 22650',
        'tex: rosalinda 4902',
        'tex: lott 5453',
        '09d293f1-ba1e-4df6-8f6d-7d3a60e5ce65',
        '9998a6ef-cd69-4673-b3e6-c0494c271306',
        '09e3edb7-0e88-411a-b7e1-0b9bcf0def28',
        'tex: salinas 5964',
        'tex: landrum 11719',
        'tex: henrickson 23617',
        'tex: wendt 1222',
    ]


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
    # random_subset()
    get_a_genus('Inga')
