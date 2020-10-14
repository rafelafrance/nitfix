"""Extract, transform, and load data related to the images."""

import os
from os.path import join
import multiprocessing
from collections import namedtuple
from glob import glob
from itertools import chain

import pandas as pd

import lib.db as db
import lib.image_util as i_util
import lib.util as util

Dimensions = namedtuple('Dimensions', 'width height')

BATCH_SIZE = 100


def ingest_images():
    """Process image files."""
    cxn = db.connect()

    # Get a list of all images and split it into roughly equal batches.
    # Then send each batch to a subprocess. The subprocess returns a list of
    # successfully processed images and a list of images that errored which are
    # both combined into their own dataframes.
    old_images, old_errors = get_old_images(cxn)
    image_files = get_images_to_process(old_images, old_errors)
    batches = [image_files[i:i + BATCH_SIZE]
               for i in range(0, len(image_files), BATCH_SIZE)]

    with multiprocessing.Pool(processes=util.PROCESSES) as pool:
        results = [pool.apply_async(ingest_batch, (b, )) for b in batches]
        results = [r.get() for r in results]

    new_images = list(chain(*[batch[0] for batch in results]))
    new_errors = list(chain(*[batch[1] for batch in results]))
    new_images = pd.DataFrame(new_images)
    new_errors = pd.DataFrame(new_errors)

    # Now finalize the image and error tables. Also handle any errors that can
    # only be caught when the batches are combined. In this case it's looking
    # for duplicate UUID errors.
    images = pd.concat([old_images, new_images], ignore_index=True)
    images, dupes = find_duplicate_uuids(images)
    errors = pd.concat(
        [old_errors, new_errors, dupes], ignore_index=True, sort=True)

    # Fix images where we have solutions to errors
    errors = resolve_errors(errors).drop_duplicates('image_file')
    images = manually_insert_images(images).drop_duplicates()

    create_image_table(cxn, images)
    create_image_errors_table(cxn, errors)


def create_image_table(cxn, images):
    """Create images table."""
    images.to_sql('images', cxn, if_exists='replace', index=False)

    cxn.executescript("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            images_sample_id ON images (sample_id);
        CREATE UNIQUE INDEX IF NOT EXISTS
            images_image_file ON images (image_file);
        """)


def create_image_errors_table(cxn, errors):
    """Create image errors table."""
    errors.to_sql('image_errors', cxn, if_exists='replace', index=False)
    cxn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            image_errors_image_file ON image_errors (image_file);""")


def get_old_images(cxn):
    """Get images already in the database."""
    # Handle the case where there is no image or error table in the DB.
    # NOTE: There will always be an error table if there is an image table.
    try:
        old_images = pd.read_sql('SELECT * FROM images;', cxn)
        old_errors = pd.read_sql('SELECT * FROM image_errors;', cxn)
        old_images.image_file = old_images.image_file.apply(
            util.normalize_file_name)
        old_errors.image_file = old_errors.image_file.apply(
            util.normalize_file_name)
    except pd.io.sql.DatabaseError:  # noqa
        old_images = pd.DataFrame(columns=['image_file', 'sample_id'])
        old_errors = pd.DataFrame(
            columns=['image_file', 'msg'])
    return old_images, old_errors


def find_duplicate_uuids(images):
    """
    Create error records for UUID duplicates.

    There is no real way to figure out which is the correct image to keep, so
    we keep the first one and mark all of the others as an error. We also
    remove the images with duplicate UUIDs from the image dataframe.
    """
    dupe_mask = images.sample_id.duplicated(keep='first')
    dupes = images.loc[dupe_mask, ['image_file', 'sample_id']]
    images = images[~dupe_mask]
    dupes['msg'] = ''   # Handle the case where there are no duplicates
    if dupes.shape[0]:
        dupes['msg'] = dupes.apply(
            lambda dupe:
            'DUPLICATES: Files {} and {} have the same QR code'.format(
                dupe.image_file,
                images.loc[images.sample_id == dupe.sample_id, 'image_file']),
            axis=1)
    dupes = dupes.drop(['sample_id'], axis=1)
    return images, dupes


def ingest_batch(image_batch):
    """Ingest image batch."""
    new_images = []
    new_errors = []

    for image_file in image_batch:
        sample_id = i_util.qr_value(image_file)
        if sample_id:
            new_images.append({
                'image_file': image_file,
                'sample_id': sample_id})
        else:
            new_errors.append({
                'image_file': image_file,
                'msg': f'MISSING: QR code missing in {image_file}',
                'ok': 0,
                'resolution': ''})

    return new_images, new_errors


def get_images_to_process(old_images, old_errors):
    """Get all image file names not in the database."""
    skip_images = set(old_images.image_file) | set(old_errors.image_file)

    image_files = []
    for image_dir in util.IMAGE_DIRS:
        pattern = os.fspath(util.PHOTOS / image_dir / '*.[Jj][Pp][Gg]')
        file_names = glob(pattern)
        image_files += map(util.normalize_file_name, file_names)
    image_files = [f for f in image_files if f not in skip_images]
    return sorted(image_files)


def resolve_errors(errors):
    """Update errors with their resolutions."""
    for resolution in get_resolutions():
        path = join(resolution[0], resolution[1])
        error = errors[errors.image_file == path]
        error.ok = resolution[2]
        error.resolution = resolution[3]
    return errors


def manually_insert_images(images):
    """Resolve some errors via a manual insert."""
    dfm = pd.DataFrame(get_manual_inserts())
    return pd.concat([images, dfm], ignore_index=True)


def get_manual_inserts():
    """Get images that can need to be added manually."""
    return [
        {'image_file': join('CAS-DOE-nitfix_specimen_photos', 'R0002092.JPG'),
         'sample_id': '8b6e0223-7fbe-4efc-a1e2-6c934da06685'},
        {'image_file': join('MO-DOE-nitfix_specimen_photos', 'R0003663.JPG'),
         'sample_id': '2eea159f-3c25-42ef-837d-27ad545a6779'}]


def get_resolutions():
    """Get the resolutions for some image errors."""
    return [
        ('DOE-nitfix_specimen_photos', 'R0000149', 1, 'OK: Genuine duplicate'),
        ('DOE-nitfix_specimen_photos', 'R0000151', 1, 'OK: Genuine duplicate'),
        ('DOE-nitfix_specimen_photos', 'R0000158', 1, 'OK: Genuine duplicate'),
        ('DOE-nitfix_specimen_photos', 'R0000165', 1, 'OK: Genuine duplicate'),
        ('DOE-nitfix_specimen_photos', 'R0000674', 1,
         'OK: Duplicate of R0000473'),
        ('DOE-nitfix_specimen_photos', 'R0000835', 1,
         'OK: Is a duplicate of R0000836'),
        ('DOE-nitfix_specimen_photos', 'R0000895', 1, 'OK: Genuine duplicate'),
        ('DOE-nitfix_specimen_photos', 'R0000937', 1, 'OK: Genuine duplicate'),
        ('DOE-nitfix_specimen_photos', 'R0001055', 1, 'OK: Genuine duplicate'),

        ('HUH_DOE-nitfix_specimen_photos', 'R0001262', 1,
         'OK: Duplicate of R0001263'),
        ('HUH_DOE-nitfix_specimen_photos', 'R0001729', 1,
         'OK: Duplicate of R0001728'),

        ('OS_DOE-nitfix_specimen_photos', 'R0000229', 1,
         'OK: Genuine duplicate'),
        ('OS_DOE-nitfix_specimen_photos', 'R0001835', 1,
         'OK: Genuine duplicate'),
        ('OS_DOE-nitfix_specimen_photos', 'R0001898', 1,
         'OK: Genuine duplicate'),

        ('CAS-DOE-nitfix_specimen_photos', 'R0001361', 1,
         'OK: Genuine duplicate'),
        ('CAS-DOE-nitfix_specimen_photos', 'R0002349', 1,
         'OK: Genuine duplicate'),

        ('MO-DOE-nitfix_specimen_photos', 'R0002933', 1,
         'OK: Genuine duplicate'),
        ('MO-DOE-nitfix_specimen_photos', 'R0003226', 1,
         'OK: Genuine duplicate'),
        ('MO-DOE-nitfix_specimen_photos', 'R0003663', 1,
         'OK: Manually fixed'),
        ('MO-DOE-nitfix_specimen_photos', 'R0003509', 0,
         'ERROR: Blurry image')]


if __name__ == '__main__':
    ingest_images()
