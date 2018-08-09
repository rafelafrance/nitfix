"""Extract, transform, and load data related to the images."""

import os
from os.path import join
from glob import glob
from pathlib import Path
from itertools import chain
import multiprocessing
from collections import namedtuple
import pandas as pd
from PIL import Image, ImageFilter
import zbarlight
import lib.db as db
import lib.util as util
import lib.google as google


Dimensions = namedtuple('Dimensions', 'width height')

PROCESSES = min(10, os.cpu_count() - 4 if os.cpu_count() > 4 else 1)
BATCH_SIZE = 100
INTERIM_DATA = Path('.') / 'data' / 'interim'


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

    with multiprocessing.Pool(processes=PROCESSES) as pool:
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

    # Add image records that don't have an image file.
    images = read_pilot_data(cxn, images)
    images = read_corrales_data(cxn, images)

    # Fix images where we have solutions to errors
    errors = resolve_errors(errors)
    images = manually_insert_images(images)

    images.to_sql('images', cxn, if_exists='replace', index=False)
    errors.to_sql('image_errors', cxn, if_exists='replace', index=False)


def get_old_images(cxn):
    """Get images already in the database."""
    # Handle the case where there is no image or error table in the DB.
    # NOTE: There will always be an error table if there is an image table.
    try:
        images = pd.read_sql('SELECT * FROM images', cxn)
        errors = pd.read_sql('SELECT * FROM image_errors', cxn)
        images.image_file = images.image_file.apply(util.normalize_file_name)
        errors.image_file = errors.image_file.apply(util.normalize_file_name)
    except pd.io.sql.DatabaseError:
        images = pd.DataFrame(columns=['image_file', 'sample_id'])
        errors = pd.DataFrame(
            columns=['image_file', 'msg'])
    return images, errors


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
                ('DUPLICATES: Files {} and {} have the same QR code').format(
                    dupe.image_file,
                    images.loc[images.sample_id == dupe.sample_id,
                               'image_file']),
            axis=1)
    dupes = dupes.drop(['sample_id'], axis=1)
    return images, dupes


def ingest_batch(image_batch):
    """Ingest image batch."""
    new_images = []
    new_errors = []

    for image_file in image_batch:
        sample_id = get_image_data(image_file)
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
        pattern = os.fspath(util.IMAGE_ROOT / image_dir / '*.JPG')
        file_names = glob(pattern)
        image_files += map(util.normalize_file_name, file_names)
    image_files = [f for f in image_files if f not in skip_images]
    return sorted(image_files)


def get_image_data(image_file):
    """Read and process image."""
    with open(util.IMAGE_ROOT / image_file, 'rb') as image_file:
        image = Image.open(image_file)
        image.load()
    return get_qr_code(image)


def get_qr_code(image):
    """
    Extract QR code from image.

    Try various methods to find the QR code in the image. Starting from
    quickest and moving to the most unlikely method.
    """
    qr_code = zbarlight.scan_codes('qrcode', image)
    if qr_code:
        return qr_code[0].decode('utf-8')

    qr_code = get_qr_code_using_slider(image)
    if qr_code:
        return qr_code

    qr_code = get_qr_code_by_rotation(image)
    if qr_code:
        return qr_code

    return get_qr_code_by_sharpening(image)


def get_qr_code_using_slider(image):
    """Try sliding a window over the image to search for the QR code."""
    for box in window_slider(image):
        cropped = image.crop(box)
        qr_code = zbarlight.scan_codes('qrcode', cropped)
        if qr_code:
            return qr_code[0].decode('utf-8')
    return None


def get_qr_code_by_rotation(image):
    """Try rotating the image to find the QR code *sigh*."""
    for degrees in range(5, 85, 5):
        rotated = image.rotate(degrees)
        qr_code = zbarlight.scan_codes('qrcode', rotated)
        if qr_code:
            return qr_code[0].decode('utf-8')
    return None


def get_qr_code_by_sharpening(image):
    """Try to sharpen the image to find the QR code."""
    sharpened = image.filter(ImageFilter.SHARPEN)
    qr_code = zbarlight.scan_codes('qrcode', sharpened)
    if qr_code:
        return qr_code[0].decode('utf-8')
    return None


def window_slider(image_size, window=None, stride=None):
    """
    Create slider window.

    It helps with feature extraction by limiting the search area.
    """
    window = window if window else Dimensions(400, 400)
    stride = stride if stride else Dimensions(200, 200)

    for top in range(0, image_size.height, stride.height):
        bottom = top + window.height
        bottom = image_size.height if bottom > image_size.height else bottom

        for left in range(0, image_size.width, stride.width):
            right = left + window.width
            right = image_size.width if right > image_size.width else right

            box = (left, top, right, bottom)

            yield box


def read_pilot_data(cxn, images):
    """Read pilot data."""
    csv_path = INTERIM_DATA / 'pilot.csv'

    google.sheet_to_csv('UFBI_identifiers_photos', csv_path)
    pilot = pd.read_csv(csv_path)

    # Create a fake path for the file name
    pilot['image_file'] = pilot['File'].apply(
        lambda x: f'UFBI_sample_photos/{x}')

    pilot = (pilot.drop(['File'], axis=1)
                  .rename(columns={'Identifier': 'pilot_id'}))
    pilot.pilot_id = pilot.pilot_id.str.lower().str.split().str.join(' ')

    pilot.to_sql('raw_pilot', cxn, if_exists='replace', index=False)

    already_in = pilot.sample_id.isin(images.sample_id)
    pilot = pilot[~already_in]

    pilot = pilot.drop('pilot_id', axis=1)
    return pd.concat([images, pilot], ignore_index=True, sort=True)


def read_corrales_data(cxn, images):
    """Read Corrales data."""
    csv_path = INTERIM_DATA / 'corrales.csv'

    google.sheet_to_csv('corrales_data', csv_path)
    corrales = pd.read_csv(csv_path)
    corrales.corrales_id = corrales.corrales_id.str.lower()

    corrales.to_sql('raw_corrales', cxn, if_exists='replace', index=False)

    already_in = corrales.sample_id.isin(images.sample_id)
    corrales = corrales[~already_in]

    corrales = corrales.drop('corrales_id', axis=1)
    return pd.concat([images, corrales], ignore_index=True, sort=True)


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
    df = pd.DataFrame(get_manual_inserts())
    return pd.concat([images, df], ignore_index=True)


def get_manual_inserts():
    """Get images that can need to be added manually."""
    return [
        {'image_file': join('CAS-DOE-nitfix_specimen_photos', 'R0002092'),
         'sample_id': '8b6e0223-7fbe-4efc-a1e2-6c934da06685'},
        {'image_file': join('MO-DOE-nitfix_specimen_photos', 'R0003663'),
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
