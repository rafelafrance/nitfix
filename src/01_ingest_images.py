"""Extract, transform, and load data related to the images."""

import os
from math import ceil
from glob import glob
from pathlib import Path
from itertools import chain
import multiprocessing
from collections import namedtuple
import pandas as pd
from PIL import Image, ImageFilter
import zbarlight
import lib.db as db
import lib.google as google


Dimensions = namedtuple('Dimensions', 'width height')

CPUS = min(10, os.cpu_count() - 4 if os.cpu_count() > 4 else 1)
RAW_DATA = Path('.') / 'data' / 'raw'
INTERIM_DATA = Path('.') / 'data' / 'interim'

IMAGE_DIRS = {
    'osu': RAW_DATA / 'OS_DOE-nitfix_specimen_photos',
    'cas': RAW_DATA / 'CAS-DOE-nitfix_specimen_photos',
    'mobot': RAW_DATA / 'MO-DOE-nitfix_specimen_photos',
    'nybg_1': RAW_DATA / 'DOE-nitfix_specimen_photos',
    'nybg_2': RAW_DATA / 'NY_visit_2',
    'nybg_3': RAW_DATA / 'NY_DOE-nitfix_visit3',
    'nybg_4': RAW_DATA / 'NY_DOE-nitfix_visit4',
    'harvard': RAW_DATA / 'HUH_DOE-nitfix_specimen_photos'}


def ingest_images():
    """Process image files."""
    cxn = db.connect()

    old_images = pd.read_sql('SELECT * FROM raw_images', cxn)
    old_errors = pd.read_sql('SELECT * FROM image_errors', cxn)
    old_images.image_file = old_images.image_file.str[3:]  # DELETE ME !!!!!!!!
    old_errors.image_file = old_errors.image_file.str[3:]  # DELETE ME !!!!!!!!

    image_files = get_images_to_process(old_images, old_errors)
    batches = split_images_into_batches(image_files)

    with multiprocessing.Pool(processes=CPUS) as pool:
        results = [pool.apply_async(ingest_batch, (b, )) for b in batches]
        results = [r.get() for r in results]

    new_images = list(chain(*[batch[0] for batch in results]))
    new_errors = list(chain(*[batch[1] for batch in results]))
    new_images = pd.DataFrame(new_images)
    new_errors = pd.DataFrame(new_errors)

    # create_tables(cxn)
    #
    # read_harvard_herbaria(cxn, skip_images)
    # read_osu_herarium(cxn, skip_images)
    # read_cas_herbarium(cxn, skip_images)
    # read_mobot(cxn, skip_images)
    # read_nybg_1(cxn, skip_images)
    # read_nybg_2(cxn, skip_images)
    # read_nybg_3(cxn, skip_images)
    # read_nybg_4(cxn, skip_images)
    #
    # read_pilot_data(cxn, images)
    # read_corrales_data(cxn, images)


def get_images_to_process(old_images, old_errors):
    """Get all image file names not in the database."""
    skip_images = set(old_images.image_file) | set(old_errors.image_file)

    image_files = []
    for image_dir in IMAGE_DIRS.values():
        pattern = str(image_dir / '*.JPG')
        image_files += glob(pattern)
    image_files = [f for f in image_files if f not in skip_images]
    return sorted(image_files)


def split_images_into_batches(image_files):
    """Split images into batches we can put into subprocesses."""
    total = len(image_files)
    step = ceil(total / CPUS)
    return [image_files[i:i + step] for i in range(0, total, step)]


def qq_ingest_batch(image_batch):
    """A."""
    return image_batch[:-3], image_batch[-3:]


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


def get_image_data(image_file):
    """Read and process image."""
    with open(image_file, 'rb') as image_file:
        image = Image.open(image_file)
        image.load()
    return get_qr_code(image)


def get_qr_code(image):
    """Extract QR code from image."""
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


def resolve_error(cxn, dir_name, image_file, ok, resolution):
    """Resolve an error."""
    image_file = str(dir_name / f'{image_file}.JPG')
    sql = """
        UPDATE image_errors
           SET ok = ?, resolution = ?
         WHERE image_file = ?"""
    cxn.execute(sql, (ok, resolution, image_file))
    cxn.commit()


def manual_insert(cxn, dir_name, image_file, sample_id, skip_images):
    """Manually set an image record."""
    image_file = str(dir_name / f'{image_file}.JPG')
    if image_file in skip_images:
        return
    sql = 'INSERT INTO raw_images (sample_id, image_file) VALUES (?, ?)'
    cxn.execute(sql, (sample_id, image_file))
    cxn.commit()


def read_nybg_1(cxn, skip_images):
    """Read New York Botanical Garden (1st trip)."""
    path = RAW_DATA / 'DOE-nitfix_specimen_photos'

    ingest_image_batch(path, skip_images)

    resolve_error(cxn, path, 'R0000149', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0000151', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0000158', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0000165', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0000674', 1, 'OK: Is a duplicate of R0000473')
    resolve_error(cxn, path, 'R0000835', 1, 'OK: Is a duplicate of R0000836')
    resolve_error(cxn, path, 'R0000895', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0000937', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0001055', 1, 'OK: Genuine duplicate')


def read_harvard_herbaria(cxn, skip_images):
    """Read Harvard Herbaria."""
    path = RAW_DATA / 'HUH_DOE-nitfix_specimen_photos'

    ingest_image_batch(cxn, path, skip_images)

    resolve_error(cxn, path, 'R0001262', 1, 'OK: Is a duplicate of R0001263')
    resolve_error(cxn, path, 'R0001729', 1, 'OK: Is a duplicate of R0001728')


def read_osu_herarium(cxn, skip_images):
    """Read Ohio State University Herbarium."""
    path = RAW_DATA / 'OS_DOE-nitfix_specimen_photos'

    ingest_image_batch(cxn, path, skip_images)

    resolve_error(cxn, path, 'R0000229', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0001835', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0001898', 1, 'OK: Genuine duplicate')


def read_cas_herbarium(cxn, skip_images):
    """Read California Academy of Sciences Herbarium."""
    path = RAW_DATA / 'CAS-DOE-nitfix_specimen_photos'

    ingest_image_batch(cxn, path, skip_images)

    resolve_error(cxn, path, 'R0001361', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0002349', 1, 'OK: Genuine duplicate')


def read_mobot(cxn, skip_images):
    """Read Missouri Botanical Garden."""
    path = RAW_DATA / 'MO-DOE-nitfix_specimen_photos'

    ingest_image_batch(cxn, path, skip_images)

    resolve_error(cxn, path, 'R0002933', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0003226', 1, 'OK: Genuine duplicate')
    resolve_error(cxn, path, 'R0003663', 1, 'OK: Manually fixed')
    resolve_error(cxn, path, 'R0003509', 0, 'ERROR: Blurry image')

    manual_insert(
        cxn,
        path,
        'R0003663',
        '2eea159f-3c25-42ef-837d-27ad545a6779',
        skip_images)


def read_nybg_2(cxn, skip_images):
    """Read New York Botanical Garden (2nd trip)."""
    path = RAW_DATA / 'NY_visit_2'

    ingest_image_batch(cxn, path, skip_images)


def read_nybg_3(cxn, skip_images):
    """Read New York Botanical Garden (3rd trip)."""
    path = RAW_DATA / 'NY_DOE-nitfix_visit3'

    ingest_image_batch(cxn, path, skip_images)


def read_nybg_4(cxn, skip_images):
    """Read New York Botanical Garden (3rd trip)."""
    path = RAW_DATA / 'NY_DOE-nitfix_visit4'

    ingest_image_batch(cxn, path, skip_images)


def read_pilot_data(cxn, images):
    """Read pilot data."""
    csv_name = 'pilot.csv'
    csv_path = INTERIM_DATA / csv_name

    google.sheet_to_csv('UFBI_identifiers_photos', csv_path)

    pilot = pd.read_csv(csv_path)

    pilot['image_file'] = pilot['File'].apply(
        lambda x: f'../data/raw/UFBI_sample_photos/{x}')

    pilot.drop(['File'], axis=1, inplace=True)
    pilot.rename(columns={'Identifier': 'pilot_id'}, inplace=True)
    pilot.pilot_id = pilot.pilot_id.str.lower().str.split().str.join(' ')

    name = 'raw_pilot'
    pilot.to_sql(name, cxn, if_exists='replace', index=False)

    already_in = pilot.sample_id.isin(images.sample_id)
    pilot = pilot[~already_in]

    pilot.drop('pilot_id', axis=1, inplace=True)
    pilot.to_sql('raw_images', cxn, if_exists='append', index=False)


def read_corrales_data(cxn, images):
    """Read Corrales data."""
    csv_name = 'corrales.csv'
    csv_path = INTERIM_DATA / csv_name

    google.sheet_to_csv('corrales_data', csv_path)

    corrales = pd.read_csv(csv_path)
    corrales.corrales_id = corrales.corrales_id.str.lower()
    corrales.head()

    name = 'raw_corrales'
    corrales.to_sql(name, cxn, if_exists='replace', index=False)

    already_in = corrales.sample_id.isin(images.sample_id)
    corrales = corrales[~already_in]

    corrales.drop('corrales_id', axis=1, inplace=True)
    corrales.to_sql('raw_images', cxn, if_exists='append', index=False)


if __name__ == '__main__':
    ingest_images()


# def create_images_table(cxn):
#     """Create images and errors tables."""
#     # cxn.execute('DROP TABLE IF EXISTS raw_images')
#     cxn.execute("""
#         CREATE TABLE raw_images (
#             sample_id  TEXT PRIMARY KEY NOT NULL,
#             image_file TEXT NOT NULL UNIQUE
#         )""")
#     cxn.execute("""CREATE INDEX image_idx ON raw_images (sample_id)""")
#     cxn.execute("""CREATE INDEX image_file ON raw_images (image_file)""")
#
#
# def create_errors_table(cxn):
#     """Create errors table for persisting errors."""
#     # cxn.execute('DROP TABLE IF EXISTS image_errors')
#     cxn.execute("""
#         CREATE TABLE errors (
#             image_file TEXT NOT NULL,
#             msg        TEXT,
#             ok         INTEGER,
#             resolution TEXT
#         )""")
#     cxn.execute("""CREATE INDEX error_idx ON image_errors (image_file)""")
#
#
# def create_tables(cxn):
#     """Create image and image_errors tables if they do not exist."""
#     sql = """
#       SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{}'
#         """
#
#     exists = cxn.execute(sql.format('raw_images')).fetchone()[0]
#     if not exists:
#         create_images_table(cxn)
#
#     exists = cxn.execute(sql.format('image_errors')).fetchone()[0]
#     if not exists:
#         create_errors_table(cxn)
#
#
# def qq_ingest_image_batch(cxn, dir_name, skip_images):
#     """Ingest image batch."""
#     pattern = str(dir_name / '*.JPG')
#
#     sample_ids = {}  # Keep track of already used sample_ids
#
#     images = []  # A batch of images to insert
#     errors = []  # A batch of errors to insert
#
#     files = sorted(glob(pattern))
#
#     for image_file in tqdm(files):
#         if image_file in skip_images:
#             continue
#
#         sample_id = get_image_data(image_file)
#
#         # Handle a missing sample ID
#         if not sample_id:
#             msg = 'MISSING: QR code missing in {}'.format(image_file)
#             errors.append((image_file, msg))
#
#         # Handle a duplicate sample ID
#         elif sample_ids.get(sample_id):
#             msg = ('DUPLICATES: Files {} and {} have the same '
#                    'QR code').format(sample_ids[sample_id], image_file)
#             errors.append((image_file, msg))
#
#         # The image seems OK
#         else:
#             sample_ids[sample_id] = image_file
#             images.append((sample_id, image_file))
#
#     # Insert the image and error batches
#     if images:
#         sql = 'INSERT INTO raw_images (sample_id, image_file) VALUES (?, ?)'
#         cxn.executemany(sql, images)
#         cxn.commit()
#
#     if errors:
#         sql = 'INSERT INTO image_errors (image_file, msg) VALUES (?, ?)'
#         cxn.executemany(sql, errors)
#         cxn.commit()
