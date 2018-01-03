"""Build a Notes from Nature expedition."""

import os
from os.path import join, split
import csv
from shutil import copyfile
import lib.db as db


EXPEDITION_DIR = join('data', 'osu_expedition')
PROVIDER = ''
MANIFEST = 'osu_expedition.csv'
FILE_PATTERN = 'data/OS_DOE-nitfix_specimen_photos/%'


def copy_images(images):
    """Copy images to the expedition repository."""
    for image in images:
        _, file_name = split(image['file_name'])
        path = join(EXPEDITION_DIR, file_name)
        copyfile(image['file_name'], path)


def create_manifest(images):
    """Create expedition manifest."""
    csv_path = join(EXPEDITION_DIR, MANIFEST)
    with open(csv_path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ['image_name', 'provider_id', 'qr_code', 'resolved_name'])
        for image in images:
            _, file_name = split(image[0])
            writer.writerow([
                file_name,
                image['provider_id'],
                image['image_id'],
                image['scientific_name']])


def main():
    """Create expedition."""
    with db.connect() as db_conn:
        images = db.get_images_taxonomies(db_conn, FILE_PATTERN)
    os.makedirs(EXPEDITION_DIR, exist_ok=True)
    copy_images(images)
    create_manifest(images)


if __name__ == '__main__':
    main()
