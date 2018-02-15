"""Build a Notes from Nature expedition."""

import os
from os.path import join, split
import csv
from shutil import copyfile
import lib.db as db


EXPEDITION_DIR = join('data', 'osu_expedition')
PROVIDER = 'OSU'


def get_absent_data(db_conn):
    """Get absent data."""
    absent = []
    no_image = []
    csv_path = join('data', 'NYBarcodeSummary.csv')
    with open(csv_path) as csv_file:
        reader = csv.reader(csv_file)
        next(reader)
        for row in reader:
            if row[2] != 'Present':
                record = get_image_info(db_conn, row, no_image)
                if record:
                    absent.append(record)
    return absent, no_image


def get_image_info(db_conn, row, no_image):
    """Get the image info from the taxonomies and images tables."""
    taxonomy = db.get_taxonomy_by_provider(db_conn, PROVIDER, row[1])
    image = db.get_image(db_conn, taxonomy[5])
    if not image:
        no_image.append(('{} {}'.format(PROVIDER, row[1]), taxonomy[2]))
        return None
    return image[1], '{} {}'.format(PROVIDER, row[1]), image[0], taxonomy[2]


def copy_images(absent):
    """Copy images to the expedition repository."""
    for image in absent:
        _, file_name = split(image[0])
        path = join(EXPEDITION_DIR, file_name)
        copyfile(image[0], path)


def create_manifest(absent):
    """Create expedition manifest."""
    csv_path = join(EXPEDITION_DIR, 'nybg_expedition.csv')
    with open(csv_path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ['image_name', 'nybg_bar_code', 'qr_code', 'resolved_name'])
        for image in absent:
            _, file_name = split(image[0])
            writer.writerow([file_name, image[1], image[2], image[3]])


def main():
    """Create expedition."""
    with db.connect() as db_conn:
        absent, no_image = get_absent_data(db_conn)
    os.makedirs(EXPEDITION_DIR, exist_ok=True)
    copy_images(absent)
    create_manifest(absent)
    for problem in no_image:
        print(problem)


if __name__ == '__main__':
    main()
