"""Build a Notes from Nature expedition."""

from os.path import join, split
import csv
import lib.db as db


EXPEDITION_DIR = 'data/OS_DOE-nitfix_specimen_photos'
MANIFEST = 'osu_expedition.csv'
FILE_PATTERN = EXPEDITION_DIR + '/%.JPG'


def create_manifest(images):
    """Create expedition manifest."""
    csv_path = join(EXPEDITION_DIR, MANIFEST)
    with open(csv_path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            ['image_name', 'provider_id', 'qr_code', 'resolved_name'])
        for image in images:
            _, file_name = split(image['file_name'])
            writer.writerow([
                file_name,
                image['provider_id'],
                image['sample_id'],
                image['scientific_name']])


if __name__ == '__main__':
    with db.connect() as db_conn:
        IMAGES = db.get_images_taxons(db_conn, FILE_PATTERN)
    create_manifest(IMAGES)
