"""Build a Notes from Nature expedition."""

from os.path import split
from pathlib import Path
import csv
import lib.db as db


EXPEDITION_DIR = Path('data') / 'raw' / 'MO-DOE-nitfix_specimen_photos'
MANIFEST = Path('data') / 'interim' / 'mobot_expedition.csv'
FILE_PATTERN = EXPEDITION_DIR / '%.JPG'


def create_manifest(images):
    """Create expedition manifest."""
    with open(MANIFEST, 'w') as csv_file:
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
    with db.connect() as cxn:
        IMAGES = db.get_images_taxons(cxn, FILE_PATTERN)
    create_manifest(IMAGES)
