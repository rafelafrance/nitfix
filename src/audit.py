"""Run some sanity checks on the data."""

import os
from glob import glob
import lib.db as db
import lib.util as util


def audit():
    """Run the audit."""
    cxn = db.connect()
    check_image_records(cxn)


def check_taxon_ids(cxn):
    """Check that the taxon counts line up."""


def check_image_records(cxn):
    """Check that the image counts line up."""
    image_files = 0
    for image_dir in util.IMAGE_DIRS:
        pattern = os.fspath(util.IMAGE_ROOT / image_dir / '*.JPG')
        image_files += len(glob(pattern))

    sql = """SELECT COUNT(*) FROM raw_images"""
    image_recs = cxn.execute(sql).fetchone()[0]

    errors = cxn.execute('SELECT COUNT(*) FROM image_errors').fetchone()[0]

    sql = """SELECT COUNT(*) FROM raw_images
              WHERE image_file LIKE 'UFBI_sample_photos/%'"""
    pilot = cxn.execute(sql).fetchone()[0]

    sql = """SELECT COUNT(*) FROM raw_images
              WHERE image_file LIKE 'missing_photos/%'"""
    missing = cxn.execute(sql).fetchone()[0]

    check = image_recs
    check += errors
    check -= image_files
    check -= pilot
    check -= missing
    result = 'fail' if check else 'pass'

    print(f'Image records:    {image_recs:6,d}')
    print(f'Image errors:   + {errors:6,d}')
    print(f'Image files:    - {image_files:6,d} This includes errors')
    print(f'Pilot images:   - {pilot:6,d} No files for these')
    print(f'Missing images: - {missing:6,d} No files for these')
    print(f'Check:          = {check:6,d} {result}')


if __name__ == '__main__':
    audit()
