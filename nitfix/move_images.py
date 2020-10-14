"""Move color adjusted images as per comments.

1) Move the 2400 from prior adjustments into the an album_0.
2) Move files to make the remaining albums closer to 2000 (except album_8).
3) Create manifests of the albums.
"""

import csv
from collections import defaultdict
from shutil import copy, move
import pandas as pd
import lib.db as db
import lib.util as util


OUT_DIR = util.ADJUSTED_DIR


def move_2400():
    """Move the prior 2400 into album 0."""
    src = util.SAMPLED_DATA / 'nitfix_sample_2400_2020-04-07a.csv'
    dst = OUT_DIR / 'album_0.csv'
    copy(src, dst)

    df = pd.read_csv(dst)
    manifest_files = set(df['manifest_file'].tolist())

    dst = str(OUT_DIR / 'album_0')

    for i in range(1, 9):
        in_dir = OUT_DIR / f'album_{i}'
        for path in in_dir.glob('*'):
            if path.name in manifest_files:
                src = str(path)
                move(src, dst)


def even_out_dirs():
    """Move files to make albums closer to 2000 images per each."""
    for i in range(1, 7):
        album_7 = OUT_DIR / f'album_7'
        photos_7 = list(album_7.glob('*'))

        album_i = OUT_DIR / f'album_{i}'
        count = 2000 - len(list(album_i.glob('*')))

        if count <= 0:
            continue

        dst = str(album_i)
        for j in range(count):
            src = str(photos_7[j])
            move(src, dst)


def make_manifests():
    """Create manifests for the albums."""
    images = defaultdict(dict)
    with db.connect() as cxn:
        cursor = cxn.execute('select * from images')
        for image in cursor:
            manifest_file = image[0].replace('/', '_')
            images[manifest_file]['image_file'] = image[0]
            images[manifest_file]['sample_id'] = image[1]

    for i in range(1, 8):
        with open(OUT_DIR / f'album_{i}.csv', 'w') as manifest:
            writer = csv.writer(manifest)
            writer.writerow(['image_file', 'sample_id', 'manifest_file'])
            album = OUT_DIR / f'album_{i}'
            for photo in album.glob('*'):
                writer.writerow([
                    images[photo.name]['image_file'],
                    images[photo.name]['sample_id'],
                    photo.name,
                ])


if __name__ == '__main__':
    # move_2400()
    # even_out_dirs()
    make_manifests()
