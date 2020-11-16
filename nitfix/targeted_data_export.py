"""Handle data export requests.

These are all one-off requests that have different requirements so only one function
will be "active" at a time.
"""

import csv
import os
import sqlite3
from os.path import dirname

import pandas as pd
from PIL import Image

import lib.db as db
import lib.util as util

REQUEST_DIR = util.RAW_DATA / 'export_requests'

CXN = db.connect()
CXN.row_factory = sqlite3.Row


def export_images(df: pd.DataFrame, image_dir, factor=0.75):
    """Get the target images."""
    image_zip_dir = util.TEMP_DATA / image_dir
    os.makedirs(image_zip_dir, exist_ok=True)

    missing = []
    for _, row in df.iterrows():
        image_file = row['image_file']
        if not image_file:
            missing.append((row['sample_id'], row['sci_name']))
            continue
        src = util.PHOTOS / image_file
        dst = image_zip_dir / image_file.replace('/', '_')
        original = Image.open(src)
        transformed = original.resize((
            int(original.size[0] * factor),
            int(original.size[1] * factor)))
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

    for image in missing:
        print(image)


def export_mirbelioids():
    """Export all Merbelioids images and NfN data from a given list in a CSV file."""
    with open(REQUEST_DIR / 'Mirbelioids_Data.csv') as csv_file:
        reader = csv.reader(csv_file)
        sample_ids = {r[7] for r in reader}
    values = [f"('{s}')" for s in sample_ids]
    values = ','.join(values)

    sql = f"""
          with targets(sample_id) as (select distinct * from (values {values}))
        select *
          from taxonomy_ids
     left join taxonomy using (sci_name)
     left join images using (sample_id)
     left join nfn_data using (sample_id)
         where sample_id in (select sample_id from targets);
        """
    df = pd.read_sql(sql, CXN)
    df.to_csv(util.TEMP_DATA / 'Mirbelioids_data_2020-11-16a.csv', index=False)
    export_images(df, 'Mirbelioid_images')


if __name__ == '__main__':
    export_mirbelioids()
