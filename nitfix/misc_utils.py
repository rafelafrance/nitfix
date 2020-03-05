"""A grab bag of utilities."""

import re
from os.path import basename
from pathlib import Path
import zipfile
import pandas as pd
import lib.util as util
import lib.db as db


PHOTOS = Path('.') / 'data' / 'raw' / 'photos'

IS_UUID = re.compile(
    r""" \b [0-9a-f]{8} - [0-9a-f]{4} - [1-5][0-9a-f]{3}
        - [89ab][0-9a-f]{3} - [0-9a-f]{12} \b """,
    flags=re.IGNORECASE | re.VERBOSE)

MISSING = '<missing/>'


def image_zip():
    """Get images from a list and zip them."""
    cxn = db.connect()

    sample_ids = _get_sample_ids()
    images = []

    for sample_id in sample_ids:
        if IS_UUID.search(sample_id):
            sql = """SELECT image_file FROM images WHERE sample_id = ?"""
        else:
            sql = """SELECT image_file FROM pilot_data WHERE pilot_id = ?"""
        cur = cxn.cursor()
        cur.execute(sql, (sample_id, ))
        row = cur.fetchone()
        images.append(row[0] if row else MISSING)

    csv_file = util.TEMP_DATA / 'Images_2020-03-05a.csv'
    csv_data = pd.DataFrame({'sample_id': sample_ids, 'image_file': images})
    csv_data.to_csv(csv_file, index=False)

    zip_file = util.TEMP_DATA / 'Images_list_2020-03-05a.zip'
    with zipfile.ZipFile(zip_file, mode='w') as zippy:
        zippy.write(csv_file,
                    arcname=basename(csv_file),
                    compress_type=zipfile.ZIP_DEFLATED)
        for image in images:
            if image == MISSING:
                continue
            path = PHOTOS / image
            zippy.write(
                path, arcname=image, compress_type=zipfile.ZIP_DEFLATED)


def _get_sample_ids():
    return [
        'ny: telles 7394',
        'ny: lewis 1723',
        'ny: carvalho 5909',
        '3aa815bd-d4cf-458b-822c-a97e0f88d50d',
        '3ac5f8c7-fef3-41cd-88ab-31f1caf5484a',
        '3acd1733-57aa-424c-a311-864b978635aa',
        'b9b1583f-0a1d-4dd2-bd74-bae4d636181e',
        '0e9ccc62-f031-4d92-8a0b-527cec45bb0d',
        'ny: beaman 11372',
        'a3267333-837d-4875-adad-9da7270d75e6',
        'a1b87db5-e73d-4b31-a212-8e3e40ac1ec7',
        'tex: church 729',
        '1350a3ff-6512-42f2-8128-d5dcb2097172',
        '253a5672-5f75-4fa1-94d0-26507768dcce',
        'b91cd5af-b17f-4138-9798-78d308f2ee31',
        'tex: cuevas 78',
        'tex: ballesteros 344',
        '8252b59d-0a19-49ab-a62e-420d20a37e36',
        'tex: henrickson 8292',
        'tex: webster 33479',
        '3f068026-62bb-45d5-99d1-a703b088f33a',
        '3f05a8e4-12a2-4c70-9628-13d7796bb8dc',
        'tex: webster 4926',
        'tex: correll 42435',
        '3ef8577e-67f2-4813-9f3b-332cdf53f95c',
        '088b524c-b577-4d8f-8a4c-10404d60bd12',
        '0ff29671-cac5-4aec-a001-82c1ea01d707',
        '0fe6fec2-5bc1-484d-9ebb-eea94f17ea93',
        '9fad1c62-2240-4a74-9b11-5796693b4049',
        '3ef7b767-60e1-4401-b82b-c3a78d5855e7',
        '0fe6987a-c259-4edd-918c-4d2e063c6372',
        '0fded544-6fc7-44b2-99e3-337b76a2b16e',
        '0fd1f6c3-fe08-413f-a365-db6aab8bb418',
        'tex: howard 246',
        '108a5119-3f74-447e-9cd6-95e878f562f8',
        '3c9812d4-762a-4ae1-b49d-2552aaf4dbe6',
        '3c8438c9-f6ca-4f1c-8da2-fedc94876df7',
        '8b3dece5-0b80-40cf-a7aa-6a2c96be3ad6',
        '3c823c0d-2a6c-43e2-bd1e-7626efa8c35b',
        '0d5b4d06-5b74-4eda-b795-ac4aa2c0191e',
        '0d644710-2d49-41ba-a2ff-50887c28f240',
        'tex: phillipson 3166',
        'tex: contreras 8742',
        '0de59b5a-2df0-4cb3-9295-eebb664ffc02',
        '3beee92d-04a4-4d6d-9f5d-6416ac843c86',
        '3bdaecde-b52b-49b1-b580-c2bbcaa7487d',
        '0ca7f306-b0fd-49eb-b242-d4a0999a1576',
        '3f2ee9bf-ad3b-4deb-9424-1135314794d0',
        'tex: seigler ds-14450',
        'tex: pereira 2263',
        '3bda1fa1-dec3-47ce-aade-84ca875ceaaf',
        '3bdfc29f-2309-4e8d-9510-57d803ce5879',
        '3be63f7f-6f4f-4a70-8df8-4d45d0ea5b5f',
        '40a378ac-e289-4c0e-aa21-8b8248c52c59',
        '3aa0ebcb-2137-4e9f-9092-936dff10971a',
        '3aa0b8e9-1fe2-4875-8f70-1e1f30e3a000',
        '0b513105-f917-43b9-88bd-00939471d92e',
        'tex: turner 5255',
        '40462f12-a4b9-4a3b-93bb-c9dbe82a3b25',
        'tex: donner 10079',
        'tex: panero 7161',
        'tex: panero 6950',
        'a23ab012-7658-4e8b-97c3-705db38a0639',
        '09b6e692-1c6b-4d33-89a3-ef68c847d380',
        'tex: santos-guerra sgrjh 085',
        '09b936c2-9a8b-4736-bfb5-f3129ddfc013',
        '41daf6aa-1284-4fdd-95dc-ed4e625de6ff',
        'c31e5dd6-bb66-4f9c-a922-3ae282ca8d79',
        'tex: campos v4458',
        '12605d32-7db7-4ebe-b424-239f0f2ca70b',
        '39620ad5-ac9d-4abe-a184-8c2a0bb08bd0',
        'tex: vlastimil 3320',
        '3a5b4c2e-398d-49bc-91b9-35157da4325c',
        '3c374396-b060-4d95-86e4-58ad1f0db2b1',
        'tex: lewis 881795',
        'tex: tupayachi 810',
        '3a5cc0f5-e13f-4d2c-ab8e-3114e6d98e2c',
        '39523c91-f722-4855-842a-16dea817dbf9',
        '3a5f3373-6148-4937-bbad-cbab7f38e99d',
        '97963add-32c3-4936-9877-0d56af3efdd3',
        '3a5f33b7-59e7-448b-ba7f-2f4b4cf6f5a2',
        '3a66feae-9ed4-47e6-876a-95ffc08b7610',
        'tex: henrickson 22650',
        'tex: rosalinda 4902',
        'tex: lott 5453',
        '09d293f1-ba1e-4df6-8f6d-7d3a60e5ce65',
        '9998a6ef-cd69-4673-b3e6-c0494c271306',
        '09e3edb7-0e88-411a-b7e1-0b9bcf0def28',
        'tex: salinas 5964',
        'tex: landrum 11719',
        'tex: henrickson 23617',
        'tex: wendt 1222',
        ]


if __name__ == '__main__':
    image_zip()
