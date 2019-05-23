"""A grab bag of utilities."""

from os.path import basename
from pathlib import Path
import zipfile
import pandas as pd
import lib.util as util
import lib.db as db


PHOTOS = Path('..') / 'Dropbox'


def image_zip():
    """Get the images for the files associated with the image IDs."""
    sample_ids = [f"'{id}'" for id in _get_sample_ids()]
    sql = f"SELECT * FROM images WHERE sample_id IN ({','.join(sample_ids)});"
    images = pd.read_sql(sql, db.connect())

    csv_file = util.TEMP_DATA / 'image_data.csv'
    images.to_csv(csv_file, index=False)

    zip_file = util.TEMP_DATA / 'image_data.zip'
    with zipfile.ZipFile(zip_file, mode='w') as zippy:
        zippy.write(csv_file,
                    arcname=basename(csv_file),
                    compress_type=zipfile.ZIP_DEFLATED)
        for image in images.image_file:
            path = PHOTOS / image
            zippy.write(
                path, arcname=image, compress_type=zipfile.ZIP_DEFLATED)


def _get_sample_ids():
    return [
        'a89546ed-2278-4e25-a9a2-0836d223fb6b',
        'c84c6871-887f-479e-bf1e-ff1c68b1c490',
        'c8ed1cae-805a-49a9-8234-af21d8c0d6a0',
        'c8c33923-6d14-42ea-a879-2418426d7708',
        'c89c6644-9f5a-4405-ada4-486827e7ce14',
        'c8e52dfa-ac86-4996-b5b0-4505c3e669d1',
        'c730cd6a-dadd-4956-8322-33c128f191e6',
        'c714ab6f-a0fb-4c84-b8d9-0ab1ab9e24c1',
        'b9bdb69c-fd34-4603-b981-69c8222ac255',
        'b91c54fe-461b-4e61-a233-74ceebd167d5',
        'b9b1583f-0a1d-4dd2-bd74-bae4d636181e',
        'baa6db45-81b5-454e-9c53-34436d71a006',
        'bb089eb5-5ae7-408b-b070-8f15b925afea',
        'bb29160c-2a73-401d-b0a8-f0ad6c415173',
        'c10b53db-96c8-4eb5-af79-dd22b00e3b60',
        'ba9f2209-f421-4a42-ab5f-1f426d1dabcd',
        '8ac2a3a7-01c3-4601-a5a8-686625ba8cea',
        '89e6df67-b261-44e8-813e-b107a96b6ba0',
        '817f087d-9349-4ba7-9638-1dda6c77de80',
        '8b58d447-31d7-4826-b46e-50785d8238c1',
        '8b6e0223-7fbe-4efc-a1e2-6c934da06685',
        '8b471b5e-cc2d-4047-865a-89923e28e34d',
        '7ee22e13-7c71-47ca-8614-ef6d12171bfe',
        '8b53cbe2-6072-44e5-aa32-7cfd33294e57',
        '8b37c595-0cf0-46a7-8cbf-10d3e79254fb',
        '7ee24628-33da-4b12-9122-43eec72fe837',
        '7ef1f930-2e30-47af-9c9a-57c648a405d6',
        'b4f69f83-90f7-4f96-89bc-ae1ce834d8ce',
        'ac4b3d47-7820-44dd-b0f5-878eab3e556e',
        'a457f490-aca8-4dc9-a31e-5b6a4673eca4',
        '8b344de5-e4b6-4a24-8641-0e27d3096242',
        '8b3dece5-0b80-40cf-a7aa-6a2c96be3ad6',
        'a737534d-7b94-45c9-92c6-f2e45cf7ef7c',
        'b5d45e4b-ef21-4f1b-927d-a1f1e0198763',
        'bce2c6d8-fb01-4a3e-889c-605c48a4c5b6',
        'abef514e-3ea1-4049-9311-f39fdebfabc5',
        'b60b17d4-87d9-4296-87ee-7febc0b848a1',
        'af636490-a31d-4e1a-89c3-df5b3061c32d',
        'af310769-c3c4-4a9c-8220-39d67730cb02',
        'bc81aa8e-8116-4a07-9ee2-2fbe4074d2a7',
        'adb0b357-f57b-4b93-bc79-f1b4d56816c7',
        'ad1ad63f-ebc3-4ee5-bace-314cf8882499',
        'ad5a75fa-adf4-483d-9209-e21b5affeeb8',
        'aecbdbe1-2cc2-4773-9752-3207c902133e',
        'e507d82b-6d3a-466c-99c6-6df57b77577d',
        'b7787033-1023-413a-b41a-e6359cb0402c',
        'b7144f8a-149a-41dc-b6f6-cbf7016b6a61',
        'b5c6b207-911d-4935-aa59-cf09baa1c98a',
        'e75ea922-68f6-4f4f-88a2-564d640396b6',
        'b45d4796-96d2-47af-8d3e-9ff2d482dcad',
        'b0650fdc-19f2-4d05-ab12-a5d4fd3e20ad',
        'b76d70a8-8bd6-4f6d-b1b0-937d21fba569',
        'e59983ec-25ba-479d-89f0-6f3d38dde835',
        'b787147e-2039-4fc6-b1f1-f76b88905c63',
        'e6648cc7-3854-4002-807d-aade98d69185',
        'bb453f8c-7cce-4c15-8b49-9cfd36a35cad',
        'a44674eb-9a44-469e-a63f-8d062d9e1c60',
        'a4bb0be5-2db7-4308-b59a-350a2c4fb90e',
        '8abd1caa-8aca-452b-8aac-a5478c3fa67a',
        '49520e89-ecc9-4ead-a269-0881390851ab',
        '826151c5-cad9-4aef-932b-0c1e8dde5364',
        '830bfb9d-c3f6-49c7-b4de-8ce1882ea0df',
        '618c3bd7-e8c4-47b5-88c4-65e8d6eb4092',
        'b1142568-9c26-45f2-a736-8ffe2248daae',
        'b1887b94-8cc1-4e35-9904-209e888a34c2',
        'c0ac07fe-b8f3-48fb-bdc1-0aabd03f1538',
        'c913b4a1-abe4-4271-a69b-24aa097ae7c4',
        'bec486d0-15a7-4eb2-98f4-de18881a5646',
        'e817e610-0b82-4dcf-8c6d-2a06fe2d1715',
        'b8c30d01-7040-4812-8e97-b5162e428d1c',
        'be780c11-da5e-43d2-9e41-b2fa27aeee96',
        'baf1c023-1d2e-4cab-b480-ae32e46f69e0',
        'cc9236bc-a0dd-46ba-a05d-2efebada1c79',
        'bb021b22-54e0-4412-a9cf-a9235419c97e',
        '00ba1fb6-feba-4bdb-83f9-9af440cd07b5',
        'fd62c73b-aa74-438e-83bc-e3eb0b5371b5',
        '008a54c8-4051-4b77-afd2-e0be2519a7c4',
        'a6364104-6912-4e76-8fd3-547b64ac7b0d',
        '88be1cc0-4494-4906-953a-cc3caa7cccf4',
        '8282bf45-4b11-49ee-9769-7da8118e2922',
        '808caf10-298f-4542-a891-0cf18c20cff0',
        '828a1bb6-d6cc-4751-bb4d-a0567a6f05e7',
        '82a4e40e-d31e-4fed-95fc-c6b163666c6a',
        '82d7399a-aaa6-4823-9919-56dc123fd44e',
        '82ab80a0-1f20-4e7f-8797-5a258a8880dc',
        '81be3ee5-9e7a-4bf6-8203-4e133c62fa3f',
        '7efc7f0b-d72a-4262-a97a-76541199ae30',
        '7f137da8-3d83-4213-86d0-9da3a8da484b',
        '7f65b436-06b4-41e1-b27a-d04b220ec92d',
        '7f8a780c-80ab-49f3-9d98-b895723c7742',
        '4281f9dc-c0af-4199-af1c-c4428bf7623f',
        '4291443f-46e3-43fb-a757-72b955d73fc5',
        '4283fd82-97bf-4da4-bf42-fb6f578100a2',
        '4276142b-0bd4-4616-a6a2-1c4aa68d251b',
        '42790eed-d40b-45e9-a8cd-60b8c2373240',
        '4260c85b-fee8-4f28-9ae3-daa0de8e9f38']


if __name__ == '__main__':
    image_zip()
