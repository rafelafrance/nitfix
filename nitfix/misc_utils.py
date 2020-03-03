"""A grab bag of utilities."""

from os.path import basename
from pathlib import Path
import zipfile
import pandas as pd
import lib.util as util
import lib.db as db


PHOTOS = Path('.') / 'data' / 'raw' / 'photos'


def image_zip():
    """Get the images for the files associated with the image IDs."""
    sample_ids = [f"'{i}'" for i in _get_sample_ids()]
    sql = f"SELECT * FROM images WHERE sample_id IN ({','.join(sample_ids)});"
    images = pd.read_sql(sql, db.connect())

    csv_file = util.TEMP_DATA / 'UFIB_list_2020-03-03a.csv'
    images.to_csv(csv_file, index=False)

    zip_file = util.TEMP_DATA / 'UFIB_list_2020-03-03a.zip'
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
        '0217e1ed-15ae-4b72-ab38-9d7d4d953771',
        '0213106b-291e-43a9-9574-14d56c23bbb1',
        '01df7b41-200b-4295-b38b-a44287623e1f',
        '022b5bbf-60fd-4c6f-9c88-638dabf1c0e8',
        '01f88034-5aa8-4d79-b38d-9c313f27cdbb',
        '02276fa8-8d50-49f3-941f-e348a2ca2c55',
        '13a471a0-a6bd-4088-b463-06cd6356d79d',
        'dd21a192-49c9-43da-b394-ab1aed23a832',
        '022c8616-6027-4e4b-b2ea-71a94f7914d2',
        '9256d266-8fb1-4ddf-ba1d-bcaf9d3d27fe',
        '914463f7-e342-4120-a7b4-9c3aa87004fe',
        '8fa80c56-8ed6-4ddc-bd43-92dbd8eb9a39',
        '17ad7917-51b9-4915-b68f-8b4d9396523d',
        'fba5da6c-6751-4782-a2df-4797d6736aef',
        'fde2dd06-86bf-41dd-9779-e83c3f5c2c7e',
        '54941fc8-2459-451e-9eb3-ad40fa322cb8',
        '01841766-2994-4a5e-8e78-843d94e1915e',
        '095fe9cb-2f69-4fc6-ba79-c4b2e44c1ccc',
        'fdd10a7e-9152-463c-bdd3-7a6226e0ea3d',
        'c8391c5d-8865-4b73-87ae-466d690c78e8',
        '28944fa9-3fbd-4c44-9104-afcaac46a97d',
        'f1661493-85f8-480a-83f1-0c35695bc1d8',
        'c838b351-3f4f-467d-bfce-117283ab4208',
        '325b20e2-834e-4db9-9247-45d300eca8c6',
        'b00e0a6d-a2f3-4160-ab70-996790d5c008',
        '32555000-c14d-4a13-b013-513c9beb1a74',
        'fc9240a5-995f-4c94-ac3b-f080e2fc7aef',
        'fc8d0413-534b-4a13-a0bb-0c7a1bdd3f08',
        'fc8c6f3a-4ecf-4b89-8db0-07f4d8de64da',
        'c51ea264-a26f-4b4f-a0ed-13e0e76f3941',
        '322a9150-5e74-4177-bec8-3b594b84f2e5',
        '3229a488-e8d8-40ae-b566-87f235789913',
        '3224e3c6-0fc1-4f72-932b-d41908bc6c97',
        '70b464da-5d14-4588-a9f4-753ad07bcf4e',
        '7bd1933b-2e30-43c6-a878-7d19e6701d43',
        '7b029843-b321-403c-922b-7925514ac303',
        '7aab5e88-7ae9-453d-bf4f-75be18dc4cad',
        '7a81630e-4187-44bb-9aa8-4a375f3a92bf',
        '7a812975-1ccc-4b65-bb87-713ecac2db6c',
        'fc48bd9c-274e-42f6-9ae5-360684c5b71c',
        '78a408ea-14b6-4f27-bd9e-4d5a9162f071',
        '78bb8dbe-e5f4-4345-8956-1c5af0700316',
        '62a4a952-12bf-4530-933e-57237f35f6d3',
        'b2dd7db1-2e41-4283-9e63-585584ff7194',
        '83edc358-9845-40ce-829b-9582810d34cd',
        '3113019d-1347-478a-afac-ec0efdcb789a',
        '6ca4c891-0301-4b51-8fd4-3f9b430a6025',
        '215b1df4-d439-4925-a0bb-2f117a881f56',
        '31a2448d-2af9-4aaa-b440-163a2c5b593b',
        'fcf7b73a-a531-44f0-b0d8-9ed26780149d',
        '939934c9-ab3f-400e-8e37-e2b57760f29d',
        '93445eed-c722-4917-90cc-59d775946095',
        '93c8baeb-0b51-4f4c-802b-212893874b74',
        '5a941bcb-7120-4cc2-a34e-62f2c264a2f5',
        '949aef93-4ab0-40db-b1cb-2636bdedbfe7',
        '5171a46a-2cfd-49de-a06e-e92a9065edfa',
        '506bc5f9-f5c7-4016-b1eb-64e45ea1252d',
        '939816bb-94ad-48e2-b144-9fe6221213b4',
        '9384e0a6-85ea-451a-a7a3-bd73c4c635c0',
        '936b483c-d283-4e26-a98c-444dc4da3893',
        '935d74c5-9010-45b1-b55f-20609849b5a1',
        'cebc6003-a36d-4e63-b498-0db5be44771a',
        '7542c61a-89c6-40ba-a474-567830d68b37',
        '0187f509-943f-440f-bec1-8a9c2822347c',
        '018837df-ce27-4a0e-8c5a-64402dadc22f',
        '018d2e3e-6dca-4245-bd30-a3eb0ba05187',
        '785c38c6-efa6-4fad-9ef1-be001249360e',
        'ecc952cc-602a-4d7a-8f0c-5d4ec4332eac',
        'ecbabfcc-2739-43da-b306-297634a8162a',
        '0d2af541-ff06-41db-a41d-ecd92ad8ed1a',
        '0aac87a8-72e9-432a-9450-5cde1b056a1c',
        '0ac6d157-c3d0-4469-8cc0-ab7b71a8e0c6',
        '0d2cbc64-aa54-401a-83af-3d591ba13160',
        'eca81818-53c5-4d08-b3a8-400ade09b2ba',
        '0ab91335-0842-4412-ae72-7f17e0525efa',
        '0d291046-9e38-4637-a33d-4512cbb04bd2',
        '0d25df14-ab82-4abd-a0fc-32e2a23bf36e',
        '782d7c1b-9b4c-481c-8e37-8b6b8e9d04bc',
        '0ab8ad93-a888-41c7-b8e6-5d5444985a65',
        '7844f1ad-7590-4a0c-9c57-8b61f58ef4a5',
        '78576ff1-b2ff-40e9-acf5-f48c2560c0b3',
        '78654121-484e-4ed6-8e6b-5f94b63ad43b',
        '696ca42a-7715-400e-8474-321ea6ea5b6e',
        '0d2e563a-9b48-43e6-8645-bba0f88ce8f2',
        '5ebe0199-cc9f-4fb0-bc40-3e1423418dbc',
        'e29aaa15-e5b3-4e23-a7ec-7ea06508e0ff',
        '0b84254b-b9c8-4f5c-b829-8a55fcab941b',
        '8395d3b8-1bea-4abf-a96f-5ec93a9619c8',
        '427c2bf9-6e27-4bcc-83ef-b31102f0fd63',
        '99de22f9-753e-4ef8-9a80-d92ecc771162',
        '2fc6b254-9c7b-47f3-9c80-8ef010240e1e',
        '2fb9dff8-b6b3-4cb9-8add-7b7302ac3622',
        '2fb7ce8a-1e8c-40a9-9361-590206692ead',
        '2fb33e1e-1cfc-4fde-8ac0-03d455533530',
        '2fac9c3b-81c8-4d4e-80f5-7f8707960ffd',
        '2f1a05c8-42b9-4c32-a476-4b98ad011383',
        '2ef3d410-0e90-4f27-b62d-13162738fd95',
        '01f589f9-5fc3-4069-9421-ea6808476fd8',
        'fc396306-ba13-47eb-bbf2-1b09757164df',
        '01eb5b3e-56a4-46fa-bcbf-515275ad4203',
        ]


if __name__ == '__main__':
    image_zip()
