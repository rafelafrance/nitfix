"""Extract QR codes and image exif data from pictures of museum specimens."""

from os.path import join
from glob import glob
import argparse
import sqlite3
from PIL import Image
import exifread
import zbarlight


def parse_command_line():
    """Get comand line arguments."""
    parser = argparse.ArgumentParser(
        description="""Extract information for the given files.""")

    parser.add_argument('-f', '--files', required=True, action='append',
                        help="""Process files matching this pattern. You may
                            use standard file glob wildcards like '*', '?',
                            or '**'. Note: you may need to quote this argument
                            like so: --files 'path/to/images/*.jpg'""")
    args = parser.parse_args()
    return args


def get_image_data(file_name):
    """Get the image data."""
    with open(file_name, 'rb') as image_file:
        exif = exifread.process_file(image_file)
        image = Image.open(image_file)
        image.load()
    qr_code = zbarlight.scan_codes('qrcode', image)
    return str(exif['Image DateTime']), qr_code[0].decode('utf-8')


def main():
    """Run the program loop."""
    uuids = {}
    duplicates = 0
    args = parse_command_line()

    db_name = join('data', 'nitfix.sqlite.db')
    sql = """
        INSERT INTO images (id, file_name, image_created) VALUES (?, ?, ?)
        """

    with sqlite3.connect(db_name) as db_conn:
        for pattern in args.files:
            for file_name in sorted(glob(pattern)):
                image_created, uuid = get_image_data(file_name)
                print(file_name, uuid)

                # Skip duplicate images
                if uuids.get(uuid):
                    duplicates += 1
                    print('DUPLICATE')
                    continue
                uuids[uuid] = 1

                db_conn.execute(sql, (uuid, file_name, image_created))
                db_conn.commit()

        print('Duplicates', duplicates)


if __name__ == '__main__':
    main()
