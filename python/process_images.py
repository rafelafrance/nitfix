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

    parser.add_argument('-c', '--create-table', action='store_true',
                        help="""Drop and recreate the images table in the
                            SQL database.""")

    args = parser.parse_args()
    return args


def get_image_data(file_name):
    """Get the image data."""
    with open(file_name, 'rb') as image_file:
        exif = exifread.process_file(image_file)
        image = Image.open(image_file)
        image.load()

    qr_code = zbarlight.scan_codes('qrcode', image)
    qr_code = qr_code[0].decode('utf-8') if qr_code else None

    return str(exif['Image DateTime']), qr_code


def create_tables(db_conn):
    """Reset the images table."""
    db_conn.execute('DROP TABLE IF EXISTS images')
    db_conn.execute("""
                    CREATE TABLE images (
                      id            TEXT PRIMARY KEY,
                      file_name     TEXT UNIQUE,
                      image_created TEXT)
                     """)
    db_conn.execute('DROP TABLE IF EXISTS errors')
    db_conn.execute("""CREATE TABLE errors (msg TEXT)""")


def main():
    """Run the program loop."""
    uuids = {}

    args = parse_command_line()

    db_name = join('data', 'nitfix.sqlite.db')
    insert = """
        INSERT INTO images (id, file_name, image_created) VALUES (?, ?, ?)
        """
    error = """INSERT INTO errors (msg) VALUES (?)"""

    with sqlite3.connect(db_name) as db_conn:
        if args.create_table:
            create_tables(db_conn)

        for pattern in args.files:
            for file_name in sorted(glob(pattern)):
                image_created, uuid = get_image_data(file_name)
                print(file_name, uuid)

                if not uuid:
                    msg = 'MISSING: QR code missing in {}'.format(file_name)
                    print(msg)
                    db_conn.execute(error, (msg, ))
                elif uuids.get(uuid):
                    msg = ('DUPLICATES: Files {} and {} have the same '
                           'QR code').format(uuids[uuid], file_name)
                    print(msg)
                    db_conn.execute(error, (msg, ))
                else:
                    uuids[uuid] = file_name
                    db_conn.execute(insert, (uuid, file_name, image_created))
                db_conn.commit()


if __name__ == '__main__':
    main()
