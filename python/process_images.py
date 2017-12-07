"""Extract QR codes and image exif data from pictures of museum specimens."""

from os.path import join
from glob import glob
import argparse
import sqlite3
from collections import namedtuple
from PIL import Image, ImageFilter
import exifread
import zbarlight


Dimensions = namedtuple('Dimensions', 'width height')


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

    parser.add_argument('-d', '--dry-run', action='store_true',
                        help="""Don't output to the database.""")

    parser.add_argument('--set-uuid', metavar='UUID',
                        help="""Set the UUID to this value.""")

    args = parser.parse_args()
    return args


def window_slider(image_size, window=None, stride=None):
    """Slide a window over the image to help with feature extraction."""
    window = window if window else Dimensions(400, 400)
    stride = stride if stride else Dimensions(200, 200)

    for top in range(0, image_size.height, stride.height):
        bottom = top + window.height
        bottom = image_size.height if bottom > image_size.height else bottom

        for left in range(0, image_size.width, stride.width):
            right = left + window.width
            right = image_size.width if right > image_size.width else right

            box = (left, top, right, bottom)

            yield box


def get_qr_code(image, args):
    """Get the QR code from the image."""
    # If the UUID argument is set return that
    if args.set_uuid:
        return args.set_uuid

    # Try a direct extraction
    qr_code = zbarlight.scan_codes('qrcode', image)
    if qr_code:
        return qr_code[0].decode('utf-8')

    # Try a slider
    for box in window_slider(image):
        cropped = image.crop(box)
        qr_code = zbarlight.scan_codes('qrcode', cropped)
        if qr_code:
            return qr_code[0].decode('utf-8')

    # Try rotating the image *sigh*
    for degrees in range(5, 85, 5):
        rotated = image.rotate(degrees)
        qr_code = zbarlight.scan_codes('qrcode', rotated)
        if qr_code:
            return qr_code[0].decode('utf-8')

    # Try to sharpen the image
    sharpened = image.filter(ImageFilter.SHARPEN)
    qr_code = zbarlight.scan_codes('qrcode', sharpened)
    if qr_code:
        return qr_code[0].decode('utf-8')

    return None


def get_image_data(file_name, args):
    """Get the image data."""
    with open(file_name, 'rb') as image_file:
        exif = exifread.process_file(image_file)
        image = Image.open(image_file)
        image.load()

    qr_code = get_qr_code(image, args)

    return str(exif['Image DateTime']), qr_code


def create_tables(args, db_conn):
    """Reset the images table."""
    if not args.create_table or args.dry_run:
        return

    db_conn.execute('DROP TABLE IF EXISTS images')
    db_conn.execute("""
                    CREATE TABLE images (
                      id            TEXT PRIMARY KEY NOT NULL,
                      file_name     TEXT NOT NULL UNIQUE,
                      image_created TEXT)
                     """)
    db_conn.execute('DROP TABLE IF EXISTS errors')
    db_conn.execute("""CREATE TABLE errors (msg TEXT)""")


def insert_error(args, db_conn, msg):
    """Insert into the errors table."""
    insert = """INSERT INTO errors (msg) VALUES (?)"""

    print(msg)

    if not args.dry_run:
        db_conn.execute(insert, (msg, ))
        db_conn.commit()


def insert_image(args, db_conn, uuid, file_name, image_created):
    """Insert into the images table."""
    insert = """
        INSERT INTO images (id, file_name, image_created) VALUES (?, ?, ?)
        """

    print(file_name, uuid, image_created)

    if not args.dry_run:
        db_conn.execute(insert, (uuid, file_name, image_created))
        db_conn.commit()


def main():
    """Run the program loop."""
    uuids = {}

    args = parse_command_line()
    db_name = join('data', 'nitfix.sqlite.db')

    with sqlite3.connect(db_name) as db_conn:
        create_tables(args, db_conn)

        for pattern in args.files:
            for file_name in sorted(glob(pattern)):
                image_created, uuid = get_image_data(file_name, args)

                if not uuid:
                    msg = 'MISSING: QR code missing in {}'.format(file_name)
                    insert_error(args, db_conn, msg)
                elif uuids.get(uuid):
                    msg = ('DUPLICATES: Files {} and {} have the same '
                           'QR code').format(uuids[uuid], file_name)
                    insert_error(args, db_conn, msg)
                else:
                    uuids[uuid] = file_name
                    insert_image(args, db_conn, uuid, file_name, image_created)


if __name__ == '__main__':
    main()
