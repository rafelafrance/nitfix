"""Build a Notes from Nature expedition."""

# import os
import csv
from os.path import join, split
import lib.db as db


NYBG_DIR = join('data', 'nybg_expedition')


def get_absent_data(db_conn):
    """Get absent data."""
    absent = []
    no_image = []
    csv_path = join('data', 'NYBarcodeSummary.csv')
    with open(csv_path) as csv_file:
        reader = csv.reader(csv_file)
        next(reader)
        for row in reader:
            if row[2] != 'Present':
                record = get_image_info(db_conn, row, no_image)
                if record:
                    absent.append(record)
                    print(record)
    return absent, no_image


def get_image_info(db_conn, row, no_image):
    """Get the image info from the taxonomies and images tables."""
    taxonomy = db.get_taxonomy_by_provider(db_conn, 'NYBG', row[1])
    image = db.get_image(db_conn, taxonomy[5])
    if not image:
        no_image.append(('NYBG {}'.format(row[1]), taxonomy[2]))
        return None
    # _, file_name = split(image[1])
    return (image[1], 'NYBG {}'.format(row[1]), image[0], taxonomy[2])


def copy_images(absent):
    """Copy images to the expedition repository."""
    print(absent)


def create_manifest(absent):
    """Create expedition manifest."""
    print(absent)


def main():
    """Create expedition."""
    with db.connect() as db_conn:
        absent, no_image = get_absent_data(db_conn)
    print(len(absent))
    for problem in no_image:
        print(problem)
    print(len(no_image))

    # os.makedirs(NYBG_DIR, exist_ok=True)
    # copy_images(absent)
    # create_manifest(absent)


if __name__ == '__main__':
    main()
