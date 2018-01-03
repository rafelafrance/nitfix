"""Compare the UUIDs in the Google sheet with what is in the database."""

import lib.db as db


def main():
    """Show basic usage of the Sheets API."""
    with db.connect() as db_conn:
        for row in db.get_taxonomy_image_mismatches(db_conn):
            if not row['image_id']:
                print('Taxonomy without an image: {} {}'.format(
                    row['tissue_sample_id'], row['scientific_name']))
            else:
                print('Image with out a taxonomy: {} {}'.format(
                    row['image_id'], row['file_name']))


if __name__ == '__main__':
    main()
