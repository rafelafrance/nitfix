"""Compare the UUIDs in the Google sheet with what is in the database."""

import lib.db as db


def main():
    """Show basic usage of the Sheets API."""
    with db.connect() as db_conn:
        for taxonomy in db.get_taxonomies(db_conn):
            images = db.get_image(db_conn, taxonomy['tissue_sample_id'])
            if len(images) < 1:
                print('Taxonomy without an image: {} {}'.format(
                    taxonomy['tissue_sample_id'], taxonomy['scientific_name']))
            if len(images) > 1:
                print('Taxonomy with too many images: {} {}'.format(
                    taxonomy['tissue_sample_id'], taxonomy['scientific_name']))

        for image in db.get_images(db_conn):
            taxonomies = db.get_taxonomy_by_image_id(
                db_conn, image['image_id'])
            if len(taxonomies) < 1:
                print('Image with out a taxonomy: {} {}'.format(
                    image['image_id'], image['file_name']))
            if len(taxonomies) > 1:
                print('Image with too many taxonomies: {} {}'.format(
                    image['image_id'], image['file_name']))


if __name__ == '__main__':
    main()
