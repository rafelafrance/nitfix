"""Compare the UUIDs in the Google sheet with what is in the database."""

import lib.db as db


def main():
    """Show basic usage of the Sheets API."""
    with db.connect() as db_conn:
        for taxon in db.get_taxons(db_conn):
            images = db.get_image(db_conn, taxon['sample_id'])
            if len(images) < 1:
                print('Taxon without an image: {} {}'.format(
                    taxon['sample_id'], taxon['scientific_name']))
            if len(images) > 1:
                print('Taxon with too many images: {} {}'.format(
                    taxon['sample_id'], taxon['scientific_name']))

        for image in db.get_images(db_conn):
            taxons = db.get_taxon_by_sample_id(
                db_conn, image['sample_id'])
            if len(taxons) < 1:
                print('Image with out a taxon: {} {}'.format(
                    image['sample_id'], image['file_name']))
            if len(taxons) > 1:
                print('Image with too many taxons: {} {}'.format(
                    image['sample_id'], image['file_name']))


if __name__ == '__main__':
    main()
