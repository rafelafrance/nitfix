"""Compare the UUIDs in the Google sheet with what is in the database."""

import lib.db as db


def main():
    """Show basic usage of the Sheets API."""
    with db.connect() as cxn:
        for taxon in db.get_taxons(cxn):
            images = db.get_image(cxn, taxon['sample_id'])
            if len(images) < 1:
                print('Taxon without an image: {} {}'.format(
                    taxon['sample_id'], taxon['scientific_name']))
            if len(images) > 1:
                print('Taxon with too many images: {} {}'.format(
                    taxon['sample_id'], taxon['scientific_name']))

        for image in db.get_images(cxn):
            taxons = db.get_taxon_by_sample_id(
                cxn, image['sample_id'])
            if len(taxons) < 1:
                print('Image with out a taxon: {} {}'.format(
                    image['sample_id'], image['file_name']))
            if len(taxons) > 1:
                print('Image with too many taxons: {} {}'.format(
                    image['sample_id'], image['file_name']))


if __name__ == '__main__':
    main()
