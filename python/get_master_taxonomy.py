"""Get master taxonomy info from Google sheet and put it into the database."""

# pylint: disable=no-member

import lib.db as db
import lib.google_sheet as google_sheet


def get_sheet_data():
    """Get UUIDs from the google sheet."""
    service = google_sheet.get_service('sheets')
    range_name = 'NitFixList!A2:I'
    result = service.spreadsheets().values().get(
        spreadsheetId=google_sheet.MASTER_TAXONOMY, range=range_name).execute()
    values = result.get('values', [])

    with db.connect() as db_conn:
        db.create_taxonomies_table(db_conn)
        for row in values:
            print(row[2])
            insert_row(db_conn, row)


def insert_row(db_conn, row):
    """Build a row for inserting into the taxonomy table."""
    i = len(row)
    key = row[0]
    family = row[1] if i > 1 else ''
    scientific_name = row[2] if i > 2 else ''
    taxonomic_authority = row[3] if i > 3 else ''
    synonyms = row[4] if i > 4 else ''
    tissue_sample_id = row[5] if i > 5 else ''
    provider_acronym = row[6] if i > 6 else ''
    provider_id = row[7] if i > 7 else ''
    quality_notes = row[8] if i > 8 else ''
    record = (key, family, scientific_name, taxonomic_authority, synonyms,
              tissue_sample_id, provider_acronym, provider_id, quality_notes)
    db.insert_taxonomy(db_conn, record)


def import_master_taxonomy():
    """Import sample plate data from the Google sheet."""
    with open('data/master_taxonomy.csv', 'wb') as temp_csv:
        google_sheet.export_sheet_csv('NitFixMasterTaxonomy', temp_csv)
        temp_csv.close()

        # with open(temp_csv.name) as csv_file:
        #     reader = csv.reader(csv_file)
        #     print(reader)
        #     for row in reader:
        #         print(row)


if __name__ == '__main__':
    # get_sheet_data()
    import_master_taxonomy()
