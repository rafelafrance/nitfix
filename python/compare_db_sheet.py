"""Compare the UUIDs in the google sheet with what is in the database."""

# pylint: disable=no-member

from os.path import join
import csv
import uuid
import lib.db as db
import lib.google_sheet as google_sheet


def get_sheet_guids():
    """Get UUIDs from the google sheet."""
    service = google_sheet.get_service()
    range_name = 'NitFixList!A2:I'
    result = service.spreadsheets().values().get(
        spreadsheetId=google_sheet.MASTER_TAXONOMY, range=range_name).execute()
    values = result.get('values', [])

    guids = {}
    for row in values:
        if len(row) > 5:
            for guid in row[5].split(';'):
                guid = guid.strip()
                try:
                    uuid.UUID(guid)
                    guids[guid] = row
                except ValueError:
                    pass

    return guids


def get_db_guids():
    """Get UUIDs from the database."""
    guids = {}
    select = 'SELECT * FROM images'
    with db.connect() as db_conn:
        cursor = db_conn.cursor()
        cursor.execute(select)
        guids = {g[0]: g for g in cursor.fetchall()}

    return guids


def main():
    """Show basic usage of the Sheets API."""
    db_guids = get_db_guids()
    sheet_guids = get_sheet_guids()

    db_guids_set = set(db_guids.keys())
    sheet_guids_set = set(sheet_guids.keys())

    db_minus_sheet = db_guids_set - sheet_guids_set
    sheet_minus_db = sheet_guids_set - db_guids_set

    print('DB UUIDs', len(db_guids))
    print('Sheet UUIDs', len(sheet_guids))
    print('DB UUIDs not in sheet', len(db_minus_sheet))
    print('Sheet UUIDs not in DB', len(sheet_minus_db))
    print()

    path = join('data', 'uuids_in_db_missing_from_sheet.csv')
    with open(path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['uuid', 'file_name'])
        for guid in db_minus_sheet:
            writer.writerow([guid, db_guids[guid][1]])
    # for guid in db_minus_sheet:
    #     print(guid, db_guids[guid])
    # print()

    path = join('data', 'uuids_in_sheet_missing_from_db.csv')
    with open(path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['uuid', 'scientificName'])
        for guid in sheet_minus_db:
            writer.writerow([guid, sheet_guids[guid][2]])
    # for guid in sheet_minus_db:
    #     print(guid, sheet_guids[guid])
    # print()


if __name__ == '__main__':
    main()
