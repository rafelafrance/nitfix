"""Get master taxonomy info from Google sheet and put it into the database."""

# pylint: disable=no-member

# from os.path import join
# import sqlite3
import lib.google_sheet as google_sheet


def get_sheet_data():
    """Get UUIDs from the google sheet."""
    service, spreadsheet_id = google_sheet.get_info()
    range_name = 'NitFixList!A2:I'
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    guids = {}
    for row in values:
        if len(row) > 5:
            for guid in row[5].split(';'):
                guid = guid.strip()
                try:
                    guids[guid] = row
                except ValueError:
                    pass

    return guids
