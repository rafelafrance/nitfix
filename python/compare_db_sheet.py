"""Compare the UUIDs in the google sheet with what is in the database."""

# pylint: disable=no-member

import os
from os.path import join
import csv
import uuid
import sqlite3
import argparse
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

FLAGS = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = 'data/client_id.json'
APPLICATION_NAME = 'Google Sheets API Python'


def get_credentials():
    """Validate user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.

    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(
        credential_dir, 'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        credentials = tools.run_flow(flow, store, FLAGS)
        print('Storing credentials to ' + credential_path)
    return credentials


def get_sheet_guids():
    """Get UUIDs from the google sheet."""
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    url = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
    service = discovery.build(
        'sheets', 'v4', http=http, discoveryServiceUrl=url)
    spreadsheet_id = '14J1_gHf4g4BHfG-qVJTx3Z296xyXPIXWNAGRx0uReWk'
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
                    uuid.UUID(guid)
                    guids[guid] = row
                except ValueError:
                    pass

    return guids


def get_db_guids():
    """Get UUIDs from the database."""
    guids = {}
    db_path = join('data', 'nitfix.sqlite.db')
    select = 'SELECT * FROM images'
    with sqlite3.connect(db_path) as db_conn:
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
    # print()

    path = join('data', 'uuids_in_db_not_in_sheet.csv')
    with open(path, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['uuid', 'file_name'])
        for guid in db_minus_sheet:
            writer.writerow([guid, db_guids[guid][1]])
    # for guid in db_minus_sheet:
    #     print(guid, db_guids[guid])
    # print()

    path = join('data', 'uuids_in_sheet_not_in_db.csv')
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
