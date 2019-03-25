"""Utilities for connecting to Google sheets."""

import os
from os.path import join, exists
import argparse
import httplib2
from apiclient import discovery         # pylint: disable=import-error
from oauth2client import client         # pylint: disable=import-error
from oauth2client import tools          # pylint: disable=import-error
from oauth2client.file import Storage   # pylint: disable=import-error


def get_credentials():
    """Validate user's credentials from cache or prompt for new credentials."""
    home_dir = os.path.expanduser('~')
    credential_dir = join(home_dir, '.credentials')
    if not exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = join(credential_dir, 'drive-nitfix.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(
            'data/secrets/client_drive_secrets.json',
            'https://www.googleapis.com/auth/drive')
        flow.user_agent = 'Google Drive API Python'
        flags = argparse.ArgumentParser(
            parents=[tools.argparser]).parse_args()
        credentials = tools.run_flow(flow, store, flags)
        print('Storing credentials to ' + credential_path)
    return credentials


def sheet_download(sheet_name, csv_path, mime_type):
    """Export the Google Sheet."""
    http = get_credentials().authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)

    files = service.files().list(
        q='name="{}" and mimeType="{}"'.format(
            sheet_name, 'application/vnd.google-apps.spreadsheet'),
        orderBy='modifiedTime desc,name').execute().get('files', [])

    data = service.files().export(
        fileId=files[0]['id'], mimeType=mime_type).execute()

    if not data:
        raise FileNotFoundError(f'Could not read Google sheet {sheet_name}')

    with open(csv_path, 'wb') as csv_file:
        csv_file.write(data)


def sheet_to_csv(sheet_name, csv_path):
    """Export the Google Sheet."""
    sheet_download(sheet_name, csv_path, 'text/csv')


def sheet_to_excel(sheet_name, csv_path):
    """Export the Google Sheet."""
    sheet_download(
        sheet_name,
        csv_path,
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
