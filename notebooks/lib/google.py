"""Utilities for connecting to Google sheets."""

import os
from os.path import join, exists
import argparse
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage


SCOPES = 'https://www.googleapis.com/auth/drive'
CREDENTIALS = 'drive-nitfix.json'
APPLICATION = 'Google Drive API Python'
SECRETS = 'data/secrets/client_drive_secrets.json'
VERSION = 'v3'


def get_credentials():
    """Validate user's credentials from cache or prompt for new credentials."""
    home_dir = os.path.expanduser('~')
    credential_dir = join(home_dir, '.credentials')
    if not exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = join(credential_dir, CREDENTIALS)

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(SECRETS, SCOPES)
        flow.user_agent = APPLICATION
        flags = argparse.ArgumentParser(
            parents=[tools.argparser]).parse_args()
        credentials = tools.run_flow(flow, store, flags)
        print('Storing credentials to ' + credential_path)
    return credentials


def get_service(google_app):
    """Get the Google Drive API."""
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    return discovery.build(google_app, VERSION, http=http)


def export_sheet_csv(sheet_name, csv_file):
    """Export the Google Sheet."""
    service = get_service('drive')
    files = service.files().list(
        q='name="{}" and mimeType="{}"'.format(
            sheet_name, 'application/vnd.google-apps.spreadsheet'),
        orderBy='modifiedTime desc,name').execute().get('files', [])

    data = service.files().export(
        fileId=files[0]['id'], mimeType='text/csv').execute()

    if not data:
        raise FileNotFoundError(
            'Could not read Google sheet {}'.format(sheet_name))

    csv_file.write(data)
    csv_file.flush()
    csv_file.seek(0)
