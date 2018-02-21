"""Utilites for connecting to Google sheets."""

import os
import argparse
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

SRC_MIME_TYPE = 'application/vnd.google-apps.spreadsheet'
DST_MIME_TYPE = 'text/csv'

MASTER_TAXONOMY = '14J1_gHf4g4BHfG-qVJTx3Z296xyXPIXWNAGRx0uReWk'
SAMPLE_PLATES = '1uPOtAuu3VQUcVkRsY4q2GtfeFrs1l9udkyNEaxvqjmA'

FLAGS = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()

# If modifying these scopes, delete your previously saved credentials
GOOGLE_APP = {
    'sheets': {
        'scopes': 'https://www.googleapis.com/auth/spreadsheets.readonly',
        'discoveryServiceUrl':
            'https://sheets.googleapis.com/$discovery/rest?version=v4',
        'credentials': 'sheets-nitfix-readonly.json',
        'application_name': 'Google Sheets API Python',
        'secrets': 'data/client_sheets_secrets.json',
        'version': 'v4',
    },
    'drive': {
        'scopes': 'https://www.googleapis.com/auth/drive.readonly',
        'discoveryServiceUrl': None,
        'credentials': 'drive-nitfix-readonly.json',
        'application_name': 'Google Drive API Python',
        'secrets': 'data/client_drive_secret.json',
        'version': 'v3',
    }
}


def get_credentials(app):
    """Validate user credentials from storage."""
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, app['credentials'])

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(app['secrets'], app['scopes'])
        flow.user_agent = app['application_name']
        credentials = tools.run_flow(flow, store, FLAGS)
        print('Storing credentials to ' + credential_path)
    return credentials


def get_service(google_app):
    """Get the Google service."""
    app = GOOGLE_APP[google_app]
    credentials = get_credentials(app)
    http = credentials.authorize(httplib2.Http())

    if not app['discoveryServiceUrl']:
        return discovery.build(google_app, app['version'], http=http)

    return discovery.build(
        google_app,
        app['version'],
        http=http,
        discoveryServiceUrl=app['discoveryServiceUrl'])


def export_sheet_csv(sheet_name, csv_file):
    """Get info from the Google sheet."""
    service = get_service('drive')
    files = service.files().list(
        q='name="{}" and mimeType="{}"'.format(sheet_name, SRC_MIME_TYPE),
        orderBy='modifiedTime desc,name').execute().get('files', [])

    data = service.files().export(
        fileId=files[0]['id'], mimeType=DST_MIME_TYPE).execute()

    if not data:
        raise FileNotFoundError(  # noqa
            'Could not read Google sheet {}'.format(sheet_name))

    csv_file.write(data)
    csv_file.flush()
    csv_file.seek(0)
