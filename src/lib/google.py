"""Utilites for connecting to Google sheets."""

import os
import argparse
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage


# If modifying these scopes, delete your previously saved credentials
GOOGLE_APP = {
    'sheets': {
        'scopes': 'https://www.googleapis.com/auth/spreadsheets',
        'discoveryServiceUrl':
            'https://sheets.googleapis.com/$discovery/rest?version=v4',
        'credentials': 'sheets-nitfix.json',
        'application_name': 'Google Sheets API Python',
        'secrets': 'data/secrets/client_sheets_secrets.json',
        'version': 'v4',
    },
    'drive': {
        'scopes': 'https://www.googleapis.com/auth/drive',
        'discoveryServiceUrl': None,
        'credentials': 'drive-nitfix.json',
        'application_name': 'Google Drive API Python',
        'secrets': 'data/secrets/client_drive_secrets.json',
        'version': 'v3',
    }
}

SHEETS = {
    'master_taxonomy': '14J1_gHf4g4BHfG-qVJTx3Z296xyXPIXWNAGRx0uReWk',
    'sample_plates': '1uPOtAuu3VQUcVkRsY4q2GtfeFrs1l9udkyNEaxvqjmA',
    # 'picogreen': '1smqCaVVWp35nPl1IrS7OXpmsgr-abtt3F2VC8dQUFmc',
    'picogreen': '1uXI4-m8xmy50ljxghGnN3zEjF9K2bqXkghin9mWf_0o',
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
        flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
        credentials = tools.run_flow(flow, store, flags)
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
        q='name="{}" and mimeType="{}"'.format(
            sheet_name, 'application/vnd.google-apps.spreadsheet'),
        orderBy='modifiedTime desc,name').execute().get('files', [])

    data = service.files().export(
        fileId=files[0]['id'], mimeType='text/csv').execute()

    if not data:
        raise FileNotFoundError(  # noqa
            'Could not read Google sheet {}'.format(sheet_name))

    csv_file.write(data)
    csv_file.flush()
    csv_file.seek(0)


def update_sheet(
        sheet_name,
        range_,
        values,
        major_dimension='ROWS',
        value_input_option='RAW'):
    """Update the given sheet with the given data."""
    service = get_service('sheets')
    request_body = {
        'value_input_option': value_input_option,
        'data': {
            'major_dimension': major_dimension,
            'range': range_,
            'values': values,
        }
    }
    request = service.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEETS[sheet_name], body=request_body)
    response = request.execute()
    # from pprint import pprint
    # pprint(response)
    return response
