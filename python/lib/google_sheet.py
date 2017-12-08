"""Utilites for connecting to Google sheets."""

import os
import argparse
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

MASTER_TAXONOMY = '14J1_gHf4g4BHfG-qVJTx3Z296xyXPIXWNAGRx0uReWk'

FLAGS = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = 'data/client_id.json'
APPLICATION_NAME = 'Google Sheets API Python'


def get_credentials():
    """Validate user credentials from storage."""
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


def get_service():
    """Get inforation so that we can access the Google spreadsheet."""
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    url = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
    service = discovery.build(
        'sheets', 'v4', http=http, discoveryServiceUrl=url)
    return service
