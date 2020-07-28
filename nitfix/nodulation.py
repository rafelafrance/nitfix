#!/usr/bin/env python3

"""Fill in the nodulation Excel file."""

import re

import pandas as pd

from lib.util import RAW_DATA


NOD_DIR = RAW_DATA / 'nodulation'
NOD_EXCEL = NOD_DIR / 'Nitfix_Nodulation_Data_Sheets.v2.xlsx'

SPRENT_2009 = NOD_DIR / 'Sprent_2009_chrome.txt'


def sprent_sheet():
    """Fill in the full species names for the Sprent sheet."""
    df = pd.read_excel(NOD_EXCEL, 'Sprent', header=1, usecols=[0, 1, 2])
    df = df.fillna('')
    genus = ''
    for _, row in df.iterrows():
        words = row['Genus_species'].split()
        if len(words) == 1:
            genus = words[0]
        else:
            sci_name = f'{genus} {words[1]}'
            row['Genus_species'] = sci_name

    genus_only = df['Genus_species'].str.count(' ') == 1
    # print(genus_only[100:120])
    df = df[genus_only]

    print(df[100:120])


def sprent_2009_ingest():
    """See if we can cleanly import the Sprent data."""
    with open(SPRENT_2009) as in_file:
        text = in_file.read()

    text = 1


if __name__ == '__main__':
    sprent_2009_ingest()
    # sprent_sheet()
