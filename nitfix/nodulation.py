#!/usr/bin/env python3

"""Fill in the nodulation Excel file."""

import pandas as pd
import camelot

import lib.db as db
from lib.util import RAW_DATA


NOD_DIR = RAW_DATA / 'nodulation'
NOD_EXCEL = NOD_DIR / 'Nitfix_Nodulation_Data_Sheets.v2.xlsx'

NOD_CSV = str(NOD_DIR / 'Nitfix_Nodulation_Data_Sheets.v2.')

SPRENT89 = NOD_DIR / 'Sprent_1989_Table_1.pdf'


def sprent_sheet():
    """Fill in the full species names for the Sprent sheet."""
    sheet = 'Sprent'
    df = pd.read_excel(NOD_EXCEL, sheet, header=1, usecols=[0, 1, 2])
    df['sci_name'] = ''
    df = df.fillna('')

    genus = ''
    for _, row in df.iterrows():
        words = row['Genus_species'].split()
        if len(words) == 1:
            genus = words[0]
        elif words[0][0] == genus[0]:
            sci_name = f'{genus} {words[1]}'
            row['sci_name'] = sci_name

    save(df, sheet)


def sprent_counts_sheet():
    """Get the counts from the Sprent 1989 PDF."""
    tables = camelot.read_pdf(str(SPRENT89), pages='1-end')
    print(f'tables: {tables.n}')
    for table in tables:
        print(table.df.head())


def save(df, sheet):
    """Convert the dataframe/Excel sheet into a CSV file."""
    name = NOD_CSV + sheet + '.csv'
    df.to_csv(name, index=False)

    table = 'nodulation_' + sheet
    df.to_sql(table, db.connect(), if_exists='replace', index=False)


if __name__ == '__main__':
    # sprent_sheet()
    sprent_counts_sheet()
