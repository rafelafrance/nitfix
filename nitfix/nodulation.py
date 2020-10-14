#!/usr/bin/env python3

"""Fill in the nodulation Excel file."""

import pandas as pd

import lib.db as db
import lib.util as util


def sprent_sheet():
    """Fill in the full species names for the Sprent sheet."""
    sheet = util.SPRENT_SHEET
    df = pd.read_excel(util.NOD_EXCEL, sheet, header=1, usecols=[0, 1, 2])
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
    df = pd.read_csv(util.SPRENT_COUNTS_CSV)
    save(df, util.SPRENT_COUNTS_SHEET)


def save(df, sheet):
    """Convert the dataframe/Excel sheet into a CSV file."""
    name = util.NOD_CSV + sheet + '.csv'
    df.to_csv(name, index=False)

    table = 'nodulation_' + sheet
    df.to_sql(table, db.connect(), if_exists='replace', index=False)


if __name__ == '__main__':
    # sprent_sheet()
    sprent_counts_sheet()
