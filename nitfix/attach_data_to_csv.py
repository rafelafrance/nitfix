"""Attach information from the database into the CSV file."""

import re
import datetime

import pandas as pd

import lib.db as db
from lib.util import RAW_DATA, TEMP_DATA


# IN_CSV = RAW_DATA / 'proportion_ontarget_renamed_renamed.csv'
# OUT_CSV = TEMP_DATA / 'proportion_ontarget_renamed_renamed_renamed.csv'
# IN_CSV = RAW_DATA / 'Greenness_data_update.csv'
IN_CSV = RAW_DATA / 'Greenness_data_update_4rafe.csv'
OUT_CSV = TEMP_DATA / 'Greenness_data_update_2020-07-29b.csv'


def attach_sample_ids():
    """Get the sample ID from the species if we can."""
    df = pd.read_csv(IN_CSV)

    sql = """
        with singles as (select sci_name from taxonomy_ids
                       group by sci_name having count(*) = 1)
        select * from taxonomy_ids where sci_name in singles;
        """

    with db.connect() as cxn:
        map_df = pd.read_sql(sql, cxn)

    name_map = map_df.set_index('sci_name')['sample_id'].to_dict()

    df['sci_name'] = df['file'].str.split('_').str[:2].str.join(' ')

    df['sample_id'] = df['sci_name'].map(name_map)

    df.to_csv(OUT_CSV, index=False)


def attach_families():
    """Get the family from the sci_name."""
    df = pd.read_csv(IN_CSV)
    sql = """ select sci_name, family from taxonomy;"""

    with db.connect() as cxn:
        map_df = pd.read_sql(sql, cxn)

    name_map = map_df.set_index('sci_name')['family'].to_dict()

    df['family'] = df['sci_name'].map(name_map)

    df.to_csv(OUT_CSV, index=False)


def attach_nfn_data():
    """Attach NfN data from to the records."""
    df = pd.read_csv(IN_CSV, index_col='sample_id').fillna('NA')
    df['state_province'] = 'NA'
    df['county'] = 'NA'
    df['family'] = 'NA'
    df['herbarium'] = 'NA'

    now = datetime.datetime.now()

    with db.connect() as cxn:
        sql = 'select * from nfn_data;'
        nfn_df = pd.read_sql(sql, cxn, index_col='sample_id')

        sql = 'select sci_name, family from taxonomy;'
        fam_map = pd.read_sql(sql, cxn, index_col='sci_name').astype(str)
        fam_map = fam_map['family'].to_dict()

        sql = 'select * from images;'
        img_map = pd.read_sql(sql, cxn, index_col='sample_id').astype(str)
        img_map = img_map['image_file'].to_dict()

    cols = """
        country state_province county
        month_1 day_1 year_1
        collected_by
        """.split()

    for sample_id, row in df.iterrows():
        if sample_id in nfn_df.index:
            for col in cols:
                if row[col] == 'NA':
                    df.at[sample_id, col] = nfn_df.at[sample_id, col]
            year_1 = df.at[sample_id, 'year_1']
            if not isinstance(year_1, str):
                year_1 = year_1[0]
            else:
                year_1 = year_1.split('|')[0]
            if row['yr.bp'] == 'NA' and year_1 not in ('NA', 'Not Shown', ''):
                df.at[sample_id, 'yr.bp'] = now.year - int(year_1)


    df['family'] = df['sci_name'].map(fam_map)
    df['herbarium'] = df.index.map(img_map)
    df['herbarium'] = df['herbarium'].apply(herbarium)

    df.to_csv(OUT_CSV)


def herbarium(image_file):
    """Map the image file name to a herbarium."""
    image_file = str(image_file)
    if image_file == 'nan':
        return ''
    image_file = image_file.upper()
    parts = re.split(r'[_-]', image_file)
    return parts[1] if parts[0] == 'TINGSHUANG' else parts[0]


if __name__ == '__main__':
    attach_nfn_data()
