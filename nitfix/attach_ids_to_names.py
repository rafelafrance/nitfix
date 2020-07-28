"""SImages were renamed to species, I need to get the UUID back."""

import pandas as pd

import lib.db as db
from lib.util import RAW_DATA, TEMP_DATA


# IN_CSV = RAW_DATA / 'proportion_ontarget_renamed_renamed.csv'
# OUT_CSV = TEMP_DATA / 'proportion_ontarget_renamed_renamed_renamed.csv'
IN_CSV = RAW_DATA / 'Greenness_data_update.csv'
OUT_CSV = TEMP_DATA / 'Greenness_data_update_2020-07-25.csv'


def main_old():
    """Do it."""
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


def main():
    """Do it."""
    df = pd.read_csv(IN_CSV)
    sql = """ select sci_name, family from taxonomy;"""

    with db.connect() as cxn:
        map_df = pd.read_sql(sql, cxn)

    name_map = map_df.set_index('sci_name')['family'].to_dict()

    df['family'] = df['sci_name'].map(name_map)

    df.to_csv(OUT_CSV, index=False)


if __name__ == '__main__':
    main()
