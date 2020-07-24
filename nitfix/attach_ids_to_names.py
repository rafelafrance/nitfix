"""SImages were renamed to species, I need to get the UUID back."""

import pandas as pd

import lib.db as db
from lib.util import RAW_DATA, TEMP_DATA


IN_CSV = RAW_DATA / 'proportion_ontarget_renamed_renamed.csv'
OUT_CSV = TEMP_DATA / 'proportion_ontarget_renamed_renamed_renamed.csv'


def main():
    """Do it."""
    in_df = pd.read_csv(IN_CSV)

    sql = """
        with singles as (select sci_name from taxonomy_ids
                       group by sci_name having count(*) = 1)
        select * from taxonomy_ids where sci_name in singles;
        """

    with db.connect() as cxn:
        id_df = pd.read_sql(sql, cxn)
    name_map = id_df.set_index('sci_name')['sample_id'].to_dict()

    in_df['sci_name'] = in_df['file'].str.split('_').str[:2].str.join(' ')
    in_df['sample_id'] = in_df['sci_name'].map(name_map)

    in_df.to_csv(OUT_CSV, index=False)


if __name__ == '__main__':
    main()
