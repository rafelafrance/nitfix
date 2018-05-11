"""
Create a list of samples to keep or toss given the criteria below.

1. Toss everything with a value < 1 (not <=).
   This means < 1 ng/uL or < 100 ng total DNA.

2. Priority rules.
    2a. Must have one representative of each genus.
    2b. If we have <= 5 species TOTAL in a genus, submit everything we have.
    2c. If we have > 5 but <= 12 species TOTAL a the genus, submit 50% of them.
    2d. If we have > 12 species in a genus, submit 25% of what we have.
    ** Bias the selection to favor higher DNA yield?
"""

import math
from pathlib import Path
from datetime import datetime
import pandas as pd
import lib.db as db


def main():
    """Generate the report."""
    sql = """
        SELECT family, genus, scientific_name, ng_microliter_mean,
               wells.sample_id, plate_id, local_no, well_no, wells.well,
               picogreen_id
          FROM taxonomy
     LEFT JOIN taxon_ids USING (scientific_name)
     LEFT JOIN wells     ON    (wells.sample_id = taxon_ids.id)
     LEFT JOIN picogreen USING (picogreen_id)
    """
    taxons = pd.read_sql(sql, db.connect())
    taxons['keep'] = False

    has_family = taxons.family.notna()
    has_genus = taxons.genus.notna()
    taxons = taxons.loc[has_family & has_genus, :]

    taxons = taxons.sort_values(
        ['family', 'genus', 'ng_microliter_mean', 'scientific_name'],
        ascending=[True, True, False, True])
    taxons.keep = taxons.groupby(['family', 'genus']).keep.transform(toss)

    report_name = f'toss_list_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
    report_path = Path('data') / 'interim' / report_name
    with pd.ExcelWriter(report_path) as writer:
        taxons.to_excel(writer, sheet_name='By Taxonomy', index=False)


def toss(group):
    """Handle a group of data."""
    if group.shape[0] <= 5:
        group = True
    elif group.shape[0] <= 12:
        top = math.ceil(0.5 * group.shape[0])
        group.iloc[:top] = True
    else:
        top = math.ceil(0.25 * group.shape[0])
        group.iloc[:top] = True
    return group


if __name__ == '__main__':
    main()
