"""
Create a list of samples to select given the criteria below.

1. Toss everything with a value < 100 (not <=). This means < 100 ng total DNA.

2. Priority rules.
    2a. If we have <= 5 species TOTAL in a genus, submit everything we have.
    2b. If we have > 5 but <= 12 species TOTAL of genus, submit 50% of them.
    2c. If we have > 12 species in a genus, submit 25% of what we have.

    ** NOTE: These cutoff rules are not consistent. A genus with 12 species
       will have 6 slots but a genus with 13 species will have 4 (rounding up).

3. Also have to keep samples that have already been submitted for sequencing.
   So the sort order is submitted then yield grouped by genus.
"""

import os
import math
from pathlib import Path
from datetime import datetime
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import lib.db as db


COLUMNS = """family genus scientific_name category rapid_total_dna
       sample_id local_no plate_id well_no well""".split()


def main():
    """Generate the report."""
    pd.options.mode.chained_assignment = None
    now = datetime.now()

    genera = get_raw_data()

    species = []
    for genus, samples in genera:
        species += categorize_species(genus, samples)

    template_dir = os.fspath(Path('src') / 'reports')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('sample_selection.html')

    report = template.render(now=now, species=species)

    report_name = f'sample_selection_report_{now.strftime("%Y-%m-%d")}.html'
    report_path = Path('output') / report_name
    with report_path.open('w') as out_file:
        out_file.write(report)


def categorize_species(genus, species):
    """Put species rows into their categories."""
    slots = get_slots(species)

    species = species.to_dict(orient='records')

    header = {k: '' for k, v in species[0].items()}
    header['family'] = genus[0]
    header['genus'] = genus[1]
    header['category'] = '0_header'

    rows = [header]

    for row in species:
        set_row_category(row, rows, slots)
        rows.append(row)

    count_row_categories(rows, slots)

    return sorted(rows, key=lambda r: (r['category'], r['scientific_name']))


def set_row_category(row, rows, slots):
    """Set row category."""
    if row['rapid_total_dna'] < 100.0 and row['source_plate']:
        row['category'] = '6_reject'
    elif row['rapid_total_dna'] < 100.0 and not row['source_plate']:
        row['category'] = '5_unprocessed'
    elif False:  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! TODO
        row['category'] = '1_sequenced'
    elif len([r for r in rows if r['category'] == '2_choose']) < slots:
        row['category'] = '2_choose'
    else:
        row['category'] = '4_available'


def count_row_categories(rows, slots):
    """Get row counts for each category."""
    categories = """1_sequenced 2_choose 3_empty 4_available 5_unprocessed
        6_reject""".split()
    for category in categories:
        rows[0][category] = len([r for r in rows if r['category'] == category])
    rows[0]['slots'] = slots
    rows[0]['samples'] = len(rows) - 1
    rows[0]['3_empty'] = max(
        0, slots - rows[0]['1_sequenced'] - rows[0]['2_choose'])


def get_raw_data():
    """Read from database and format the data for further processing."""
    sql = """
        SELECT family, genus, scientific_name, rapid_total_dna, source_plate,
               source_well, rapid_input.sample_id, rapid_id
          FROM taxonomy
          JOIN taxon_ids USING (scientific_name)
     LEFT JOIN rapid_input ON (rapid_input.sample_id = taxon_ids.id)
      ORDER BY family, genus, rapid_total_dna DESC, scientific_name
    """
    taxons = pd.read_sql(sql, db.connect())
    pd.options.display.float_format = '{:.0f}'.format
    taxons.rapid_total_dna = pd.to_numeric(
        taxons.rapid_total_dna.fillna('0'), errors='coerce')
    cols = ['sample_id', 'source_plate', 'source_well', 'rapid_id']
    taxons.update(taxons[cols].fillna(''))

    return taxons.groupby(['family', 'genus'])


def get_slots(species):
    """Get the target number of species for the genus."""
    if species.shape[0] <= 5:
        return species.shape[0]
    elif species.shape[0] <= 12:
        return math.ceil(0.5 * species.shape[0])
    else:
        return math.ceil(0.25 * species.shape[0])


if __name__ == '__main__':
    main()
