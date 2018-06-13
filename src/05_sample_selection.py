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

    genera = get_sampled_species()
    genera_slots = get_genera_slots()

    species = []
    for genus, samples in genera:
        species += categorize_species(genus, samples, genera_slots[genus])

    template_dir = os.fspath(Path('src') / 'reports')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('sample_selection.html')

    report = template.render(now=now, species=species)

    report_name = f'sample_selection_report_{now.strftime("%Y-%m-%d")}.html'
    report_path = Path('output') / report_name
    with report_path.open('w') as out_file:
        out_file.write(report)

    csv_path, _ = os.path.splitext(report_path)
    csv_path += '.csv'
    csv_rows = build_csv(species)
    df = pd.DataFrame(csv_rows).sort_values(['source_plate', 'source_well'])
    df.category = df.category.str[2:]
    df['selected'] = df.category.apply(
        lambda x: 'Yes' if x in ['sequenced', 'chosen'] else 'No')
    df = df[['source_plate', 'source_well', 'sample_id', 'selected',
             'family', 'genus', 'scientific_name', 'category',
             'rapid_total_dna', 'samples', '2_sequenced',
             '3_chosen', '4_available', '5_unprocessed', '6_rejected',
             'slots', 'empty_slots']]
    df = df.rename(columns={
        'source_plate': 'Plate',
        'source_well': 'Well',
        'sample_id': 'Sample ID',
        'selected': 'Selected',
        'family': 'Family',
        'genus': 'Genus',
        'scientific_name': 'Scientific Name',
        'category': 'Category',
        'rapid_total_dna': 'Total DNA (ng)',
        'samples': 'Samples',
        '2_sequenced': 'Sequenced',
        '3_chosen': 'Automatically Chosen',
        '4_available': 'Available to Choose',
        '5_unprocessed': 'Unprocessed Samples',
        '6_rejected': 'Rejected Samples',
        'slots': 'Slots',
        'empty_slots': 'Empty Slots'})
    df.to_csv(csv_path, index=False)


def build_csv(species):
    """Create the rows that will go into the CSV."""
    plated = '2_sequenced 3_chosen 4_available 6_rejected'.split()
    rows = []

    for row in species:
        if row['category'] == '1_header':
            header = row
        elif row['category'] in plated:
            rows.append({**header, **row})

    return rows


def categorize_species(genus, species, genus_slots):
    """Put species rows into their categories."""
    species = species.to_dict(orient='records')

    header = {k: '' for k, v in species[0].items()}
    header['family'] = genus[0]
    header['genus'] = genus[1]
    header['category'] = '1_header'

    rows = [header]

    for row in species:
        set_row_category(row, rows, genus_slots)
        rows.append(row)

    count_row_categories(rows, genus_slots)

    return sorted(rows, key=lambda r: (r['category'], r['scientific_name']))


def set_row_category(row, rows, genus_slots):
    """Set row category."""
    if row['rapid_total_dna'] < 100.0 and row['source_plate']:
        row['category'] = '6_rejected'
    elif row['rapid_total_dna'] < 100.0 and not row['source_plate']:
        row['category'] = '5_unprocessed'
    elif False:  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! TODO
        row['category'] = '2_sequenced'
    elif len([r for r in rows if r['category'] == '3_chosen']) < genus_slots:
        row['category'] = '3_chosen'
    else:
        row['category'] = '4_available'


def count_row_categories(rows, genus_slots):
    """Get row counts for each category."""
    categories = """2_sequenced 3_chosen 4_available 5_unprocessed
        6_rejected""".split()
    for category in categories:
        rows[0][category] = len([r for r in rows if r['category'] == category])
    rows[0]['slots'] = genus_slots
    rows[0]['samples'] = len(rows) - 1
    rows[0]['empty_slots'] = max(
        0, genus_slots - rows[0]['2_sequenced'] - rows[0]['3_chosen'])


def get_sampled_species():
    """Read from database and format the data for further processing."""
    pd.options.display.float_format = '{:.0f}'.format
    sql = """
        SELECT family, genus, scientific_name, rapid_total_dna, source_plate,
               source_well, rapid_input.sample_id, rapid_id
          FROM taxonomy
          JOIN taxon_ids USING (scientific_name)
     LEFT JOIN rapid_input ON (rapid_input.sample_id = taxon_ids.id)
      ORDER BY family, genus, rapid_total_dna DESC, scientific_name
    """
    taxons = pd.read_sql(sql, db.connect())
    taxons.rapid_total_dna = pd.to_numeric(
        taxons.rapid_total_dna.fillna('0'), errors='coerce')
    cols = ['sample_id', 'source_plate', 'source_well', 'rapid_id']
    taxons.update(taxons[cols].fillna(''))

    return taxons.groupby(['family', 'genus'])


def get_genera_slots():
    """Get the fraction of species in a genus we want to cover."""
    sql = """
        SELECT family, genus, COUNT(*) AS genus_count
          FROM taxonomy
         WHERE scientific_name IS NOT NULL
      GROUP BY family, genus
    """
    genera = pd.read_sql(sql, db.connect())
    genera['slots'] = genera.genus_count.apply(get_slots)
    return genera.set_index(['family', 'genus']).slots.to_dict()


def get_slots(genus_count):
    """Get the target number of species for the genus."""
    if genus_count <= 5:
        return genus_count
    elif genus_count <= 12:
        return math.ceil(0.5 * genus_count)
    else:
        return math.ceil(0.25 * genus_count)


if __name__ == '__main__':
    main()
