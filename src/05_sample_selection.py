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


pd.options.mode.chained_assignment = None


def main():
    """Generate the report."""
    samples = get_sampled_species()
    totals = get_genera_totals()
    species = categorize_samples(samples, totals)
    totals = accum_totals(totals)

    report_path = output_html(species, totals)
    output_csv(report_path, species)


def categorize_samples(samples, totals):
    """Put the samples into to their category and accumulate totals."""
    groups = []
    for genus, species in samples.groupby(['family', 'genus']):
        slots = totals.at[genus, 'slots']
        is_available = species.category == '4_available'
        species.iloc[:slots].loc[is_available, 'category'] = '3_chosen'
        totals.at[genus, 'samples'] = species.shape[0]
        flds = """2_sequenced 3_chosen 4_available 5_unprocessed 6_rejected"""
        for field in flds.split():
            field_mask = species.category == field
            totals.at[genus, field] = field_mask.sum()
        totals.at[genus, 'empty_slots'] = max(
            0,
            totals.at[genus, 'slots']
            - totals.at[genus, '2_sequenced'] - totals.at[genus, '3_chosen'])
        groups.append(species)
    return pd.concat(groups)


def output_html(species, totals):
    """Output the HTML report."""
    now = datetime.now()
    template_dir = os.fspath(Path('src') / 'reports')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('sample_selection.html')

    totals['scientific_name'] = ''

    data = list(species.to_dict(orient='index').values())
    data += list(totals.to_dict(orient='index').values())
    data = sorted(
        data, key=lambda x: (x['family'], x['genus'],
                             x['category'], x['scientific_name']))

    report = template.render(now=now, data=data)

    report_name = f'sample_selection_report_{now.strftime("%Y-%m-%d")}.html'
    report_path = Path('output') / report_name
    with report_path.open('w') as out_file:
        out_file.write(report)

    return report_path


def output_csv(report_path, species):
    """Output the CSV sidecar file."""
    # csv_path = os.path.splitext(report_path)[0] + '.csv'
    # # csv_rows = build_csv(species)
    # df = pd.DataFrame(csv_rows).sort_values(['source_plate', 'source_well'])
    # df.category = df.category.str[2:]
    # df['selected'] = df.category.apply(
    #     lambda x: 'Yes' if x in ['sequenced', 'chosen'] else 'No')
    # df = df[['source_plate', 'source_well', 'sample_id', 'selected',
    #          'family', 'genus', 'scientific_name', 'category',
    #          'total_dna', 'genus_count', 'samples', '2_sequenced',
    #          '3_chosen', '4_available', '5_unprocessed', '6_rejected',
    #          'slots', 'empty_slots']]
    # df = df.rename(columns={
    #     'source_plate': 'Plate',
    #     'source_well': 'Well',
    #     'sample_id': 'Sample ID',
    #     'selected': 'Selected',
    #     'family': 'Family',
    #     'genus': 'Genus',
    #     'scientific_name': 'Scientific Name',
    #     'category': 'Category',
    #     'total_dna': 'Total DNA (ng)',
    #     'genus_count': 'Species in Genus',
    #     'samples': 'Sampled',
    #     '2_sequenced': 'Sequenced',
    #     '3_chosen': 'Automatically Chosen',
    #     '4_available': 'Available to Choose',
    #     '5_unprocessed': 'Unprocessed Samples',
    #     '6_rejected': 'Rejected Samples',
    #     'slots': 'Slots',
    #     'empty_slots': 'Empty Slots'})
    # df.to_csv(csv_path, index=False)


def get_sampled_species():
    """Read from database and format the data for further processing."""
    pd.options.display.float_format = '{:.0f}'.format
    sql = """
        SELECT family, genus, scientific_name, total_dna, source_plate,
               source_well, rapid_input.sample_id
          FROM taxonomy
          JOIN taxon_ids USING (scientific_name)
     LEFT JOIN rapid_input ON (rapid_input.sample_id = taxon_ids.id)
      ORDER BY family, genus, total_dna DESC, scientific_name
    """
    species = pd.read_sql(sql, db.connect())
    species['category'] = '4_available'  # Use this as the default

    species.total_dna = pd.to_numeric(
        species.total_dna.fillna('0'), errors='coerce')

    cols = ['sample_id', 'source_plate', 'source_well']
    species.update(species[cols].fillna(''))

    processed = species.source_plate != ''
    below_threshold = species.total_dna < 100.0
    species.loc[processed & below_threshold, 'category'] = '6_rejected'
    species.loc[~processed & below_threshold, 'category'] = '5_unprocessed'

    return species


def accum_totals(genera):
    """Calculate the family totals from the genera totals."""
    genera = genera.reset_index()

    families = genera.copy().groupby('family').sum()
    families = families.reset_index()
    families['category'] = '0_family'
    families['genus'] = ''

    grand = genera.copy()
    grand.family = '~Total~'
    grand = grand.groupby('family').sum()
    grand = grand.reset_index()
    grand['category'] = '9_total'
    grand['genus'] = ''

    totals = pd.concat([genera, families, grand], sort=True)
    totals = totals.set_index(['family', 'genus'], drop=False)
    totals = totals.sort_index()

    return totals


def get_genera_totals():
    """Get the fraction of species in a genus we want to cover."""
    sql = """
        SELECT family, genus, COUNT(*) AS genus_count
          FROM taxonomy
         WHERE scientific_name IS NOT NULL
      GROUP BY family, genus
    """
    genera = pd.read_sql(sql, db.connect())
    genera['slots'] = genera.genus_count.apply(get_slots)
    genera['empty_slots'] = genera.slots
    genera['category'] = '1_genus'
    for field in """2_sequenced 3_chosen 4_available 5_unprocessed 6_rejected
            samples""".split():
        genera[field] = 0
    genera = genera.set_index(['family', 'genus'])
    return genera


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
