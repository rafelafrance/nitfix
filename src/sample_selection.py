"""
Create a list of samples to select given the criteria below.

1. Toss every sample with a total DNA < 10 ng.

2. Priority taxa rules based off of the priority_taxa table:
    2a) Reject samples whose genus is not in the table.
    2b) Accept samples whose genus has a Priority of "High" in the table.

3. Genus count rules, given the above:
    3a. If we have <= 5 species TOTAL in a genus, submit everything we have.
    3b. If we have > 5 but <= 12 species TOTAL of genus, submit 50% of them.
    3c. If we have > 12 species in a genus, submit 25% of what we have.

    ** NOTE: These cutoff rules are not consistent. A genus with 12 species
       will have 6 slots but a genus with 13 species will have 4 (rounding up).

4. Also have to keep samples that have already been submitted for sequencing.
   So the sort order is submitted then yield grouped by genus.
"""

import os
import math
from enum import Enum, auto
from collections import OrderedDict
from pathlib import Path
from datetime import datetime
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import lib.db as db


pd.options.mode.chained_assignment = None
pd.options.display.float_format = '{:.0f}'.format


class Status(Enum):
    sequenced = auto()
    selected = auto()
    available = auto()
    unprocessed = auto()
    rejected = auto()


def select_samples():
    """Generate the report."""
    cxn = db.connect()

    families = get_families(cxn)
    get_genera(cxn, families)

    for (family_name, genus_name), samples in get_sampled_species(cxn):
        family = families[family_name]
        genus = family['genera'][genus_name]

        apply_rules_to_genus(samples, genus)

        sum_genus_totals(samples, genus)
        put_samples_in_genus(samples, genus)

    sum_family_totals(families)
    totals = sum_grand_totals(families)

    report_path = output_html(families, totals)
    output_csv(report_path, families)


def apply_rules_to_genus(samples, genus):
    """Update the samples according to the rules."""
    rule_mark_already_sequenced(samples, genus)
    rule_reject_low_priority_taxa(samples, genus)
    rule_mark_unprocessed(samples, genus)
    rule_reject_total_dna_too_low(samples, genus, threshold=10.0)
    rule_mark_available(samples, genus)
    rule_select_by_genus_count(samples, genus)


def rule_mark_already_sequenced(samples, genus):
    """Identify samples sequenced by RAPiD."""
    # samples[samples.qq.notna(), 'status'] = Status.sequenced


def rule_mark_unprocessed(samples, genus):
    """Identify unprocessed samples."""
    no_status = samples.status.isna()
    unprocessed = samples.source_plate.isna()
    samples.loc[no_status & unprocessed, 'status'] = Status.unprocessed


def rule_reject_total_dna_too_low(samples, genus, threshold=10.0):
    """Toss every sample with a total DNA < 10 ng."""
    no_status = samples.status.isna()
    too_low = samples.total_dna < threshold
    samples.loc[no_status & too_low, 'status'] = Status.rejected


def rule_reject_low_priority_taxa(samples, genus):
    """
    Select samples given the following rules:

    2a) Reject samples whose genus is not in the table.
    2b) Accept samples whose genus has a Priority of "High" in the table.
    """
    status = None if genus['priority'] else Status.rejected
    no_status = samples.status.isna()
    samples.loc[no_status, 'status'] = status


def rule_mark_available(samples, genus):
    """Everything else is available."""
    no_status = samples.status.isna()
    samples.loc[no_status, 'status'] = Status.available


def rule_select_by_genus_count(samples, genus):
    """Select samples based on the available slots and available samples."""
    end = samples.index[0] + genus['slots']
    slots = samples.index < end
    available = samples.status == Status.available
    samples.loc[slots & available, 'status'] = Status.selected


def sum_genus_totals(samples, genus):
    """Accumulate totals for the genus."""
    genus['sampled'] = samples.shape[0]
    for status_name, status in Status.__members__.items():
        genus[status_name] = samples.loc[
            samples.status == status, 'status'].count()


def sum_family_totals(families):
    """Accumulate totals."""
    keys = ['species_count', 'sampled', 'slots', 'empty_slots']
    keys += [status_name for status_name in Status.__members__.keys()]
    for family_name, family in families.items():
        for genus_name, genus in family['genera'].items():
            for key in keys:
                family[key] += genus[key]


def sum_grand_totals(families):
    """Accumulate totals."""
    grand = {
        'species_count': 0,
        'sampled': 0,
        'slots': 0,
        'empty_slots': 0}
    for status_name in Status.__members__.keys():
        grand[status_name] = 0

    for family_name, family in families.items():
        for key in grand.keys():
            grand[key] += family[key]

    return grand


def put_samples_in_genus(samples, genus):
    """Move the samples into the genus dictionary."""
    samples['status_value'] = samples.status.apply(lambda x: x.value)
    genus['samples'] = samples.fillna('').sort_values(
        ['status_value', 'sci_name']).to_dict(orient='records')


def get_families(cxn):
    """Extract the families from the taxonomy table."""
    sql = """SELECT DISTINCT family FROM taxonomy"""
    families = pd.read_sql(sql, cxn).set_index('family')

    families['species_count'] = 0
    families['slots'] = 0
    families['empty_slots'] = 0
    families['sampled'] = 0
    for status_name in Status.__members__.keys():
        families[status_name] = 0

    return families.to_dict(orient='index', into=OrderedDict)


def get_genera(cxn, families):
    """Get the genera from the taxonomy table and mark the priority genera."""
    sql = """
            WITH genera AS (
                SELECT family, genus, count(*) AS species_count
                  FROM taxonomy
              GROUP BY family, genus)
        SELECT family, genus, species_count,
               COALESCE(priority = 'High', 0) AS priority
          FROM genera
     LEFT JOIN priority_taxa USING (family, genus)
    """
    genera = pd.read_sql(sql, cxn)

    genera['slots'] = genera.species_count.apply(calculate_available_slots)
    not_priority = genera['priority'] == 0
    genera.loc[not_priority, 'slots'] = 0

    genera['empty_slots'] = genera['slots']
    genera['sampled'] = 0
    for status_name in Status.__members__.keys():
        genera[status_name] = 0

    for family_name, group in genera.groupby('family'):
        families[family_name]['genera'] = group.set_index('genus').to_dict(
            orient='index', into=OrderedDict)


def get_sampled_species(cxn):
    """Read from database and format the data for further processing."""
    sql = """
        SELECT family, genus, sci_name, total_dna, NULL as status,
               source_plate, source_well, rapid_qc_wells.sample_id
          FROM sample_wells
          JOIN taxon_ids           USING (sample_id)
          JOIN taxonomy            USING (sci_name)
     LEFT JOIN rapid_qc_wells      USING (plate_id, well)
     LEFT JOIN rapid_reformat_data USING (source_plate, source_well)
      ORDER BY family, genus, total_dna DESC, sci_name
    """
    species = pd.read_sql(sql, cxn)

    species.total_dna = species.total_dna.fillna(0)
    return species.groupby(['family', 'genus'])


def calculate_available_slots(count):
    """
    Get the target number of species for the genus.

    3a. If we have <= 5 species TOTAL in a genus, submit everything we have.
    3b. If we have > 5 but <= 12 species TOTAL of genus, submit 50% of them.
    3c. If we have > 12 species in a genus, submit 25% of what we have.
    """
    if count <= 5:
        return count
    elif count <= 12:
        return math.ceil(0.5 * count)
    else:
        return math.ceil(0.25 * count)


def output_html(families, totals):
    """Output the HTML report."""
    now = datetime.now()
    template_dir = os.fspath(Path('src') / 'reports')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('sample_selection.html')

    report = template.render(
        now=now, families=families, Status=Status, totals=totals)

    report_name = f'sample_selection_report_{now.strftime("%Y-%m-%d")}.html'
    report_path = Path('output') / report_name
    with report_path.open('w') as out_file:
        out_file.write(report)

    return report_path


def output_csv(report_path, families):
    """Output the CSV sidecar file."""
    csv_path = os.path.splitext(report_path)[0] + '.csv'

    all_samples = []
    for family_name, family in families.items():
        for genus_name, genus in family['genera'].items():
            samples = []
            for sample in genus.get('samples', []):
                if not sample['source_plate']:
                    continue
                row = OrderedDict()
                row['Plate'] = sample['source_plate']
                row['Well'] = sample['source_well']
                row['Sample ID'] = sample['sample_id']
                row['Family'] = family_name
                row['Genus'] = genus_name
                row['Scientific Name'] = sample['sci_name']
                row['Selected'] = ('Yes' if sample['status'].name
                                   in ['sequenced', 'selected'] else '')
                row['Status'] = sample['status'].name
                row['Total DNA (ng)'] = sample['total_dna']
                row['Species in Genus'] = genus['species_count']
                row['Sampled'] = genus['sampled']
                row['Sequenced'] = genus['sequenced']
                row['Automatically Selected'] = genus['selected']
                row['Available to Select'] = genus['available']
                row['Unprocessed Samples'] = genus['unprocessed']
                row['Rejected Samples'] = genus['rejected']
                row['Slots'] = genus['slots']
                row['Empty Slots'] = genus['empty_slots']
                samples.append(row)
            all_samples.append(pd.DataFrame(samples))

    all_samples = pd.concat(all_samples).sort_values(['Plate', 'Well'])
    all_samples.to_csv(csv_path, index=False)


if __name__ == '__main__':
    select_samples()
