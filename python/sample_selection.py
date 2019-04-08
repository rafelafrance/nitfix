"""
Create a list of samples to select given the criteria below.

1. Remove samples associated with more than one scientific names.

2. Toss every sample with a total DNA < 10 ng.

3. All outgroups are high priority. An outgroup will have a ":" in its family
   field.

4. Priority taxa rules based off of the priority_taxa table:
    2a) Reject samples whose genus is not in the table.
    2b) Accept samples whose genus has a Priority of "High" in the table.
    2c) Medium priority genera are filtered in step 3.

5. Genus count rules, given the above:
    3a. If we have <= 5 species TOTAL in a genus, submit everything we have.
    3b. If we have > 5 but <= 12 species TOTAL of genus, submit 50% of them.
    3c. If we have > 12 species in a genus, submit 25% of what we have.

    ** NOTE: These cutoff rules are not consistent. A genus with 12 species
       will have 6 slots but a genus with 13 species will have 4 (rounding up).

6. Also have to keep samples that have already been submitted for sequencing.
   So the sort order is submitted then yield grouped by genus.
"""

import math
from enum import Enum, auto
from collections import OrderedDict
from datetime import datetime
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import lib.db as db
import lib.util as util


pd.options.mode.chained_assignment = None
pd.options.display.float_format = '{:.0f}'.format


class Status(Enum):
    """Sample status based upon the above criteria."""

    sequenced = auto()
    selected = auto()
    available = auto()
    unprocessed = auto()
    reject_scientific_name = auto()
    reject_yield_too_low = auto()
    reject_low_priority = auto()


def select_samples():
    """Generate the report."""
    cxn = db.connect()

    taxonomy_errors = get_taxonomy_errors(cxn)
    families = get_families(cxn)
    get_genera(cxn, families)
    sample_groups = get_sampled_species(cxn, taxonomy_errors)

    for (family_name, genus_name), samples in sample_groups:
        family = families[family_name]
        genus = family['genera'][genus_name]

        apply_rules_to_genus(samples, genus, taxonomy_errors)

        sum_genus_totals(samples, genus)
        put_samples_in_genus(samples, genus)

    sum_family_totals(families)
    totals = sum_grand_totals(families)

    output_html(families, totals)
    output_csv(families)


def apply_rules_to_genus(samples, genus, taxonomy_errors):
    """Update the samples according to the rules."""
    rule_mark_already_sequenced()
    rule_mark_unprocessed(samples)
    rule_mark_available(samples)
    rule_reject_too_many_sci_names(samples, taxonomy_errors)
    rule_reject_total_dna_too_low(samples, threshold=10.0)
    rule_select_all_outgroups(samples, genus)
    rule_reject_no_priority(samples, genus)
    rule_select_high_priority_taxa(samples, genus)
    rule_select_by_genus_count(samples, genus)


def rule_mark_already_sequenced():  # samples, genus):
    """Identify samples sequenced by RAPiD."""
    # samples[samples.qq.notna(), 'status'] = Status.sequenced


def rule_mark_unprocessed(samples):
    """Identify unprocessed samples."""
    no_status = samples.status.isna()
    unprocessed = samples.source_plate.isna()
    samples.loc[no_status & unprocessed, 'status'] = Status.unprocessed


def rule_mark_available(samples):
    """Identify samples that may be selected."""
    no_status = samples.status.isna()
    samples.loc[no_status, 'status'] = Status.available


def rule_reject_too_many_sci_names(samples, taxonomy_errors):
    """Toss every sample associated with more than one scientific name."""
    available = samples.status == Status.available
    names_err = samples.sample_id.isin(taxonomy_errors)
    samples.loc[
        available & names_err, 'status'] = Status.reject_scientific_name


def rule_reject_total_dna_too_low(samples, threshold=10.0):
    """Toss every sample with a total DNA < threshold ng."""
    available = samples.status == Status.available
    too_low = samples.total_dna < threshold
    samples.loc[available & too_low, 'status'] = Status.reject_yield_too_low


def rule_select_all_outgroups(samples, genus):
    """All outgroups are high priority."""
    if ':' in genus['family']:
        available = samples.status == Status.available
        samples.loc[available, 'status'] = Status.selected


def rule_reject_no_priority(samples, genus):
    """Reject any genus without a priority."""
    if genus['priority'] == '':
        available = samples.status == Status.available
        samples.loc[available, 'status'] = Status.reject_low_priority


def rule_select_high_priority_taxa(samples, genus):
    """Select any sample with a high priority."""
    if genus['priority'] == 'High':
        available = samples.status == Status.available
        samples.loc[available, 'status'] = Status.selected


def rule_select_by_genus_count(samples, genus):
    """Select samples based on the available slots and available samples."""
    if genus['priority'] == 'Medium':
        end_index = samples.index[0] + genus['slots']
        slots = samples.index < end_index
        available = samples.status == Status.available
        samples.loc[slots & available, 'status'] = Status.selected


def get_accum_keys():
    """Get fields to accumulate."""
    keys = ['species_count', 'sampled', 'slots', 'sent_for_qc', 'rejected']
    return keys + [status_name for status_name in Status.__members__.keys()]


def sum_genus_totals(samples, genus):
    """Accumulate totals for the genus."""
    for key in get_accum_keys():
        genus.setdefault(key, 0)

    genus['sampled'] = samples.shape[0]
    genus['sent_for_qc'] = samples.source_plate.notna().sum()
    for status_name, status in Status.__members__.items():
        genus[status_name] = samples.loc[
            samples.status == status, 'status'].count()

    rejects = (n for n in Status.__members__.keys() if n.startswith('reject_'))
    for status_name in rejects:
        genus['rejected'] += genus[status_name]


def sum_family_totals(families):
    """Accumulate totals."""
    keys = get_accum_keys()
    for family in families.values():
        for genus in family['genera'].values():
            for key in keys:
                family.setdefault(key, 0)
                family[key] += genus.get(key, 0)


def sum_grand_totals(families):
    """Accumulate totals."""
    grand = {}
    keys = get_accum_keys()
    for family in families.values():
        for key in keys:
            grand.setdefault(key, 0)
            grand[key] += family.get(key, 0)

    return grand


def get_taxonomy_errors(cxn):
    """Get taxonomy errors from the database."""
    taxonomy_errors = pd.read_sql('SELECT * FROM taxonomy_errors;', cxn)
    return taxonomy_errors.set_index('sample_id').sci_name_1.to_dict()


def put_samples_in_genus(samples, genus):
    """Move the samples into the genus dictionary."""
    samples['status_value'] = samples.status.apply(lambda x: x.value)
    genus['samples'] = samples.fillna('').sort_values(
        ['status_value', 'sci_name']).to_dict(orient='records')


def get_families(cxn):
    """Extract the families from the taxonomy table."""
    sql = """SELECT DISTINCT family FROM taxonomy;"""
    families = pd.read_sql(sql, cxn).set_index('family')

    return families.to_dict(orient='index', into=OrderedDict)


def get_genera(cxn, families):
    """Get the genera from the taxonomy table and mark the priority genera."""
    sql = """
            WITH genera AS (
                SELECT family, genus, count(*) AS species_count
                  FROM taxonomy
              GROUP BY family, genus)
        SELECT family, genus, species_count, COALESCE(priority, '') AS priority
          FROM genera
     LEFT JOIN priority_taxa USING (family, genus);
    """
    genera = pd.read_sql(sql, cxn)

    genera['slots'] = genera.species_count.apply(calculate_available_slots)
    not_priority = genera['priority'] == ''
    genera.loc[not_priority, 'slots'] = 0

    for family_name, group in genera.groupby('family'):
        families[family_name]['genera'] = group.set_index('genus').to_dict(
            orient='index', into=OrderedDict)


def get_sampled_species(cxn, taxonomy_errors):
    """Read from database and format the data for further processing."""
    sql = """
        SELECT family, genus, sci_name, total_dna, NULL as status,
               source_plate, source_well, rapid_qc_wells.sample_id
          FROM sample_wells
     LEFT JOIN taxonomy_ids        USING (sample_id)
     LEFT JOIN taxonomy            USING (sci_name)
     LEFT JOIN rapid_qc_wells      USING (plate_id, well)
     LEFT JOIN rapid_reformat_data USING (source_plate, source_well)
         WHERE length(sample_wells.sample_id) = 36
      ORDER BY family, genus, total_dna DESC, sci_name;
    """
    species = pd.read_sql(sql, cxn)

    problems = species.sample_id.isin(taxonomy_errors)
    species.loc[problems, 'sci_name'] = 'Unknown species'
    species = species.drop_duplicates(['source_plate', 'source_well'])

    species.total_dna = species.total_dna.fillna(0)

    return species.groupby(['family', 'genus'])


def calculate_available_slots(count):
    """
    Get the target number of species for the genus.

    a. If we have <= 5 species TOTAL in a genus, submit everything we have.
    b. If we have > 5 but <= 12 species TOTAL of genus, submit 50% of them.
    c. If we have > 12 species in a genus, submit 25% of what we have.
    """
    if count <= 5:
        return count
    if count <= 12:
        return math.ceil(0.5 * count)
    return math.ceil(0.25 * count)


def output_html(families, totals):
    """Output the HTML report."""
    now = datetime.now()
    template_dir = util.get_reports_dir()
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('sample_selection.html')

    report = template.render(
        now=now, families=families, Status=Status, totals=totals)

    report_path = util.get_output_dir() / 'sample_selection.html'
    with report_path.open('w') as out_file:
        out_file.write(report)


def output_csv(families):
    """Output the CSV sidecar file."""
    all_samples = []
    for family_name, family in families.items():
        for genus_name, genus in family['genera'].items():
            samples = []
            for sample in genus.get('samples', []):
                if not sample['source_plate']:
                    continue

                selected = ('Yes' if sample['status'].name
                            in ['sequenced', 'selected'] else '')
                status = sample['status'].name.replace('_', ' ')

                row = OrderedDict()
                row['Plate'] = sample['source_plate']
                row['Well'] = sample['source_well']
                row['Sample ID'] = sample['sample_id']
                row['Family'] = family_name
                row['Genus'] = genus_name
                row['Scientific Name'] = sample['sci_name']
                row['Selected'] = selected
                row['Status'] = status
                row['Total DNA (ng)'] = sample['total_dna']
                row['Priority'] = genus['priority']
                row['Species in Genus'] = genus['species_count']
                row['Sampled'] = genus['sampled']
                row['Sent to Rapid'] = genus['sent_for_qc']
                row['Sequenced'] = genus['sequenced']
                row['Automatically Selected'] = genus['selected']
                row['Available to Select'] = genus['available']
                row['Unprocessed Samples'] = genus['unprocessed']
                row['Rejected Samples'] = genus['rejected']
                samples.append(row)
            all_samples.append(pd.DataFrame(samples))

    all_samples = pd.concat(all_samples)

    dummies = []
    for plate in all_samples.Plate.unique():
        for row in 'ABCDEFGH':
            for col in range(1, 13):
                dummies.append({
                    'Plate': plate,
                    'Well': f'{row}{col:02d}'})
    dummies = pd.DataFrame(dummies)

    all_samples = pd.merge_ordered(
        dummies, all_samples, how='left', on=['Plate', 'Well'])

    all_samples = all_samples.sort_values(['Plate', 'Well'])
    csv_path = util.get_report_data_dir() / 'sample_selection.csv'
    all_samples.to_csv(csv_path, index=False)


if __name__ == '__main__':
    select_samples()
