"""Create a report on the sample plate data."""

from os.path import join
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import lib.db as db
import lib.util as util


def print_report():
    """Generate the report."""
    env = Environment(loader=FileSystemLoader(join('src', 'reports')))
    template = env.get_template('sample_plates_report.html')

    with db.connect(factory=db.attr_factory) as db_conn:
        plates = list(db.select_plates(db_conn))
        wells = get_plate_wells(db_conn, plates)
        taxon_ids, taxon_2_sample_ids = get_taxon_sample_ids(db_conn)
        missing = get_missing(db_conn, taxon_ids)
        genera = family_genus_coverage(db_conn, taxon_2_sample_ids)

    report = template.render(
        now=datetime.now(),
        plates=plates,
        wells=wells,
        genera=genera,
        missing=missing)

    with open('output/sample_plates_report.html', 'w') as out_file:
        out_file.write(report)


def get_taxon_sample_ids(db_conn):
    """Get the taxon sample IDs."""
    taxon_ids = set()
    taxon_2_sample_ids = {}
    for taxon in db.get_taxons(db_conn):
        for sample_id in taxon.get('sample_id', '').split(';'):
            sample_id = sample_id.strip()
            if util.is_uuid(sample_id):
                taxon_ids.add(sample_id)
                ids = taxon_2_sample_ids.get(taxon['scientific_name'], [])
                ids.append(sample_id)
                taxon_2_sample_ids[taxon['scientific_name']] = ids
    return taxon_ids, taxon_2_sample_ids


def get_missing(db_conn, taxon_ids):
    """Get the plate samples that are not in the master taxonomy."""
    missing = []
    for well in db.select_plate_wells(db_conn):
        well_id = well['sample_id']
        if util.is_uuid(well_id) and well_id not in taxon_ids:
            missing.append(well)
    return missing


def family_genus_coverage(db_conn, taxon_2_sample_ids):
    """Get family and genus coverage of images for the report."""
    images = {i['sample_id'] for i in db.get_images(db_conn)}

    total_key = ('~Total~', '')
    genera = {total_key: {'total': 0, 'imaged': 0}}
    for taxon in db.get_taxons(db_conn):
        genus_key = (taxon['family'], taxon['genus'])
        family_key = (taxon['family'], '')
        genus = genera.get(genus_key, {'total': 0, 'imaged': 0})
        family = genera.get(family_key, {'total': 0, 'imaged': 0})
        genus['total'] += 1
        family['total'] += 1
        genera[total_key]['total'] += 1
        imaged = is_taxon_imaged(taxon, taxon_2_sample_ids, images)
        genus['imaged'] += imaged
        family['imaged'] += imaged
        genera[total_key]['imaged'] += imaged
        genera[genus_key] = genus
        genera[family_key] = family

    covered = []
    for key, value in genera.items():
        percent = 0.0
        if value['total']:
            percent = value['imaged'] / value['total'] * 100.0
        covered.append({
            'family': key[0],
            'genus': key[1],
            'total': value['total'],
            'imaged': value['imaged'],
            'percent': percent,
        })
    return sorted(covered, key=lambda x: (x['family'], x['genus']))


def is_taxon_imaged(taxon, taxon_2_sample_ids, images):
    """Check if the taxon has been imaged."""
    imaged = 0
    if taxon['scientific_name'] in taxon_2_sample_ids:
        for sample_id in taxon_2_sample_ids[taxon['scientific_name']]:
            if sample_id in images:
                imaged = 1
    return imaged


def get_plate_wells(db_conn, plates):
    """Get the plate well data for the report."""
    wells = {}
    for plate in plates:
        plate_id = plate['plate_id']
        well_data = list(db.get_plate_report(db_conn, plate_id))
        for row in well_data:
            for key, value in row.items():
                row[key] = value if value else ''
        wells[plate_id] = well_data
    return wells


if __name__ == '__main__':
    print_report()
