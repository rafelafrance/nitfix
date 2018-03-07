"""Create a report on the sample plate data."""

from os.path import join
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import lib.db as db


def print_report():
    """Generate the report."""
    env = Environment(loader=FileSystemLoader(join('python', 'reports')))
    template = env.get_template('sample_plates_report.html')

    wells = {}
    with db.connect(factory=db.attr_factory) as db_conn:
        plates = list(db.select_plates(db_conn))
        for plate in plates:
            plate_id = plate['plate_id']
            wells[plate_id] = list(db.get_plate_report(db_conn, plate_id))
        missing = db.samples_not_in_taxonomies(db_conn)
        genera = db.family_genus_coverage(db_conn)
    report = template.render(
        now=datetime.now(),
        title='Sample plate report',
        plates=plates,
        wells=wells,
        genera=genera,
        missing=missing)

    with open('output/sample_plates_report.html', 'w') as out_file:
        out_file.write(report)


if __name__ == '__main__':
    print_report()
