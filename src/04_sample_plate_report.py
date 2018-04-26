"""Print project status report."""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import lib.db as db


def get_wells(cxn):
    """Get well data from the database."""
    sql = """
        SELECT wells.*,
               scientific_name,
               ng_microliter_mean,
               family,
               rapid_input.concentration AS input_concentration,
               rapid_input.volume AS rapid_input_volume,
               rapid_input.rapid_concentration,
               rapid_input.rapid_volume,
               rapid_wells.volume AS rapid_well_volume
          FROM wells
          JOIN taxon_ids ON (wells.sample_id = taxon_ids.id)
          JOIN taxonomy USING (scientific_name)
     LEFT JOIN picogreen USING (picogreen_id)
     LEFT JOIN rapid_input USING (sample_id)
     LEFT JOIN rapid_wells USING (source_plate, source_well)
        ORDER BY local_no, row, col
    """
    wells = pd.read_sql(sql, cxn)
    return wells


def get_plates(wells):
    """Get a list of plates."""
    columns = ['plate_id', 'entry_date', 'local_id', 'protocol', 'notes']
    plates = wells.loc[:, columns]
    plates = plates.drop_duplicates()
    return plates


def get_plate_wells(wells):
    """Assign wells to their plate."""
    plate_wells = {}
    for group, plate in wells.groupby('local_no'):
        plate_id = plate['plate_id'].iloc[0]
        plate_wells[plate_id] = plate.fillna('').to_dict(orient='records')
    return plate_wells


def get_genus_coverage(cxn):
    """Get family and genus coverage."""
    taxonomy = pd.read_sql('SELECT * FROM taxonomy', cxn)
    taxonomy.rename(
        columns={'scientific_name': 'total', 'image_files': 'imaged'},
        inplace=True)
    taxonomy = taxonomy[['family', 'genus', 'total', 'imaged']]

    genera = taxonomy.groupby(['family', 'genus']).count()

    taxonomy['genus'] = ''
    families = taxonomy.groupby(['family', 'genus']).count()

    taxonomy['family'] = '~Total~'
    total = taxonomy.groupby(['family', 'genus']).count()

    coverage = pd.concat([families, genera, total])
    coverage['family'] = coverage.index.get_level_values('family')
    coverage['genus'] = coverage.index.get_level_values('genus')
    coverage['percent'] = coverage['imaged'] / coverage['total'] * 100.0

    coverage.sort_values(['family', 'genus'], inplace=True)
    return coverage


def print_report():
    """Generate the report."""
    cxn = db.connect()
    now = datetime.now()

    template_dir = os.fspath(Path('src') / 'reports')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('sample_plates_report.html')

    wells = get_wells(cxn)

    report = template.render(
        now=now,
        wells=get_plate_wells(wells),
        plates=get_plates(wells).to_dict(orient='records'),
        genera=get_genus_coverage(cxn).to_dict(orient='records'))

    report_name = f'sample_plates_report_{now.strftime("%Y-%m-%d")}.html'
    report_path = Path('output') / report_name
    with report_path.open('w') as out_file:
        out_file.write(report)


if __name__ == '__main__':
    print_report()
