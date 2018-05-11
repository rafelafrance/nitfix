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
               rapid_input.volume        AS rapid_input_volume,
               rapid_input.rapid_concentration,
               rapid_input.rapid_total_dna,
               rapid_wells.volume        AS rapid_well_volume
          FROM wells
          JOIN taxon_ids   ON    (wells.sample_id = taxon_ids.id)
          JOIN taxonomy    USING (scientific_name)
     LEFT JOIN picogreen   USING (picogreen_id)
     LEFT JOIN rapid_input USING (sample_id)
     LEFT JOIN rapid_wells USING (source_plate, source_well)
      ORDER BY local_no, row, col
    """
    wells = pd.read_sql(sql, cxn)
    return wells


def get_plates(wells):
    """Get a list of plates."""
    columns = ['local_no', 'plate_id', 'entry_date',
               'local_id', 'protocol', 'notes']
    plates = wells.loc[:, columns]
    plates = plates.drop_duplicates()
    plates = plates.set_index('local_no')
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
    taxonomy = (pd.read_sql('SELECT * FROM taxonomy', cxn)
                  .rename(columns={'scientific_name': 'total',
                                   'image_files': 'imaged'}))
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

    coverage = coverage.sort_values(['family', 'genus'])
    return coverage


def generate_reports():
    """Generate all of the reports."""
    cxn = db.connect()
    now = datetime.now()

    wells = get_wells(cxn)
    plates = get_plates(wells)
    genera = get_genus_coverage(cxn)

    generate_html_report(now, wells, plates, genera)
    generate_excel_report(cxn, now, wells, plates, genera)


def generate_html_report(now, wells, plates, genera):
    """Generate the HTML version of the report."""
    template_dir = os.fspath(Path('src') / 'reports')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('sample_plates_report.html')

    report = template.render(
        now=now,
        wells=get_plate_wells(wells),
        plates=plates.to_dict(orient='records'),
        genera=genera.to_dict(orient='records'))

    report_name = f'sample_plates_report_{now.strftime("%Y-%m-%d")}.html'
    report_path = Path('output') / report_name
    with report_path.open('w') as out_file:
        out_file.write(report)


def generate_excel_report(cxn, now, wells, plates, genera):
    """Generate the Excel version of the report."""
    report_name = f'sample_plates_report_{now.strftime("%Y-%m-%d")}.xlsx'
    report_path = Path('output') / report_name

    genera = genera.drop(['family', 'genus'], axis=1)

    wells = wells.drop(
        ['entry_date', 'local_id', 'protocol', 'notes', 'plate_id',
         'row', 'col', 'results', 'rapid_well_volume'], axis=1)
    wells = wells.reindex(['local_no', 'well_no', 'well',
                           'picogreen_id', 'family',
                           'scientific_name', 'sample_id',
                           'ng_microliter_mean', 'input_concentration',
                           'rapid_input_volume', 'rapid_concentration',
                           'rapid_total_dna'], axis=1)
    expeditions = pd.read_sql('SELECT * FROM expeditions', cxn)
    wells = wells.merge(right=expeditions, how='left', on='sample_id')
    wells = wells.drop(['subject_id'], axis=1)
    wells = wells.sort_values(['local_no', 'well'])
    wells = wells.rename(columns={
        'local_no': 'Local Plate Number',
        'well_no': 'Well Offset',
        'well': 'Well',
        'picogreen_id': 'Well Number',
        'family': 'Family',
        'scientific_name': 'Scientific Name',
        'ng_microliter_mean': 'Mean Yield (ng/µL)',
        'input_concentration': 'Concentration (ng / uL)',
        'rapid_input_volume': 'Volume (uL)',
        'sample_id': 'Sample ID',
        'rapid_concentration':
            'RAPiD Genomics Lab Use ONLY\nConcentration (ng / uL)',
        'rapid_total_dna': 'RAPiD Genomics Lab Use ONLY\nTotal DNA (ng)',
        'subject_id': 'subject_id',
        'country': 'Country',
        'state_province': 'State/Province',
        'county': 'County',
        'location': 'Location',
        'minimum_elevation': 'Minimum Elevation',
        'maximum_elevation': 'Maximum Elevation',
        'main_dropdown': 'Main Dropdown',
        'latitude_deg': 'Latitude ⁰',
        'latitude_min': "Latitude '",
        'latitude_sec': 'Latitude "',
        'longitude_deg': 'Longitude ⁰',
        'longitude_min': "Longitude '",
        'longitude_sec': 'Longitude "',
        'primary_collector_last_first_middle':
            'Primary Collector (*Last* *First* *Middle*)',
        'other_collectors_as_written': 'Other Collectors (as written)',
        'collector_number_numeric_only': 'Collector Number  (numeric only)',
        'collector_number_verbatim': 'Collector Number (verbatim)',
        'month_1': 'Month #1',
        'day_1': 'Day #1',
        'year_1': 'Year #1',
        'month_2': 'Month #2',
        'day_2': 'Day #2',
        'year_2': 'Year #2',
        'subject_image_name': 'Image Name',
        'subject_nybg_bar_code': 'Bar Code',
        'subject_resolved_name': 'Resolved Name'})

    with pd.ExcelWriter(report_path) as writer:
        genera.to_excel(writer, sheet_name='Family Coverage')
        plates.to_excel(writer, sheet_name='Sample Plates')
        wells.to_excel(writer, sheet_name='Sample Plate Wells', index=False)


if __name__ == '__main__':
    generate_reports()
