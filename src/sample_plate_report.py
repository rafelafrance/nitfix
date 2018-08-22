"""Print project status report."""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import lib.db as db


def generate_reports():
    """Generate all of the reports."""
    cxn = db.connect()
    now = datetime.now()

    sample_wells = get_wells(cxn)
    plates = get_plates(sample_wells)
    genera = get_genus_coverage(cxn)

    generate_html_report(now, sample_wells, plates, genera)
    generate_excel_report(cxn, now, sample_wells, plates, genera)


def get_wells(cxn):
    """Get well data from the database."""
    sql = """
        SELECT sample_wells.*,
               sci_name,
               family,
               rapid_qc_wells.concentration AS input_concentration,
               rapid_qc_wells.volume        AS rapid_input_volume,
               rapid_qc_wells.concentration,
               rapid_qc_wells.total_dna,
               rapid_reformat_data.volume   AS rapid_well_volume
          FROM sample_wells
          JOIN taxon_ids           USING (sample_id)
          JOIN taxonomy            USING (sci_name)
     LEFT JOIN rapid_qc_wells      USING (plate_id, well)
     LEFT JOIN rapid_reformat_data USING (source_plate, source_well)
      ORDER BY local_no, row, col
    """
    sample_wells = pd.read_sql(sql, cxn)
    return sample_wells


def get_plates(sample_wells):
    """Get a list of plates."""
    columns = ['local_no', 'plate_id', 'entry_date',
               'local_id', 'rapid_plates', 'notes']
    plates = sample_wells.loc[:, columns]
    plates = plates.drop_duplicates()
    plates = plates.set_index('local_no')
    return plates


def get_plate_wells(sample_wells):
    """Assign wells to their plate."""
    plate_wells = {}
    for group, plate in sample_wells.groupby('local_no'):
        plate_id = plate['plate_id'].iloc[0]
        plate_wells[plate_id] = plate.fillna('').to_dict(orient='records')
    return plate_wells


def get_genus_coverage(cxn):
    """Get family and genus coverage."""
    sql = """
          WITH in_images AS (
            SELECT sci_name
              FROM taxonomy
             WHERE sample_id_1 IN (SELECT sample_id FROM images)
                OR sample_id_2 IN (SELECT sample_id FROM images)
                OR sample_id_3 IN (SELECT sample_id FROM images)
                OR sample_id_4 IN (SELECT sample_id FROM images)
                OR sample_id_5 IN (SELECT sample_id FROM images))
        SELECT family, genus, sci_name AS total, 1 AS imaged
          FROM taxonomy
         WHERE sci_name IN (SELECT sci_name FROM in_images)
     UNION ALL
        SELECT family, genus, sci_name AS total, 0 AS imaged
          FROM taxonomy
         WHERE sci_name NOT IN (SELECT sci_name FROM in_images)
    """
    taxonomy = pd.read_sql(sql, cxn)

    genera = taxonomy.groupby(['family', 'genus']).agg({
        'total': 'count', 'imaged': 'sum'})

    taxonomy['genus'] = ''
    families = taxonomy.groupby(['family', 'genus']).agg({
        'total': 'count', 'imaged': 'sum'})

    taxonomy['family'] = '~Total~'
    total = taxonomy.groupby(['family', 'genus']).agg({
        'total': 'count', 'imaged': 'sum'})

    coverage = pd.concat([families, genera, total])
    coverage['family'] = coverage.index.get_level_values('family')
    coverage['genus'] = coverage.index.get_level_values('genus')
    coverage['percent'] = coverage['imaged'] / coverage['total'] * 100.0

    return coverage.sort_index()


def generate_html_report(now, sample_wells, plates, genera):
    """Generate the HTML version of the report."""
    template_dir = os.fspath(Path('src') / 'reports')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('sample_plates_report.html')

    report = template.render(
        now=now,
        wells=get_plate_wells(sample_wells),
        plates=plates.to_dict(orient='records'),
        genera=genera.to_dict(orient='records'))

    report_name = f'sample_plates_report_{now.strftime("%Y-%m-%d")}.html'
    report_path = Path('output') / report_name
    with report_path.open('w') as out_file:
        out_file.write(report)


def generate_excel_report(cxn, now, sample_wells, plates, genera):
    """Generate the Excel version of the report."""
    report_name = f'sample_plates_report_{now.strftime("%Y-%m-%d")}.xlsx'
    report_path = Path('output') / report_name

    genera = genera.drop(['family', 'genus'], axis=1)

    sample_wells = sample_wells.drop(
        ['entry_date', 'local_id', 'rapid_plates', 'notes', 'plate_id',
         'row', 'col', 'results', 'rapid_well_volume'], axis=1)
    sample_wells = sample_wells.reindex(
        """local_no well_no well family sci_name sample_id
           input_concentration rapid_input_volume concentration
           total_dna""".split(), axis=1)
    nfn_data = pd.read_sql('SELECT * FROM nfn_data', cxn)
    sample_wells = sample_wells.merge(
        right=nfn_data, how='left', on='sample_id')
    sample_wells = sample_wells.drop(['subject_id'], axis=1)
    sample_wells = sample_wells.sort_values(['local_no', 'well'])
    renames = {
        'local_no': 'Local Plate Number',
        'well_no': 'Well Offset',
        'well': 'Well',
        'family': 'Family',
        'sci_name': 'Scientific Name',
        'input_concentration': 'Concentration (ng / uL)',
        'rapid_input_volume': 'Volume (uL)',
        'sample_id': 'Sample ID',
        'concentration':
            'RAPiD Genomics Lab Use ONLY\nConcentration (ng / uL)',
        'total_dna': 'RAPiD Genomics Lab Use ONLY\nTotal DNA (ng)',
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
        'subject_resolved_name': 'Resolved Name',
        'workflow_id': 'Workflow ID',
        'habitat_description': 'Habitat Description',
        'subject_provider_id': 'Provider ID',
        'collected_by_first_collector_last_name_only':
            'Primary Collector (Last Name Only)',
        'collector_number': 'Collector Number',
        'collection_no': 'Collection Number'}
    # print([x for x in renames.keys() if x not in sample_wells.columns])
    # print([x for x in sample_wells.columns if x not in renames.keys()])
    sample_wells = sample_wells.rename(columns=renames)

    with pd.ExcelWriter(report_path) as writer:
        genera.to_excel(writer, sheet_name='Family Coverage')
        plates.to_excel(writer, sheet_name='Sample Plates')
        sample_wells.to_excel(
            writer, sheet_name='Sample Plate Wells', index=False)


if __name__ == '__main__':
    generate_reports()
