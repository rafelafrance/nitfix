"""Print project status report."""

from datetime import datetime
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import lib.db as db
import lib.util as util


def generate_reports():
    """Generate all of the reports."""
    cxn = db.connect()
    now = datetime.now()

    sample_wells = get_wells(cxn)
    plates = get_plates(sample_wells)
    genera = get_genus_coverage(cxn)

    # generate_html_report(now, sample_wells, plates, genera)
    generate_excel_report(cxn, sample_wells, plates, genera)


def get_wells(cxn):
    """Get well data from the database."""
    sql = """
        select rt.sample_id,
               sci_name, family,
               qc.concentration, qc.total_dna,
               rt.volume, rt.rapid_source, rt.rapid_dest, rt.source_plate,
               rt.sample_id is not null as seq_returned,
               la.loci_assembled,
               sw.plate_id, sw.entry_date, sw.local_id, sw.local_no,
               sw.rapid_plates, sw.notes, sw.results, sw.row, sw.col,
               sw.well, sw.well_no
        from reformatting_templates as rt
        left join qc_normal_plate_layout as qc using (rapid_source)
        left join taxonomy_ids as ti using (sample_id)
        left join taxonomy as tx using (sci_name)
        left join loci_assembled as la using (rapid_dest)
        left join sample_wells as sw using (plate_id, well);
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
    for _, plate in sample_wells.groupby('local_no'):
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
         WHERE sci_name NOT IN (SELECT sci_name FROM in_images);
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
    template_dir = util.get_reports_dir()
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('sample_plates_report.html')

    report = template.render(
        now=now,
        wells=get_plate_wells(sample_wells),
        plates=plates.to_dict(orient='records'),
        genera=genera.to_dict(orient='records'))

    report_path = util.get_output_dir() / 'sample_plates_report.html'
    with report_path.open('w') as out_file:
        out_file.write(report)


def generate_excel_report(cxn, sample_wells, plates, genera):
    """Generate the Excel version of the report."""
    genera = genera.drop(['family', 'genus'], axis=1)

    sample_wells = sample_wells.drop(
        ['entry_date', 'local_id', 'rapid_plates', 'notes', 'plate_id',
         'row', 'col', 'results', 'volume'], axis=1)
    sample_wells = sample_wells.reindex(
        """local_no well_no well family sci_name sample_id rapid_source
            rapid_dest concentration total_dna loci_assembled
            """.split(), axis=1)
    nfn_data = pd.read_sql('SELECT * FROM nfn_data;', cxn)
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
        'sample_id': 'Sample ID',
        'concentration': 'Concentration (ng / uL)',
        'total_dna': 'Total DNA (ng)',
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
        'collection_no': 'Collection Number',
        'seq_returned': 'Sequence Returned?',
        'loci_assembled': 'Loci Assembled',
        'rapid_source': 'Rapid Source ID',
        'rapid_dest': 'Rapid Destination ID',
    }
    sample_wells = sample_wells.rename(columns=renames)

    xlsx_path = util.get_report_data_dir() / 'sample_plates_report.xlsx'
    with pd.ExcelWriter(xlsx_path) as writer:
        genera.to_excel(writer, sheet_name='Family Coverage')
        plates.to_excel(writer, sheet_name='Sample Plates')
        sample_wells.to_excel(
            writer, sheet_name='Sample Plate Wells', index=False)


if __name__ == '__main__':
    generate_reports()
