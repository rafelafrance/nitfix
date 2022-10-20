"""Holds miscellaneous utility function."""

import os
import re
import uuid
from os.path import basename, join, split
from pathlib import Path

# Locations within the data directory
TEMP_DATA = Path('data') / 'temp'
PROCESSED_DATA = Path('data') / 'processed'
INTERIM_DATA = Path('data') / 'interim'
RAW_DATA = Path('data') / 'raw'
EXPEDITION_DATA = RAW_DATA / 'expeditions'
SAMPLED_DATA = RAW_DATA / 'sampled_images'
PHOTOS = RAW_DATA / 'photos'
ADJUSTED_DIR = RAW_DATA / 'adjusted'  # Where to store color adjusted images
IMAGE_DIRS = [
    'CAS-DOE-nitfix_specimen_photos',
    'DOE-nitfix_specimen_photos',
    'FLAS-DOE-nitfix_group1',
    'HUH_DOE-nitfix_specimen_photos',
    'MO-DOE-nitfix_specimen_photos',
    'MO-DOE-nitfix_visit2',
    'MO-DOE-nitfix_visit3',
    'NY_DOE-nitfix_visit3',
    'NY_DOE-nitfix_visit4',
    'NY_DOE-nitfix_visit5',
    'NY_visit_2',
    'OS_DOE-nitfix_specimen_photos',
    'Tingshuang_BRIT_nitfix_photos',
    'Tingshuang_FLAS_nitfix_photos',
    'Tingshuang_F_nitfix_photos',
    'Tingshuang_HUH_nitfix_photos',
    'Tingshuang_KUN_nitfix_photos',
    'Tingshuang_MO_nitfix_photos',
    'Tingshuang_NY_nitfix_photos',
    'Tingshuang_TEX_nitfix_photos',
    'Tingshuang_US_nitfix_photos']

# Locations of locally stored raw data
NOD_DIR = RAW_DATA / 'nodulation'
NOD_EXCEL = NOD_DIR / 'Nitfix_Nodulation_Data_Sheets.v2.xlsx'
NOD_CSV = str(NOD_DIR / 'Nitfix_Nodulation_Data_Sheets.v2.')
SPRENT89 = NOD_DIR / 'Sprent_1989_Table_1.pdf'
SPRENT_SHEET = 'Sprent'
SPRENT_COUNTS_CSV = NOD_DIR / 'Sprent_1989_Table_1.csv'
SPRENT_COUNTS_SHEET = 'Sprent_counts'
NON_FABALES_CSV = RAW_DATA / 'non-fabales_nodulation.csv'
SPRENT_DATA_CSV = RAW_DATA / 'sprent' / 'Nodulation_clade.csv'
WERNER_DATA_XLS = RAW_DATA / 'werner' / 'NitFixWernerEtAl2014.xlsx'

# Names of google worksheets
CORRALES_SHEET = 'corrales_data'
GENBANK_LOCI_SHEET = 'genbank_loci'
LOCI_SHEETS = ['P002-P077_Phylo_loci_assembled']
NORMAL_PLATE_SHEETS = [
    'FMN_131001_Normal_Plate_Layout',
    'KIB_135801_Normal_Plate_Layout']
PILOT_DATA_SHEET = 'UFBI_identifiers_photos'
PILOT_DATA_DIR = 'UFBI_sample_photos'   # A fake dir for database use
PRIORITY_TAXA_SHEET = 'Priority_Taxa_List'
QC_NORMAL_PLATE_SHEETS = [
    'FMN_131002_QC_Normal_Plate_Layout',
    'KIB_135802_QC_Normal_Plate_Layout']
REFORMATTING_TEMPLATE_SHEETS = [
    'FMN_131001_Reformatting_Template',
    'KIB_135802_Reformatting_template',
    'UFBI_Reformatting_Template']
SAMPLE_PLATES_SHEET = 'sample_plates'
SAMPLE_SHEETS = ['FMN_131001_SampleSheet']  # 'KIB_135802_SampleSheet']
SEQ_METADATA_SHEETS = ['FMN_131001_Sequencing_Metadata']
TAXONOMY_SHEETS = {
    'uf': 'NitFixMasterTaxonomy',
    'tingshuang': 'Tingshuang_NitFixMasterTaxonomy',
}

# Notes from Nature expedition worksheets are downloaded and stored locally
# EXPEDITIONS = [
#     '5657_Nit_Fix_I.reconcile.4.3.csv',
#     '5857_Nit_Fix_II.reconcile.0.4.4.csv',
#     '6415_Nit_Fix_III.reconcile.4.3.csv',
#     '6779_Nit_Fix_IV.reconcile.0.4.4.csv',
#     '6801_nitrogen_fixing_plants_v_east_coast.reconcile.0.4.4.csv',
#     '12077_nitfix-the-return.reconciled.0.4.7.csv',
#
#     ('10651_understanding-a-critical-symbiosis-nitrogen-fixing-in-'
#      'plants-missouri-botanical-gardens.reconciled.0.4.5.csv'),
# ]

# Use this photo as the base for color adjustments
EXEMPLAR = PHOTOS / 'Tingshuang_TEX_nitfix_photos' / 'L1040918.JPG'

# This is used to recognize valid local_ids
LOCAL_ID = re.compile(
    r'^.*? (nitfix|rosales|test) \D* (\d+) \D*$',
    re.IGNORECASE | re.VERBOSE)

PROCESSES = max(1, min(10, os.cpu_count() - 4))  # How many processes to use


class ReplaceDict(dict):
    """A class to either return a value or the key if missing."""

    def __missing__(self, key):
        """Return the key if missing."""
        return key


def is_uuid(guid):
    """Create a function to determine if a string is a valid UUID."""
    if not guid:
        guid = ''
    guid = str(guid).strip()
    try:
        uuid.UUID(guid)
        return True
    except (ValueError, AttributeError):
        return False


def normalize_file_name(path):
    """Normalize the file name for consistency."""
    dir_name, file_name = split(path)
    return join(basename(dir_name), file_name)


def get_reports_dir():
    """Find the directory containing the report templates."""
    top = os.fspath(Path('nitfix') / 'reports')
    return 'reports' if in_sub_dir() else top


def get_output_dir():
    """Find the output reports directory."""
    top = Path('..') / 'reports'
    return top if in_sub_dir() else Path('reports')


def get_report_data_dir():
    """Find the output reports directory."""
    base = '..' if in_sub_dir() else '.'
    return Path(base) / 'reports' / 'data'


def build_local_no(local_id):
    """Convert the local_id into something we can sort on consistently."""
    match = LOCAL_ID.match(local_id)
    lab = match[1].title()
    number = match[2].zfill(4)
    return f'{lab}_{number}'


def in_sub_dir():
    """Determine if we in the root or sub dir.

    A hack for running scripts in the "nitfix" subdirectory vs running it from
    the root directory.
    """
    return os.getcwd().endswith('nitfix/nitfix')
