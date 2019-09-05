"""Holds miscellaneous utility function."""

import re
import os
from os.path import split, basename, join
from pathlib import Path
import uuid


EXPEDITION_DATA = Path('data') / 'raw' / 'expeditions'
TEMP_DATA = Path('data') / 'temp'
PROCESSED_DATA = Path('data') / 'processed'
RAW_DATA = Path('data') / 'raw'

LOCAL_ID = re.compile(
    r'^.*? (nitfix|rosales|test) \D* (\d+) \D*$',
    re.IGNORECASE | re.VERBOSE)

PHOTOS = RAW_DATA / 'photos'
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
# 'UFBI_sample_photos',


class ReplaceDict(dict):
    """A class to either return a value or the key if missing."""

    def __missing__(self, key):
        """Return the key if missing."""
        return key


def is_uuid(guid):
    """Create a function to determine if a string is a valid UUID."""
    if not guid:
        guid = ''
    guid = str(guid)
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
    """Determine if we in the root or sub dir."""
    return os.getcwd().endswith('nitfix/nitfix')
