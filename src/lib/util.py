"""Holds miscellaneous utility function."""

import uuid
from pathlib import Path
from os.path import split, basename, join


EXPEDITION_DATA = Path('data') / 'raw' / 'expeditions'
INTERIM_DATA = Path('data') / 'interim'
PROCESSED_DATA = Path('data') / 'processed'
RAW_DATA = Path('data') / 'raw'

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
    'Tingshuang_FLAS_nitfix_photos',
    'Tingshuang_HUH_nitfix_photos',
    'Tinshuang_MO_nitfix_photos']


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
