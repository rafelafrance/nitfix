"""Holds miscellaneous utility function."""

import uuid
from pathlib import Path
from os.path import split, basename, join


IMAGE_ROOT = Path('..') / 'Dropbox'
IMAGE_ROOT_UP = Path('..') / '..' / 'Dropbox'
IMAGE_DIRS = [
    'OS_DOE-nitfix_specimen_photos',
    'CAS-DOE-nitfix_specimen_photos',
    'DOE-nitfix_specimen_photos',
    'NY_visit_2',
    'NY_DOE-nitfix_visit3',
    'NY_DOE-nitfix_visit4',
    'HUH_DOE-nitfix_specimen_photos',
    'MO-DOE-nitfix_specimen_photos',
    'MO-DOE-nitfix_visit2']


def is_uuid(guid):
    """Create a function to determine if a string is a valid UUID."""
    if not guid:
        guid = ''
    try:
        uuid.UUID(guid)
        return True
    except (ValueError, AttributeError):
        return False


def normalize_file_name(path):
    """Normalize the file name for consistency."""
    dir_name, file_name = split(path)
    return join(basename(dir_name), file_name)
