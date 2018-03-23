"""Holds miscellaneous utility function."""

import uuid


def is_uuid(guid):
    """Create a function to determine if a string is a valid UUID."""
    try:
        uuid.UUID(guid)
        return True
    except ValueError:
        return False


def split_uuids(string):
    """Split a string into a list of UUIDs."""
    if not isinstance(string, str):
        string = ''
    parts = [p.strip() for p in string.split(';')]
    return [p for p in parts if is_uuid(p)]
