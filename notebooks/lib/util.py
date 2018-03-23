"""Holds miscellaneous utility function."""

import uuid


def is_uuid(guid):
    """Create a function to determine if a string is a valid UUID."""
    try:
        uuid.UUID(guid)
        return True
    except ValueError:
        return False
