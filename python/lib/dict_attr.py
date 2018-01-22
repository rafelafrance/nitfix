"""A convenience class so we can handle dictonaries like data classes."""


class DictAttrs(dict):
    """A convenience class so we can handle dictonaries like data classes."""

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
