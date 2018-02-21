"""Resolve errors in the database."""

import argparse
import lib.db as db


def parse_command_line():
    """Get comand line arguments."""
    parser = argparse.ArgumentParser(
        description="""Extract information for the given files.""")

    parser.add_argument('--error-key', '-k', metavar='KEY',
                        help="""Update this record in the errors table.""")

    parser.add_argument('--resolution', '-r', metavar='RESOLUTION',
                        help="""Resolution of the error.""")

    args = parser.parse_args()
    return args


def main():
    """Resolve an error."""
    args = parse_command_line()

    with db.connect() as db_conn:
        db.resolve_error(db_conn, args.error_key, args.resolution)


if __name__ == '__main__':
    main()
