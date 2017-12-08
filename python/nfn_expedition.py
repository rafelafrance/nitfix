"""Build a Notes from Nature expedition."""

# import os
import csv
from os.path import join


NYBG_DIR = join('data', 'nybg_expedition')


def get_files_missing_data():
    """Get information for files missing data."""
    missing = []
    csv_path = join('data', 'NYBarcodeSummary.csv')
    with open(csv_path) as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            print(row)
    return missing


def copy_images(missing):
    """Copy images to the expedition repository."""
    print(missing)


def create_manifest(missing):
    """Create expetion manifest."""
    print(missing)


def main():
    """The main function."""
    missing = get_files_missing_data()
    print(len(missing))
    # os.makedirs(NYBG_DIR, exist_ok=True)
    # copy_images(missing)
    # create_manifest(missing)


if __name__ == '__main__':
    main()
