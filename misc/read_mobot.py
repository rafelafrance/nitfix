"""Scan the MOBOT data dump for matches in the database."""

import csv
import sys
import string
from pathlib import Path
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
from lib.dict_attr import DictAttrs
import lib.db as db


DATA_DIR = Path('.') / 'data'
MOBOT_DIR = DATA_DIR / 'dwca-tropicosspecimens-v1.17'
CONTINENTS = DATA_DIR / 'Continents'
OCCURRENCE = MOBOT_DIR / 'occurrence.txt'
IN_AFRICA = DATA_DIR / 'mobo_in_africa.txt'


def mobo_in_africa():
    """Scan the MOBOT data dump for records in Africa."""
    csv.field_size_limit(sys.maxsize)
    continents = gpd.read_file(str(CONTINENTS))
    africa = continents.iloc[3, 1]
    trans_table = str.maketrans('', '', string.punctuation)
    in_africa = {}

    with open(OCCURRENCE) as in_file:
        reader = csv.DictReader(in_file, delimiter='\t')
        for row in tqdm(reader):
            row = DictAttrs(row)
            if row.decimalLatitude and row.decimalLongitude:
                # print(row.decimalLatitude, row.decimalLongitude)
                point = Point(float(row.decimalLatitude),
                              float(row.decimalLongitude))
                if africa.contains(point):
                    phylo = row.scientificName.split()
                    sci_name = ' '.join(phylo[:2])
                    sci_name = sci_name.translate(trans_table)
                    in_africa[sci_name] = 1
                    # print(sci_name)

    with open(str(IN_AFRICA), 'w') as out_file:
        for sci_name in sorted(in_africa.keys()):
            out_file.write('{}\n'.format(sci_name))


def nitfix_in_africa():
    """Look for matching nitfix records in the mobo scrap."""
    db_conn = db.connect(factory=db.attr_factory)
    taxa = [t.scientific_name for t in db.get_taxonomies(db_conn)]


def main():
    """The main function."""
    # mobo_in_africa()


if __name__ == '__main__':
    main()
