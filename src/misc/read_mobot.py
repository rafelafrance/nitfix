"""Scan the MOBOT data dump for matches in the database."""

import csv
import sys
from pathlib import Path
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
from lib.dict_attr import DictAttrs
import lib.db as db


DATA_DIR = Path('.') / 'data'
MOBOT_DIR = DATA_DIR / 'dwca-tropicosspecimens-v1.17'
CONTINENTS_FILE = DATA_DIR / 'Continents'
OCCURRENCE = MOBOT_DIR / 'occurrence.txt'
IN_AFRICA = DATA_DIR / 'mobot_in_africa.txt'
INTERSECTION = DATA_DIR / 'mobot_nitfix_africa.txt'
COUNTRY_CODES = [
    'DZ', 'AO', 'SH', 'BJ', 'BW', 'BF', 'BI', 'CM', 'CV', 'CF',
    'TD', 'KM', 'CG', 'CD', 'DJ', 'EG', 'GQ', 'ER', 'ET', 'GA',
    'GM', 'GH', 'GN', 'GW', 'CI', 'KE', 'LS', 'LR', 'LY', 'MG',
    'MW', 'ML', 'MR', 'MU', 'YT', 'MA', 'MZ', 'NA', 'NE', 'NG',
    'ST', 'RE', 'RW', 'ST', 'SN', 'SC', 'SL', 'SO', 'ZA', 'SS',
    'SH', 'SD', 'SZ', 'TZ', 'TG', 'TN', 'UG', 'CD', 'ZM', 'TZ', 'ZW']
CONTINENTS = gpd.read_file(str(CONTINENTS_FILE))
AFRICA = CONTINENTS.iloc[3, 1]


def mobot_data():
    """Look at the MOBOT occurrence data."""
    count = 0
    country = 0
    continent = 0
    country_code = 0
    decimal_latitude = 0
    decimal_longitude = 0
    genus = 0
    specific_epithet = 0
    scientific_name = 0

    with open(OCCURRENCE) as in_file:
        reader = csv.DictReader(in_file, delimiter='\t')
        print(reader.fieldnames)
        for row in tqdm(reader):
            row = DictAttrs(row)
            count += 1
            country += 1 if row.country else 0
            continent += 1 if row.continent else 0
            country_code += 1 if row.countryCode else 0
            decimal_latitude += 1 if row.decimalLatitude else 0
            decimal_longitude += 1 if row.decimalLongitude else 0
            genus += 1 if genus else 0
            specific_epithet += 1 if row.specificEpithet else 0
            scientific_name += 1 if row.scientificName else 0

    print('count', count)
    print('country', country)
    print('continent', continent)
    print('countryCode', country_code)
    print('decimalLatitude', decimal_latitude)
    print('decimalLongitude', decimal_longitude)
    print('genus', genus)
    print('specificEpithet', specific_epithet)
    print('scientificName', scientific_name)


def mobot_in_africa():
    """Scan the MOBOT data dump for records in Africa."""
    in_africa = set()

    with open(OCCURRENCE) as in_file:
        reader = csv.DictReader(in_file, delimiter='\t')
        for row in tqdm(reader):
            row = DictAttrs(row)
            if inside_africa(row):
                phylo = row.scientificName.split()
                sci_name = ' '.join(phylo[:2])
                in_africa.add(sci_name)

    with open(str(IN_AFRICA), 'w') as out_file:
        for sci_name in sorted(in_africa):
            out_file.write('{}\n'.format(sci_name))


def inside_africa(row):
    """Test if the occurrence record is for Africa."""
    if row.countryCode in COUNTRY_CODES:
        return True

    if row.continent.lower() == 'africa':
        return True

    if row.decimalLatitude and row.decimalLongitude:
        point = Point(float(row.decimalLatitude),
                      float(row.decimalLongitude))
        if AFRICA.contains(point):
            return True

    return False


def nitfix_in_africa():
    """Look for matching nitfix records in the mobo scrap."""
    cxn = db.connect(factory=db.attr_factory)
    taxa = {t.scientific_name for t in db.get_taxon_names(cxn)}
    with open(str(IN_AFRICA)) as in_file:
        mobot = in_file.readlines()
    mobot = {ln.strip() for ln in mobot}
    print('Length mobot', len(mobot))
    print('Length nitfix', len(taxa))
    intersection = mobot & taxa
    print(len(intersection))

    with open(str(INTERSECTION), 'w') as out_file:
        for taxon in sorted(intersection):
            out_file.write('{}\n'.format(taxon))


def main():
    """Run the script."""
    csv.field_size_limit(sys.maxsize)
    # mobot_data()
    mobot_in_africa()
    nitfix_in_africa()


if __name__ == '__main__':
    main()
