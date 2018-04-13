
# coding: utf-8

# <h1>Table of Contents<span class="tocSkip"></span></h1>
# <div class="toc"><ul class="toc-item"><li><span><a href="#Add-Families-to-Picogreen-Results" data-toc-modified-id="Add-Families-to-Picogreen-Results-1"><span class="toc-item-num">1&nbsp;&nbsp;</span>Add Families to Picogreen Results</a></span></li><li><span><a href="#Assign-UUIDs-to-Corrales-Samples" data-toc-modified-id="Assign-UUIDs-to-Corrales-Samples-2"><span class="toc-item-num">2&nbsp;&nbsp;</span>Assign UUIDs to Corrales Samples</a></span></li><li><span><a href="#Get-Numbers-of-Families-per-Plate" data-toc-modified-id="Get-Numbers-of-Families-per-Plate-3"><span class="toc-item-num">3&nbsp;&nbsp;</span>Get Numbers of Families per Plate</a></span></li><li><span><a href="#Get-the-Coverage-of-the-Genbank-Loci-vs.-the-Sampled-Master-Taxonomy" data-toc-modified-id="Get-the-Coverage-of-the-Genbank-Loci-vs.-the-Sampled-Master-Taxonomy-4"><span class="toc-item-num">4&nbsp;&nbsp;</span>Get the Coverage of the Genbank Loci vs. the Sampled Master Taxonomy</a></span></li><li><span><a href="#Assign-UUIDs-to-UFIB-(Pilot-Study)-Images" data-toc-modified-id="Assign-UUIDs-to-UFIB-(Pilot-Study)-Images-5"><span class="toc-item-num">5&nbsp;&nbsp;</span>Assign UUIDs to UFIB (Pilot Study) Images</a></span></li><li><span><a href="#Picogreen-Yields" data-toc-modified-id="Picogreen-Yields-6"><span class="toc-item-num">6&nbsp;&nbsp;</span>Picogreen Yields</a></span></li><li><span><a href="#Scan-MOBOT-Data-Dump" data-toc-modified-id="Scan-MOBOT-Data-Dump-7"><span class="toc-item-num">7&nbsp;&nbsp;</span>Scan MOBOT Data Dump</a></span></li><li><span><a href="#Validate-DB" data-toc-modified-id="Validate-DB-8"><span class="toc-item-num">8&nbsp;&nbsp;</span>Validate DB</a></span></li><li><span><a href="#UUIDs-as-Base-64" data-toc-modified-id="UUIDs-as-Base-64-9"><span class="toc-item-num">9&nbsp;&nbsp;</span>UUIDs as Base-64</a></span></li></ul></div>

# # Add Families to Picogreen Results

# In[1]:


from pathlib import Path

import pandas as pd

import lib.db as db
import lib.google as google


# In[2]:


CXN = db.connect()
INTERIM_DATA = Path('..') / 'data' / 'interim'


# In[3]:


name = 'picogreen_results'
google.sheet_to_excel(name, INTERIM_DATA / f'{name}.xls')


# # Assign UUIDs to Corrales Samples

# In[10]:


import uuid
from pathlib import Path

import numpy as np
import pandas as pd

import lib.db as db


# In[11]:


CXN = db.connect()
INTERIM_DATA = Path('..') / 'data' / 'interim'


# In[26]:


corrales = """
    SELECT id FROM taxon_ids
     WHERE id LIKE '%Corrales%'
        OR id LIKE '%corrales%'
    """
corrales = pd.read_sql(sql, CXN)
corrales['sample_id'] = corrales.id.map(lambda x: uuid.uuid4())

corrales['image_file'] = corrales.id.str.split(r':\s*').str[1]
corrales.image_file = corrales.image_file.str.replace(' ', '_')
corrales.image_file = corrales.image_file.apply(
    lambda x: f'../data/raw/missing_photos/{x}.jpg')

corrales.rename(columns={'id': 'corrales_id'}, inplace=True)

corrales.head()


# In[27]:


CORRALES_OUT = INTERIM_DATA / 'corrales.csv'
# corrales.to_csv(CORRALES_OUT, index=False)


# # Get Numbers of Families per Plate

# In[32]:


from pathlib import Path
import pandas as pd
import lib.db as db


# In[33]:


CXN = db.connect()
INTERIM_DATA = Path('..') / 'data' / 'interim'


# In[7]:


sql = """
select local_id, count(distinct family) as families
  from wells
  join taxon_ids on (wells.sample_id = taxon_ids.id)
  join taxonomy using (scientific_name)
 group by local_id
 order by families desc, local_no
"""
totals = pd.read_sql(sql, CXN)
totals.head()


# In[15]:


sql = """
select distinct local_id, local_no, family
  from wells
  join taxon_ids ON (wells.sample_id = taxon_ids.id)
  join taxonomy using (scientific_name)
 order by local_no, family
"""
families = pd.read_sql(sql, CXN)
families.head()


# In[28]:


report = {}
for _, family in families.iterrows():
    fams = report.get(family.local_no, [])
    fams.append(family.family)
    report[family.local_no] = fams

report = [(lid, fam) for lid, fam in report.items()]

report = sorted(report, key=lambda x: (len(x[1]), x[0]), reverse=True)
report[0]


# In[36]:


data = [['Plate', 'Count', 'Families']]
for ln in report:
    data.append([ln[0], len(ln[1]), ', '.join(ln[1])])

df = pd.DataFrame(data)
df.to_csv(INTERIM_DATA / 'families_per_plate.csv', index=False)
df.head()


# # Get the Coverage of the Genbank Loci vs. the Sampled Master Taxonomy
# 
# ** Note: Run this after ingesting the worksheets **

# In[8]:


from pathlib import Path
import pandas as pd
import lib.db as db


# In[9]:


CXN = db.connect()


# In[34]:


INTERIM_DATA = Path('..') / 'data' / 'interim'


# In[54]:


data_path = Path('..') / 'data'
db_path = str(data_path / 'processed' / 'nitfix.sqlite.db')
interim_data = data_path / 'interim'

in_both = str(interim_data / 'in_both.csv')
in_neither = str(interim_data / 'in_neither.csv')
in_loci_only = str(interim_data / 'in_loci_only.csv')
in_sampled_only = str(interim_data / 'in_sampled_only.csv')


# In[44]:


query = (
    "'SELECT DISTINCT scientific_name "
      "FROM taxons "
     "WHERE scientific_name {} ("
           "SELECT scientific_name FROM samples "
            "WHERE scientific_name IS NOT NULL) "
       "AND scientific_name {} ("
            "SELECT scientific_name FROM loci "
              "WHERE (its + atpb + matk + matr + rbcl) > 0) "
  "ORDER BY scientific_name;'")
    
in_both_sql = query.format('IN', 'IN')
in_neither_sql = query.format('NOT IN', 'NOT IN')
in_loci_only_sql = query.format('NOT IN', 'IN')
in_sampled_only_sql = query.format('IN', 'NOT IN')


# In[45]:


print(in_both_sql)
print()
print(in_neither_sql)
print()
print(in_loci_only_sql)
print()
print(in_sampled_only_sql)


# In[47]:


get_ipython().system('sqlite3 -header -csv $db_path $in_both_sql         > $in_both')
get_ipython().system('sqlite3 -header -csv $db_path $in_loci_only_sql    > $in_loci_only')
get_ipython().system('sqlite3 -header -csv $db_path $in_sampled_only_sql > $in_sampled_only')
get_ipython().system('sqlite3 -header -csv $db_path $in_neither_sql      > $in_neither')


# In[50]:


# loci = pd.read_sql("""SELECT scientific_name FROM loci
#                        WHERE its + atpb + matk + matr + rbcl > 0""", CXN)
# taxons = pd.read_sql('SELECT scientific_name FROM taxons', CXN)
# samples = pd.read_sql('SELECT scientific_name FROM samples', CXN)

# loci = loci.dropna()
# taxons = taxons.dropna()
# samples = samples.dropna()

# loci = set(loci.scientific_name.unique())
# taxons = set(taxons.scientific_name.unique())
# samples = set(samples.scientific_name.unique())


# In[51]:


# in_both = taxons & loci & samples
# in_neither = taxons - loci - samples
# in_loci_only = taxons & loci - samples
# in_samples_only = taxons - loci & samples

# print('in_both', len(in_both))
# print('in_neither', len(in_neither))
# print('in_loci_only', len(in_loci_only))
# print('in_samples_only', len(in_samples_only))


# In[53]:


# with open(INTERIM_DATA / 'in_both.csv', 'w') as out_file:
#     out_file.write('scientific_name\n')
#     for name in sorted(in_both):
#         out_file.write(f'{name}\n')

# with open(INTERIM_DATA / 'in_neither.csv', 'w') as out_file:
#     out_file.write('scientific_name\n')
#     for name in sorted(in_neither):
#         out_file.write(f'{name}\n')

# with open(INTERIM_DATA / 'in_loci_only.csv', 'w') as out_file:
#     out_file.write('scientific_name\n')
#     for name in sorted(in_loci_only):
#         out_file.write(f'{name}\n')

# with open(INTERIM_DATA / 'in_samples_only.csv', 'w') as out_file:
#     out_file.write('scientific_name\n')
#     for name in sorted(in_samples_only):
#         out_file.write(f'{name}\n')


# # Assign UUIDs to UFIB (Pilot Study) Images

# In[7]:


import uuid
from pathlib import Path
import numpy as np
import pandas as pd


# In[21]:


DATA_DIR = Path('..') / 'data'
PILOT_IN = DATA_DIR / 'raw' / 'pilot' / 'UFBI_identifiers_photos.csv'
PILOT_OUT = DATA_DIR / 'interim' / 'UFBI_identifiers_photos'


# In[22]:


pilot = pd.read_csv(PILOT_IN)
pilot['sample_id'] = np.NaN
pilot.sample_id = pilot.File.map(lambda x: uuid.uuid4())
pilot.head()


# In[23]:


# pilot.to_csv(PILOT_OUT, index=False)


# # Picogreen Yields

# In[24]:


get_ipython().run_line_magic('matplotlib', 'inline')

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import lib.db as db


# In[25]:


cnx = db.connect()

pico = pd.read_sql('SELECT * FROM picogreen', cnx)
pico.head()


# In[26]:


has_id = pico['picogreen_id'].str.match(r'\d\d_\d\d')
has_yield = pd.to_numeric(pico['ng_microliter_mean'], errors='coerce').notna()

yields = pd.to_numeric(pico[has_id & has_yield]['ng_microliter_mean'])
yields.head()


# In[27]:


bins = 50

plt.subplots(figsize=(10, 10))
plt.title(f'Number of extracts with yield ({yields.count()} total, {bins} bin)')
plt.xlabel(r'Mean yields (ng/$\mu$l)')
plt.ylabel('Count')
plt.grid(True)
plt.hist(yields, bins=bins, rwidth=0.9)
plt.show()


# # Scan MOBOT Data Dump
# 
# Scan the MOBOT data dump for matches in the database.

# In[ ]:


import csv
import sys
from pathlib import Path
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
from lib.dict_attr import DictAttrs
import lib.db as db


# In[ ]:


DATA_DIR = Path('.') / 'data'
MOBOT_DIR = DATA_DIR / 'dwca-tropicosspecimens-v1.17'
CONTINENTS_FILE = DATA_DIR / 'Continents'
OCCURRENCE = MOBOT_DIR / 'occurrence.txt'
IN_AFRICA = DATA_DIR / 'mobot_in_africa.txt'
INTERSECTION = DATA_DIR / 'mobot_nitfix_africa.txt'
COUNTRY_CODES = [
    'DZ', 'AO', 'SH', 'BJ', 'BW', 'BF', 'BI', 'CM', 'CV', 'CF', 'TD', 'KM',
    'CG', 'CD', 'DJ', 'EG', 'GQ', 'ER', 'ET', 'GA', 'GM', 'GH', 'GN', 'GW',
    'CI', 'KE', 'LS', 'LR', 'LY', 'MG', 'MW', 'ML', 'MR', 'MU', 'YT', 'MA',
    'MZ', 'NA', 'NE', 'NG', 'ST', 'RE', 'RW', 'ST', 'SN', 'SC', 'SL', 'SO',
    'ZA', 'SS', 'SH', 'SD', 'SZ', 'TZ', 'TG', 'TN', 'UG', 'CD', 'ZM', 'TZ',
    'ZW'
]
CONTINENTS = gpd.read_file(str(CONTINENTS_FILE))
AFRICA = CONTINENTS.iloc[3, 1]


# In[ ]:


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


# In[ ]:


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


# In[ ]:


def inside_africa(row):
    """Test if the occurrence record is for Africa."""
    if row.countryCode in COUNTRY_CODES:
        return True

    if row.continent.lower() == 'africa':
        return True

    if row.decimalLatitude and row.decimalLongitude:
        point = Point(float(row.decimalLatitude), float(row.decimalLongitude))
        if AFRICA.contains(point):
            return True

    return False


# In[ ]:


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


# In[ ]:


def scan_mobot():
    """Run the script."""
    csv.field_size_limit(sys.maxsize)
    # mobot_data()
    mobot_in_africa()
    nitfix_in_africa()


# In[ ]:


scan_mobot()


# # Validate DB

# In[1]:


from os.path import join
import uuid
import sqlite3


# In[ ]:


def audit():
    """Check the database."""
    select = 'SELECT * FROM images'

    db_path = join('data', 'nitfix.sqlite.db')
    with sqlite3.connect(db_path) as cxn:
        cursor = cxn.cursor()
        cursor.execute(select)

        for row in cursor.fetchall():
            guid = row[0]
            try:
                uuid.UUID(guid)
            except ValueError:
                print(f'Bad UUID "{guid}"')  # noqa


# In[ ]:


audit()


# # UUIDs as Base-64

# In[ ]:


import uuid
import base64

for _ in range(10):
    guid = uuid.uuid4()
    short = base64.b64encode(guid.bytes).decode('utf-8')
    print(guid, '\t', short)

