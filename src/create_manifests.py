
# coding: utf-8

# <h1>Table of Contents<span class="tocSkip"></span></h1>
# <div class="toc"><ul class="toc-item"><li><span><a href="#Setup" data-toc-modified-id="Setup-1"><span class="toc-item-num">1&nbsp;&nbsp;</span>Setup</a></span></li><li><span><a href="#CalAcademy-Manifest" data-toc-modified-id="CalAcademy-Manifest-2"><span class="toc-item-num">2&nbsp;&nbsp;</span>CalAcademy Manifest</a></span></li><li><span><a href="#MOBOT-Manifest-(Old)" data-toc-modified-id="MOBOT-Manifest-(Old)-3"><span class="toc-item-num">3&nbsp;&nbsp;</span>MOBOT Manifest (Old)</a></span></li></ul></div>

# # Setup

# In[2]:


from os.path import basename
from pathlib import Path

import pandas as pd

import lib.db as db
import lib.util as util


# In[3]:


CXN = db.connect()
INTERIM_DATA = Path('..') / 'data' / 'interim'


# # CalAcademy Manifest

# In[15]:


sql = """
    SELECT image_file, raw_images.sample_id, scientific_name FROM raw_images
      JOIN taxon_ids ON (taxon_ids.id = raw_images.sample_id)
     WHERE image_file LIKE '%/CAS-DOE-nitfix_specimen_photos/%'
"""
images = pd.read_sql(sql, CXN)

images.image_file = images.image_file.str.extract(r'.*/(.*)', expand=False)

print(len(images))
images.head()


# In[17]:


sql = """
    SELECT image_file FROM image_errors
     WHERE image_file LIKE '%/CAS-DOE-nitfix_specimen_photos/%'
"""
errors = pd.read_sql(sql, CXN)
errors.image_file = errors.image_file.str.extract(r'.*/(.*)', expand=False)

print(len(errors))
errors


# In[18]:


images.to_csv(INTERIM_DATA / 'cas_manifest.csv', index=False)
errors.to_csv(INTERIM_DATA / 'cas_manifest_missing.csv', index=False)


# # MOBOT Manifest (Old)

# In[17]:


taxonomy = pd.read_sql('SELECT * FROM taxons', CXN)

sql = """SELECT *
           FROM images
          WHERE file_name LIKE '%/MO-DOE-nitfix_specimen_photos/%'"""

images = pd.read_sql(sql, CXN)

taxons = {}
for key, taxon in taxonomy.iterrows():
    guids = util.split_uuids(taxon.sample_ids)
    for guid in guids:
        taxons[guid] = taxon.scientific_name


# In[20]:


for key, image in images.iterrows():
    images.loc[key, 'resolved_name'] = taxons.get(image.sample_id)

images.head()


# In[33]:


images.file_name = images.file_name.apply(basename)
print(len(images))
images.head()


# In[31]:


csv_path = INTERIM_DATA / 'mobot_manifest.csv'
images.to_csv(csv_path, index=False)


# In[32]:


missing = images.resolved_name.isna()
missing_images = images[missing]
len(missing_images)

