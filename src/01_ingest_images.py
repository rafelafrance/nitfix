"""Extract, transform, and load data related to the images."""


from glob import glob
from pathlib import Path
from collections import namedtuple
import pandas as pd
from PIL import Image, ImageFilter
import zbarlight
from tqdm import tqdm
import lib.db as db
import lib.google as google

Dimensions = namedtuple('Dimensions', 'width height')

CXN = db.connect()
RAW_DATA = Path('..') / 'data' / 'raw'
INTERIM_DATA = Path('..') / 'data' / 'interim'
PROCESSED_DATA = Path('..') / 'data' / 'processed'


def create_images_table():
    """Create images and errors tables."""
    CXN.execute('DROP TABLE IF EXISTS raw_images')
    CXN.execute("""
        CREATE TABLE raw_images (
            sample_id  TEXT PRIMARY KEY NOT NULL,
            image_file TEXT NOT NULL UNIQUE
        )""")
    CXN.execute("""CREATE INDEX image_idx ON raw_images (sample_id)""")
    CXN.execute("""CREATE INDEX image_file ON raw_images (image_file)""")


def create_errors_table():
    """Create errors table for persisting errors."""
    CXN.execute('DROP TABLE IF EXISTS image_errors')
    CXN.execute("""
        CREATE TABLE errors (
            image_file TEXT NOT NULL,
            msg        TEXT,
            ok         INTEGER,
            resolution TEXT
        )""")
    CXN.execute("""CREATE INDEX error_idx ON image_errors (image_file)""")


# In[5]:


# create_images_table()
# create_errors_table()


# # Get Files Already Read

# In[6]:


images = pd.read_sql('SELECT * FROM raw_images', CXN)
errors = pd.read_sql('SELECT * FROM image_errors', CXN)
skip_images = set(images.image_file) | set(errors.image_file)


# # Functions
#
# ## Search the image for the sample ID (UUID)
#

# In[7]:


def window_slider(image_size, window=None, stride=None):
    """
    Create slider window.

    It helps with feature extraction by limiting the search area.
    """
    window = window if window else Dimensions(400, 400)
    stride = stride if stride else Dimensions(200, 200)

    for top in range(0, image_size.height, stride.height):
        bottom = top + window.height
        bottom = image_size.height if bottom > image_size.height else bottom

        for left in range(0, image_size.width, stride.width):
            right = left + window.width
            right = image_size.width if right > image_size.width else right

            box = (left, top, right, bottom)

            yield box


def get_qr_code(image):
    """Extract QR code from image."""
    # Try a direct extraction
    qr_code = zbarlight.scan_codes('qrcode', image)
    if qr_code:
        return qr_code[0].decode('utf-8')

    # Try a slider
    for box in window_slider(image):
        cropped = image.crop(box)
        qr_code = zbarlight.scan_codes('qrcode', cropped)
        if qr_code:
            return qr_code[0].decode('utf-8')

    # Try rotating the image *sigh*
    for degrees in range(5, 85, 5):
        rotated = image.rotate(degrees)
        qr_code = zbarlight.scan_codes('qrcode', rotated)
        if qr_code:
            return qr_code[0].decode('utf-8')

    # Try to sharpen the image
    sharpened = image.filter(ImageFilter.SHARPEN)
    qr_code = zbarlight.scan_codes('qrcode', sharpened)
    if qr_code:
        return qr_code[0].decode('utf-8')

    return None


def get_image_data(image_file):
    """Read and process image."""
    with open(image_file, 'rb') as image_file:
        # exif = exifread.process_file(image_file)
        image = Image.open(image_file)
        image.load()

    qr_code = get_qr_code(image)

    return qr_code


def ingest_images(dir_name, skip_images):
    """Ingest image batch."""
    pattern = str(dir_name / '*.JPG')

    sample_ids = {}  # Keep track of already used sample_ids

    images = []  # A batch of images to insert
    errors = []  # A batch of errors to insert

    files = sorted(glob(pattern))

    for image_file in tqdm(files):
        if image_file in skip_images:
            continue

        sample_id = get_image_data(image_file)

        # Handle a missing sample ID
        if not sample_id:
            msg = 'MISSING: QR code missing in {}'.format(image_file)
            errors.append((image_file, msg))

        # Handle a duplicate sample ID
        elif sample_ids.get(sample_id):
            msg = ('DUPLICATES: Files {} and {} have the same '
                   'QR code').format(sample_ids[sample_id], image_file)
            errors.append((image_file, msg))

        # The image seems OK
        else:
            sample_ids[sample_id] = image_file
            images.append((sample_id, image_file))

    # Insert the image and error batches
    if images:
        sql = 'INSERT INTO raw_images (sample_id, image_file) VALUES (?, ?)'
        CXN.executemany(sql, images)
        CXN.commit()

    if errors:
        sql = 'INSERT INTO image_errors (image_file, msg) VALUES (?, ?)'
        CXN.executemany(sql, errors)
        CXN.commit()


def resolve_error(dir_name, image_file, ok, resolution):
    """Resolve an error."""
    image_file = str(dir_name / f'{image_file}.JPG')
    sql = """
        UPDATE image_errors
           SET ok = ?, resolution = ?
         WHERE image_file = ?"""
    CXN.execute(sql, (ok, resolution, image_file))
    CXN.commit()


def manual_insert(dir_name, image_file, sample_id, skip_images):
    """Manually set an image record."""
    image_file = str(dir_name / f'{image_file}.JPG')
    if image_file in skip_images:
        return
    sql = 'INSERT INTO raw_images (sample_id, image_file) VALUES (?, ?)'
    CXN.execute(sql, (sample_id, image_file))
    CXN.commit()


# # Read image Files
#
# ## Read New York Botanical Garden (1st trip)

# In[13]:


path = RAW_DATA / 'DOE-nitfix_specimen_photos'

ingest_images(path, skip_images)

resolve_error(path, 'R0000149', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0000151', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0000158', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0000165', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0000674', 1, 'OK: Is a duplicate of R0000473')
resolve_error(path, 'R0000835', 1, 'OK: Is a duplicate of R0000836')
resolve_error(path, 'R0000895', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0000937', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0001055', 1, 'OK: Genuine duplicate')


# ## Read Harvard Herbaria

# In[14]:


path = RAW_DATA / 'HUH_DOE-nitfix_specimen_photos'

ingest_images(path, skip_images)

resolve_error(path, 'R0001262', 1, 'OK: Is a duplicate of R0001263')
resolve_error(path, 'R0001729', 1, 'OK: Is a duplicate of R0001728')


# ## Read Ohio State University Herbarium

# In[15]:


path = RAW_DATA / 'OS_DOE-nitfix_specimen_photos'

ingest_images(path, skip_images)

resolve_error(path, 'R0000229', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0001835', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0001898', 1, 'OK: Genuine duplicate')


# ## Read California Academy of Sciences Herbarium

# In[16]:


path = RAW_DATA / 'CAS-DOE-nitfix_specimen_photos'

ingest_images(path, skip_images)

resolve_error(path, 'R0001361', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0002349', 1, 'OK: Genuine duplicate')


# ## Read Missouri Botanical Garden

# In[17]:


path = RAW_DATA / 'MO-DOE-nitfix_specimen_photos'

ingest_images(path, skip_images)

resolve_error(path, 'R0002933', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0003226', 1, 'OK: Genuine duplicate')
resolve_error(path, 'R0003663', 1, 'OK: Manually fixed')
resolve_error(path, 'R0003509', 0, 'ERROR: Blurry image')

manual_insert(
    path, 'R0003663', '2eea159f-3c25-42ef-837d-27ad545a6779', skip_images)


# ## Read New York Botanical Garden (2nd trip)

# In[18]:


path = RAW_DATA / 'NY_visit_2'

ingest_images(path, skip_images)


# ## Read New York Botanical Garden (3rd trip)

# In[19]:


path = RAW_DATA / 'NY_DOE-nitfix_visit3'

ingest_images(path, skip_images)


# # Read Pilot Data

# In[20]:


csv_name = 'pilot.csv'
csv_path = INTERIM_DATA / csv_name

google.sheet_to_csv('UFBI_identifiers_photos', csv_path)

pilot = pd.read_csv(csv_path)

pilot['image_file'] = pilot['File'].apply(
    lambda x: f'../data/raw/UFBI_sample_photos/{x}')

pilot.drop(['File'], axis=1, inplace=True)
pilot.rename(columns={'Identifier': 'pilot_id'}, inplace=True)
pilot.pilot_id = pilot.pilot_id.str.lower().str.split().str.join(' ')

print(len(pilot))
pilot.head()


# In[21]:


name = 'raw_pilot'
pilot.to_csv(PROCESSED_DATA / f'{name}.csv', index=False)
pilot.to_sql(name, CXN, if_exists='replace', index=False)

already_in = pilot.sample_id.isin(images.sample_id)
pilot = pilot[~already_in]

pilot.drop('pilot_id', axis=1, inplace=True)
pilot.to_sql('raw_images', CXN, if_exists='append', index=False)


# # Read Corrales Data

# In[22]:


csv_name = 'pilot.csv'
csv_path = INTERIM_DATA / csv_name

google.sheet_to_csv('corrales_data', csv_path)

corrales = pd.read_csv(csv_path)
corrales.corrales_id = corrales.corrales_id.str.lower()
corrales.head()


# In[23]:


name = 'raw_corrales'
corrales.to_csv(PROCESSED_DATA / f'{name}.csv', index=False)
corrales.to_sql(name, CXN, if_exists='replace', index=False)

already_in = corrales.sample_id.isin(images.sample_id)
corrales = corrales[~already_in]

corrales.drop('corrales_id', axis=1, inplace=True)
corrales.to_sql('raw_images', CXN, if_exists='append', index=False)


# # Write Image Table to CSV File

# In[24]:


csv_name = 'images.csv'

df = pd.read_sql('SELECT * FROM raw_images', CXN)

csv_path = PROCESSED_DATA / csv_name
df.to_csv(csv_path, index=False)

df.head()


# In[25]:


csv_name = 'errors.csv'

df = pd.read_sql('SELECT * FROM image_errors', CXN)

csv_path = PROCESSED_DATA / csv_name
df.to_csv(csv_path, index=False)

df.head()
