"""SQL functions."""

from pathlib import Path
import sqlite3
import lib.util as util
from lib.dict_attr import DictAttrs


def attr_factory(cursor, row):
    """Turn the rows into pseudo-data classes.attr_factory."""
    data = DictAttrs({})
    for i, col in enumerate(cursor.description):
        data[col[0]] = row[i]
    return data


def connect(factory=None):
    """Connect to the SQLite3 DB."""
    if not factory:
        factory = sqlite3.Row

    db_path = str(Path('..') / 'data' / 'processed' / 'nitfix.sqlite.db')
    cxn = sqlite3.connect(db_path)

    cxn.execute("PRAGMA page_size = {}".format(2**16))
    cxn.execute("PRAGMA busy_timeout = 10000")
    cxn.execute("PRAGMA journal_mode = OFF")
    cxn.execute("PRAGMA synchronous = OFF")
    cxn.execute("PRAGMA optimize")

    cxn.create_function('IS_UUID', 1, util.is_uuid)
    cxn.row_factory = factory
    return cxn


def get_image(cxn, sample_ids):
    """Get an image by its primary key."""
    sample_ids = ', '.join(
        ["'{}'".format(i.strip()) for i in sample_ids.split(';')])
    sql = """SELECT * FROM images WHERE sample_id IN ({})""".format(sample_ids)
    result = cxn.execute(sql)
    return result.fetchall()


def get_images(cxn):
    """Get an image by its primary key."""
    sql = """SELECT * FROM images"""
    result = cxn.execute(sql)
    return result.fetchall()


def get_taxon_by_provider(cxn, provider_acronym, provider_id):
    """Get a taxon record using the provider ID."""
    sql = """
            SELECT *
              FROM taxons
             WHERE provider_acronym = ?
               AND provider_id = ?
        """
    result = cxn.execute(sql, (provider_acronym, provider_id))
    return result.fetchone()


def get_taxons(cxn):
    """Get taxons."""
    sql = """SELECT * FROM taxons"""
    return cxn.execute(sql)


def get_taxon_names(cxn):
    """Get taxons where the tissue sample ID is a valid UUID."""
    sql = """SELECT DISTINCT scientific_name FROM taxons"""
    return cxn.execute(sql)


def get_images_taxons(cxn, file_pattern):
    """Get images joind with their matching taxons."""
    sql = """
            SELECT images.*, taxons.*
              FROM images
              JOIN taxons USING (sample_id)
              WHERE file_name LIKE ?
           ORDER BY images.file_name
        """
    return cxn.execute(sql, (file_pattern, ))


def get_taxon_by_sample_id(cxn, file_id):
    """Look for a taxon with the given file ID."""
    pattern = '%{}%'.format(file_id)
    sql = """SELECT * FROM taxons WHERE sample_id LIKE ?"""
    result = cxn.execute(sql, (pattern, ))
    return result.fetchall()


def create_uuids_table(cxn):
    """Create a table for the UUIDs."""
    cxn.execute('DROP TABLE IF EXISTS uuids')
    cxn.execute("""CREATE TABLE uuids (uuid TEXT NOT NULL)""")
    cxn.execute("""CREATE INDEX uuids_idx ON uuids (uuid)""")


def insert_uuid_batch(cxn, batch):
    """Insert a batch of UUIDs."""
    if batch:
        sql = 'INSERT INTO uuids (uuid) VALUES (?)'
        cxn.executemany(sql, batch)
        cxn.commit()


def select_plates(cxn):
    """Get plate data from the plates table."""
    sql = """
        SELECT DISTINCT plate_id, entry_date, local_id, protocol, notes
          FROM plates
      ORDER BY entry_date, plate_id
        """
    return cxn.execute(sql)


def select_plate_wells(cxn):
    """Get plate data from the plates table."""
    sql = """SELECT * FROM plates"""
    return cxn.execute(sql)


def get_plate_report(cxn, plate_id):
    """Get data for a plate."""
    sql = """
        SELECT *,
               plate_row || substr('00' || plate_col, -2) AS plate_well
          FROM plates
          JOIN taxons USING (sample_id)
     LEFT JOIN picogreen USING (sample_id)
         WHERE plate_id = ?
        ORDER BY plate_row, plate_col
        """
    return cxn.execute(sql, (plate_id, ))


def create_picogreen_table(cxn):
    """Create a table to hold data from the picogreen Google sheet."""
    cxn.execute('DROP TABLE IF EXISTS picogreen')
    cxn.execute("""
        CREATE TABLE picogreen (
            picogreen_id       TEXT NOT NULL,
            well               TEXT,
            rfu                TEXT,
            ng_microliter      NUMBER,
            ng_microliter_mean NUMBER,
            quant_method       TEXT,
            quant_date         TEXT,
            sample_id          TEXT)
        """)
    cxn.execute("""
        CREATE INDEX picogreen_idx ON picogreen (picogreen_id)""")
    cxn.execute("""
        CREATE INDEX picogreen_sample_id ON picogreen (sample_id)""")


def create_loci_table(cxn):
    """Create a table to hold the loci CSV file."""
    cxn.execute('DROP TABLE IF EXISTS loci')
    cxn.execute("""
        CREATE TABLE loci (
            scientific_name TEXT NOT NULL,
            ITS             INTEGER,
            atpB            INTEGER,
            matK            INTEGER,
            matR            INTEGER,
            rbcL            INTEGER)
        """)
    cxn.execute("""CREATE INDEX loci_idx ON loci (scientific_name)""")


def insert_loci_batch(cxn, batch):
    """Insert a sample IDs into the plates table."""
    sql = """
        INSERT INTO picogreen (
            scientific_name,
            ITS,
            atpB,
            matK,
            matR,
            rbcL)
            VALUES (?, ?, ?, ?, ?, ?)
        """
    cxn.executemany(sql, batch)
    cxn.commit()
