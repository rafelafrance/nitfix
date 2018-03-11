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

    db_path = str(Path('data') / 'processed' / 'nitfix.sqlite.db')
    db_conn = sqlite3.connect(db_path)

    db_conn.execute("PRAGMA page_size = {}".format(2**16))
    db_conn.execute("PRAGMA busy_timeout = 10000")
    db_conn.execute("PRAGMA journal_mode = OFF")
    db_conn.execute("PRAGMA synchronous = OFF")
    db_conn.execute("PRAGMA optimize")

    db_conn.create_function('IS_UUID', 1, util.is_uuid)
    db_conn.row_factory = factory
    return db_conn


def create_images_table(db_conn):
    """Create images table and index."""
    db_conn.execute('DROP TABLE IF EXISTS images')
    db_conn.execute("""
        CREATE TABLE images (
            sample_id     TEXT PRIMARY KEY NOT NULL,
            file_name     TEXT NOT NULL UNIQUE,
            image_created TEXT
        )""")
    db_conn.execute("""CREATE INDEX image_idx ON images(sample_id)""")


def insert_image(db_conn, guid, file_name, image_created):
    """Insert a record into the images table."""
    sql = """
        INSERT INTO images (sample_id, file_name, image_created)
             VALUES (?, ?, ?)
        """
    db_conn.execute(sql, (guid, file_name, image_created))
    db_conn.commit()


def get_image(db_conn, sample_ids):
    """Get an image by its primary key."""
    sample_ids = ', '.join(
        ["'{}'".format(i.strip()) for i in sample_ids.split(';')])
    sql = """SELECT * FROM images WHERE sample_id IN ({})""".format(sample_ids)
    result = db_conn.execute(sql)
    return result.fetchall()


def get_images(db_conn):
    """Get an image by its primary key."""
    sql = """SELECT * FROM images"""
    result = db_conn.execute(sql)
    return result.fetchall()


def create_errors_table(db_conn):
    """Create errors table for persisting errors."""
    db_conn.execute('DROP TABLE IF EXISTS errors')
    db_conn.execute("""
        CREATE TABLE errors (
            error_key   TEXT NOT NULL,
            msg         TEXT,
            resolution  TEXT
        )""")
    db_conn.execute("""CREATE INDEX error_idx ON errors(error_key)""")


def insert_error(db_conn, error_key, msg):
    """Insert a record into the errors table."""
    sql = """INSERT INTO errors (error_key, msg) VALUES (?, ?)"""
    db_conn.execute(sql, (error_key, msg))
    db_conn.commit()


def resolve_error(db_conn, error_key, resolution):
    """Resolve an error."""
    sql = """UPDATE errors SET resolution = ? WHERE error_key = ?"""
    db_conn.execute(sql, (resolution, error_key))
    db_conn.commit()


def create_taxons_table(db_conn):
    """Create a table from the Google sheet."""
    db_conn.execute('DROP TABLE IF EXISTS taxons')
    db_conn.execute("""
        CREATE TABLE taxons (
            taxon_key        TEXT NOT NULL,
            family           TEXT,
            scientific_name  TEXT,
            authority        TEXT,
            synonyms         TEXT,
            sample_id        TEXT,
            provider_acronym TEXT,
            provider_id      TEXT,
            quality_notes    TEXT,
            genus            TEXT)
        """)
    db_conn.execute("""CREATE UNIQUE INDEX taxons_key
                        ON taxons (taxon_key)""")
    db_conn.execute("""CREATE INDEX taxons_provider_acronym
                        ON taxons (provider_acronym)""")
    db_conn.execute("""CREATE INDEX taxons_provider_id
                        ON taxons (provider_id)""")
    db_conn.execute("""CREATE INDEX taxons_sample_id
                        ON taxons (sample_id)""")
    db_conn.execute("""CREATE INDEX taxons_family
                        ON taxons (family)""")
    db_conn.execute("""CREATE INDEX taxons_genus
                        ON taxons (genus)""")


def insert_taxon_batch(db_conn, values):
    """Insert a batch of records into the taxons table."""
    sql = """
        INSERT INTO taxons (
                taxon,
                family,
                scientific_name,
                taxonomic_authority,
                synonyms,
                sample_id,
                provider_acronym,
                provider_id,
                quality_notes,
                genus)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    db_conn.executemany(sql, values)
    db_conn.commit()


def get_taxon_by_provider(db_conn, provider_acronym, provider_id):
    """Get a taxon record using the provider ID."""
    sql = """
            SELECT *
              FROM taxons
             WHERE provider_acronym = ?
               AND provider_id = ?
        """
    result = db_conn.execute(sql, (provider_acronym, provider_id))
    return result.fetchone()


def get_taxons(db_conn):
    """Get taxons."""
    sql = """SELECT * FROM taxons"""
    return db_conn.execute(sql)


def get_taxon_names(db_conn):
    """Get taxons where the tissue sample ID is a valid UUID."""
    sql = """SELECT DISTINCT scientific_name FROM taxons"""
    return db_conn.execute(sql)


def get_images_taxons(db_conn, file_pattern):
    """Get images joind with their matching taxons."""
    sql = """
            SELECT images.*, taxons.*
              FROM images
              JOIN taxons USING (sample_id)
              WHERE file_name LIKE ?
           ORDER BY images.file_name
        """
    return db_conn.execute(sql, (file_pattern, ))


def get_taxon_by_sample_id(db_conn, file_id):
    """Look for a taxon with the given file ID."""
    pattern = '%{}%'.format(file_id)
    sql = """SELECT * FROM taxons WHERE sample_id LIKE ?"""
    result = db_conn.execute(sql, (pattern, ))
    return result.fetchall()


def create_uuids_table(db_conn):
    """Create a table for the UUIDs."""
    db_conn.execute('DROP TABLE IF EXISTS uuids')
    db_conn.execute("""CREATE TABLE uuids (uuid TEXT NOT NULL)""")
    db_conn.execute("""CREATE INDEX uuids_idx ON uuids (uuid)""")


def insert_uuid_batch(db_conn, batch):
    """Insert a batch of UUIDs."""
    if batch:
        sql = 'INSERT INTO uuids (uuid) VALUES (?)'
        db_conn.executemany(sql, batch)
        db_conn.commit()


def create_plates_table(db_conn):
    """Create a table to hold data from the sample_plates Google sheet."""
    db_conn.execute('DROP TABLE IF EXISTS plates')
    db_conn.execute("""
        CREATE TABLE plates (
            plate_id   TEXT NOT NULL,
            entry_date TEXT,
            local_id   TEXT,
            protocol   TEXT,
            notes      TEXT,
            plate_row  TEXT NOT NULL,
            plate_col  INTEGER NOT NULL,
            sample_id  TEXT NOT NULL)
        """)
    db_conn.execute("""CREATE INDEX plate_idx ON plates (plate_id)""")
    db_conn.execute("""CREATE INDEX plate_sample_ids ON plates (sample_id)""")


def insert_plates(db_conn, values):
    """Insert a sample IDs into the plates table."""
    sql = """
        INSERT INTO plates (
                plate_id,
                entry_date,
                local_id,
                protocol,
                notes,
                plate_row,
                plate_col,
                sample_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
    db_conn.executemany(sql, values)
    db_conn.commit()


def select_plates(db_conn):
    """Get plate data from the plates table."""
    sql = """
        SELECT DISTINCT plate_id, entry_date, local_id, protocol, notes
          FROM plates
      ORDER BY entry_date, plate_id
        """
    return db_conn.execute(sql)


def select_plate_wells(db_conn):
    """Get plate data from the plates table."""
    sql = """SELECT * FROM plates"""
    return db_conn.execute(sql)


def get_plate_report(db_conn, plate_id):
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
    return db_conn.execute(sql, (plate_id, ))


# def samples_not_in_taxons(db_conn):
#     """Get the plate samples that are not in the master taxon."""
#     sql = """
#         SELECT *
#           FROM plates
#          WHERE sample_id NOT IN (SELECT sample_id FROM taxons)
#         """
#     return db_conn.execute(sql)
#
#
# def family_genus_coverage(db_conn):
#     """Get the sample plates' family coverage."""
#     sql = """
#         WITH taxons AS (SELECT family, genus, COUNT(*) AS total
#                           FROM taxons
#                       GROUP BY family, genus),
#            pictures AS (SELECT family, genus, COUNT(*) AS imaged
#                           FROM taxons
#                           JOIN images USING (sample_id)
#                       GROUP BY family, genus)
#         SELECT family, genus, total, COALESCE(imaged, 0) as imaged,
#                ROUND(100.0 * COALESCE(imaged, 0) / total, 2) AS percent
#           FROM taxons
#      LEFT JOIN pictures USING (family, genus)
#     UNION
#         SELECT family, '', SUM(total), SUM(COALESCE(imaged, 0)),
#                ROUND(100.0 * SUM(COALESCE(imaged, 0)) / SUM(total), 2)
#           FROM taxons
#      LEFT JOIN pictures USING (family, genus)
#       GROUP BY family
#     UNION
#         SELECT '~Total~', '', SUM(total), SUM(COALESCE(imaged, 0)),
#                ROUND(100.0 * SUM(COALESCE(imaged, 0)) / SUM(total), 2)
#           FROM taxons
#      LEFT JOIN pictures USING (family, genus)
#         ORDER BY family, genus
#         """
#     return db_conn.execute(sql)


def create_picogreen_table(db_conn):
    """Create a table to hold data from the picogreen Google sheet."""
    db_conn.execute('DROP TABLE IF EXISTS picogreen')
    db_conn.execute("""
        CREATE TABLE picogreen (
            picogreen_id       TEXT NOT NULL,
            well               TEXT,
            rfu                TEXT,
            ng_microliter      NUMBER,
            ng_microliter_mean TEXT,
            quant_method       TEXT,
            quant_date         TEXT,
            sample_id          TEXT)
        """)
    db_conn.execute("""
        CREATE INDEX picogreen_idx ON picogreen (picogreen_id)""")
    db_conn.execute("""
        CREATE INDEX picogreen_sample_id ON picogreen (sample_id)""")


def insert_picogreen_batch(db_conn, batch):
    """Insert a sample IDs into the plates table."""
    sql = """
        INSERT INTO picogreen (
            picogreen_id,
            well,
            rfu,
            ng_microliter,
            ng_microliter_mean,
            quant_method,
            quant_date,
            sample_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
    db_conn.executemany(sql, batch)
    db_conn.commit()
