"""SQL functions."""

import uuid
from pathlib import Path
import sqlite3
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

    db_conn.create_function('IS_UUID', 1, is_uuid)
    db_conn.row_factory = factory
    return db_conn


def is_uuid(guid):
    """Create a function to determine if a string is a valid UUID."""
    try:
        uuid.UUID(guid)
        return True
    except ValueError:
        return False


def create_images_table(db_conn):
    """Create images table and index."""
    db_conn.execute('DROP TABLE IF EXISTS images')
    db_conn.execute("""
                    CREATE TABLE images (
                        image_id      TEXT PRIMARY KEY NOT NULL,
                        file_name     TEXT NOT NULL UNIQUE,
                        image_created TEXT
                    )""")


def insert_image(db_conn, guid, file_name, image_created):
    """Insert a record into the images table."""
    sql = """
        INSERT INTO images (image_id, file_name, image_created)
             VALUES (?, ?, ?)
        """
    db_conn.execute(sql, (guid, file_name, image_created))
    db_conn.commit()


def get_image(db_conn, image_ids):
    """Get an image by its primary key."""
    image_ids = ', '.join(
        ["'{}'".format(i.strip()) for i in image_ids.split(';')])
    sql = """SELECT * FROM images WHERE image_id IN ({})""".format(image_ids)
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
    db_conn.execute('CREATE INDEX error_idx ON errors(error_key)')


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


def create_taxonomies_table(db_conn):
    """Create a table from the Google sheet."""
    db_conn.execute('DROP TABLE IF EXISTS taxonomies')
    db_conn.execute("""
        CREATE TABLE taxonomies (
            taxonomy_key        TEXT NOT NULL,
            family              TEXT,
            scientific_name     TEXT,
            taxonomic_authority TEXT,
            synonyms            TEXT,
            sample_id           TEXT,
            provider_acronym    TEXT,
            provider_id         TEXT,
            quality_notes       TEXT,
            genus               TEXT)
        """)
    db_conn.execute("""CREATE UNIQUE INDEX taxonomies_key
                        ON taxonomies (taxonomy_key)""")
    db_conn.execute("""CREATE INDEX taxonomies_provider_acronym
                        ON taxonomies (provider_acronym)""")
    db_conn.execute("""CREATE INDEX taxonomies_provider_id
                        ON taxonomies (provider_id)""")
    db_conn.execute("""CREATE INDEX taxonomies_sample_id
                        ON taxonomies (sample_id)""")
    db_conn.execute("""CREATE INDEX taxonomies_family
                        ON taxonomies (family)""")
    db_conn.execute("""CREATE INDEX taxonomies_genus
                        ON taxonomies (genus)""")


def insert_taxonomy(db_conn, values):
    """Insert a record into the taxonomies table."""
    sql = """
        INSERT INTO taxonomies (
                taxonomy_key,
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


def get_taxonomy_by_provider(db_conn, provider_acronym, provider_id):
    """Get a taxonomy record using the provider ID."""
    sql = """
            SELECT *
              FROM taxonomies
             WHERE provider_acronym = ?
               AND provider_id = ?
        """
    result = db_conn.execute(sql, (provider_acronym, provider_id))
    return result.fetchone()


def get_taxonomies(db_conn):
    """Get taxonomies where the tissue sample ID is a valid UUID."""
    sql = """SELECT * FROM taxonomies WHERE IS_UUID(sample_id)"""
    return db_conn.execute(sql)


def get_taxonomy_names(db_conn):
    """Get taxonomies where the tissue sample ID is a valid UUID."""
    sql = """SELECT DISTINCT scientific_name FROM taxonomies"""
    return db_conn.execute(sql)


def get_images_taxonomies(db_conn, file_pattern):
    """Get images joind with their matching taxonomies."""
    sql = """
            SELECT images.*, taxonomies.*
              FROM images
              JOIN taxonomies ON images.image_id = taxonomies.sample_id
              WHERE file_name LIKE ?
           ORDER BY images.file_name
        """
    return db_conn.execute(sql, (file_pattern, ))


def get_taxonomy_by_image_id(db_conn, file_id):
    """Look for a taxonomy with the given file ID."""
    pattern = '%{}%'.format(file_id)
    sql = """SELECT * FROM taxonomies WHERE sample_id LIKE ?"""
    result = db_conn.execute(sql, (pattern, ))
    return result.fetchall()


def old_taxonomy_image_mismatches(db_conn):
    """Get taxonomies and images set difference."""
    sql = """
          WITH taxos AS (
            SELECT * FROM taxonomies WHERE IS_UUID(sample_id))
        SELECT images.*, taxos.*
          FROM images
     LEFT JOIN taxos ON images.image_id = taxos.sample_id
         WHERE taxos.sample_id IS NULL
     UNION
        SELECT images.*, taxos.*
          FROM taxos
     LEFT JOIN images ON images.image_id = taxos.sample_id
         WHERE images.image_id IS NULL
      ORDER BY taxos.scientific_name, images.file_name
        """
    return db_conn.execute(sql)


def create_uuids_table(db_conn):
    """Create a table for the UUIDs."""
    db_conn.execute('DROP TABLE IF EXISTS uuids')
    db_conn.execute("""CREATE TABLE uuids (uuid TEXT PRIMARY KEY NOT NULL)""")


def insert_uuid_batch(db_conn, batch):
    """Insert a batch of UUIDs."""
    if batch:
        sql = 'INSERT INTO uuids (uuid) VALUES (?)'
        db_conn.executemany(sql, batch)
        db_conn.commit()


def create_sample_plates_table(db_conn):
    """Create a table to hold data from the sample_plates Google sheet."""
    db_conn.execute('DROP TABLE IF EXISTS sample_plates')
    db_conn.execute("""
        CREATE TABLE sample_plates (
            plate_id   TEXT NOT NULL,
            entry_date TEXT,
            local_id   TEXT,
            protocol   TEXT,
            notes      TEXT,
            plate_row  TEXT NOT NULL,
            plate_col  INTEGER NOT NULL,
            sample_id  TEXT NOT NULL)
        """)
    db_conn.execute('CREATE INDEX plate_samples ON sample_plates (sample_id)')


def insert_sample_plates(db_conn, values):
    """Insert a sample IDs into the sample_plates table."""
    sql = """
        INSERT INTO sample_plates (
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
    """Get plate data from the sample_plates table."""
    sql = """
        SELECT DISTINCT plate_id, entry_date, local_id, protocol, notes
          FROM sample_plates
      ORDER BY entry_date, plate_id
        """
    return db_conn.execute(sql)


def select_plate_wells(db_conn):
    """Get plate data from the sample_plates table."""
    sql = """SELECT * FROM sample_plates"""
    return db_conn.execute(sql)


def get_plate_report(db_conn, plate_id):
    """Get data for a plate."""
    sql = """
        SELECT *,
               plate_row || substr('00' || plate_col, -2) AS plate_well
          FROM sample_plates
          JOIN taxonomies USING (sample_id)
     LEFT JOIN picogreen USING (sample_id)
         WHERE plate_id = ?
        ORDER BY plate_row, plate_col
        """
    return db_conn.execute(sql, (plate_id, ))


def samples_not_in_taxonomies(db_conn):
    """Get the plate samples that are not in the master taxonomy."""
    sql = """
        SELECT *
          FROM sample_plates
         WHERE sample_id NOT IN (SELECT sample_id FROM taxonomies)
        """
    return db_conn.execute(sql)


def family_genus_coverage(db_conn):
    """Get the sample plates' family coverage."""
    sql = """
        SELECT family,
                 '' AS genus,
                 COUNT(*) AS total,
                 COALESCE(plated, 0) AS plated,
                 ROUND(100.0 * COALESCE(plated, 0) / COUNT(*), 2) AS percent
          FROM taxonomies
          LEFT JOIN
            (SELECT family, COUNT(*) AS plated
             FROM sample_plates
             JOIN taxonomies USING (sample_id)
             GROUP BY family) USING (family)
          GROUP BY family
        UNION ALL
          SELECT family,
                 genus,
                 COUNT(*) AS total,
                 COALESCE(plated, 0) AS plated,
                 ROUND(100.0 * COALESCE(plated, 0) / COUNT(*), 2) AS percent
          FROM taxonomies
          LEFT JOIN
            (SELECT family, genus, COUNT(*) AS plated
             FROM sample_plates
             JOIN taxonomies USING (sample_id)
             GROUP BY family, genus) USING (family, genus)
          GROUP BY family, genus
        ORDER BY family, genus
        """
    return db_conn.execute(sql)


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
    db_conn.execute('CREATE INDEX picogreen_id ON picogreen (picogreen_id)')
    db_conn.execute('CREATE INDEX picogreen_samples ON picogreen (sample_id)')


def insert_picogreen_batch(db_conn, batch):
    """Insert a sample IDs into the sample_plates table."""
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
