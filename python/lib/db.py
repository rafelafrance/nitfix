"""Utilities for SQL functions."""

from os.path import join
import uuid
import sqlite3


def connect():
    """Connect to the SQLite3 DB."""
    db_path = join('data', 'nitfix.sqlite.db')
    db_conn = sqlite3.connect(db_path)

    db_conn.execute("PRAGMA page_size = {}".format(2**16))
    db_conn.execute("PRAGMA busy_timeout = 10000")
    db_conn.execute("PRAGMA journal_mode = OFF")
    db_conn.execute("PRAGMA synchronous = OFF")

    db_conn.create_function('IS_UUID', 1, is_uuid)
    db_conn.row_factory = sqlite3.Row
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
                        quality_notes       TEXT
                    )""")


def create_taxonomies_indexes(db_conn):
    """Create indexes for the taxonomies table."""
    db_conn.execute("""CREATE UNIQUE INDEX taxonomies_key
                        ON taxonomies (taxonomy_key)""")
    db_conn.execute("""CREATE INDEX taxonomies_provider_acronym
                        ON taxonomies (provider_acronym)""")
    db_conn.execute("""CREATE INDEX taxonomies_provider_id
                        ON taxonomies (provider_id)""")
    db_conn.execute("""CREATE INDEX taxonomies_sample_id
                        ON taxonomies (sample_id)""")


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
                quality_notes)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    """Taxonomies and images where the two are not in each other's table."""
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
            plate_row  INTEGER NOT NULL,
            plate_col  TEXT NOT NULL,
            sample_id  TEXT NOT NULL
        )""")
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


def get_plate_report(db_conn, plate_id):
    """Get data for a plate."""
    sql = """
        SELECT *
          FROM sample_plates
          JOIN taxonomies USING (sample_id)
         WHERE plate_id = ?
      ORDER BY plate_row, plate_col
        """
    return db_conn.execute(sql, (plate_id, ))
