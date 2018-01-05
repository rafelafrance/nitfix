"""Utilities for SQL functions."""

from os.path import join
import re
import uuid
import sqlite3


def connect():
    """Connect to the SQLite3 DB."""
    db_path = join('data', 'nitfix.sqlite.db')
    db_conn = sqlite3.connect(db_path)
    # db_conn.create_function('REGEXP', 2, regexp)
    db_conn.create_function('IS_UUID', 1, is_uuid)
    db_conn.row_factory = sqlite3.Row
    return db_conn


def regexp(expression, item):
    """Create a user function for regular expressions."""
    regex = re.compile(expression, re.IGNORECASE | re.VERBOSE)
    return regex.search(item) is not None


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
                        taxonomy_key        TEXT PRIMARY KEY NOT NULL,
                        family              TEXT,
                        scientific_name     TEXT,
                        taxonomic_authority TEXT,
                        synonyms            TEXT,
                        tissue_sample_id    TEXT,
                        provider_acronym    TEXT,
                        provider_id         TEXT,
                        quality_notes       TEXT
                    )""")
    db_conn.execute('CREATE INDEX taxonomies_key ON taxonomies (taxonomy_key)')
    db_conn.execute("""CREATE INDEX taxonomies_provider_acronym ON taxonomies
                       (provider_acronym)""")
    db_conn.execute("""CREATE INDEX taxonomies_provider_id ON taxonomies
                       (provider_id)""")


def insert_taxonomy(db_conn, record):
    """Insert a record into the taxonomies table."""
    sql = """
        INSERT INTO taxonomies (
                        taxonomy_key,
                        family,
                        scientific_name,
                        taxonomic_authority,
                        synonyms,
                        tissue_sample_id,
                        provider_acronym,
                        provider_id,
                        quality_notes)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    db_conn.execute(sql, record)
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
    sql = """SELECT * FROM taxonomies WHERE IS_UUID(tissue_sample_id)"""
    return db_conn.execute(sql)


def get_images_taxonomies(db_conn, file_pattern):
    """Get images joind with their matching taxonomies."""
    sql = """
            SELECT images.*, taxonomies.*
              FROM images
              JOIN taxonomies ON images.image_id = taxonomies.tissue_sample_id
              WHERE file_name LIKE ?
           ORDER BY images.file_name
        """
    return db_conn.execute(sql, (file_pattern, ))


def get_taxonomy_by_image_id(db_conn, file_id):
    """Look for a taxonomy with the given file ID."""
    pattern = '%{}%'.format(file_id)
    sql = """SELECT * FROM taxonomies WHERE tissue_sample_id LIKE ?"""
    result = db_conn.execute(sql, (pattern, ))
    return result.fetchall()


def old_taxonomy_image_mismatches(db_conn):
    """Taxonomies and images where the two are not in each other's table."""
    sql = """
          WITH taxos AS (
            SELECT * FROM taxonomies WHERE IS_UUID(tissue_sample_id))
        SELECT images.*, taxos.*
          FROM images
     LEFT JOIN taxos ON images.image_id = taxos.tissue_sample_id
         WHERE taxos.tissue_sample_id IS NULL
     UNION
        SELECT images.*, taxos.*
          FROM taxos
     LEFT JOIN images ON images.image_id = taxos.tissue_sample_id
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
