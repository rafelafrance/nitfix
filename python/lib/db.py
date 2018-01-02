"""Utilities for SQL functions."""

from os.path import join
import sqlite3


def connect():
    """Connect to the SQLite3 DB."""
    db_path = join('data', 'nitfix.sqlite.db')
    return sqlite3.connect(db_path)


def create_images_table(db_conn):
    """Create images table and index."""
    db_conn.execute('DROP TABLE IF EXISTS images')
    db_conn.execute("""
                    CREATE TABLE images (
                        image_id      TEXT PRIMARY KEY NOT NULL,
                        file_name     TEXT NOT NULL UNIQUE,
                        image_created TEXT
                    )""")


def insert_image(db_conn, uuid, file_name, image_created):
    """Insert a record into the images table."""
    sql = """
        INSERT INTO images (image_id, file_name, image_created)
             VALUES (?, ?, ?)
        """
    db_conn.execute(sql, (uuid, file_name, image_created))
    db_conn.commit()


def get_image(db_conn, image_ids):
    """Get an image by its primary key."""
    image_ids = ', '.join(
        ["'{}'".format(i.strip()) for i in image_ids.split(';')])
    sql = """SELECT * FROM images WHERE id IN ({})""".format(image_ids)
    result = db_conn.execute(sql)
    return result.fetchone()


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
    insert = """INSERT INTO errors (error_key, msg) VALUES (?, ?)"""
    db_conn.execute(insert, (error_key, msg))
    db_conn.commit()


def create_taxonomies_table(db_conn):
    """Create a table from the Google sheet."""
    db_conn.execute('DROP TABLE IF EXISTS taxonomies')
    db_conn.execute("""
                    CREATE TABLE taxonomies (
                        taxonmy_key         TEXT PRIMARY KEY NOT NULL,
                        family              TEXT,
                        scientific_name     TEXT,
                        taxonomic_authority TEXT,
                        synonyms            TEXT,
                        tissue_sample_id    TEXT,
                        provider_acronym    TEXT,
                        provider_id         TEXT,
                        quality_notes       TEXT
                    )""")
    db_conn.execute('CREATE INDEX taxonomies_key ON taxonomies (taxonmy_key)')
    db_conn.execute("""CREATE INDEX taxonomies_provider_acronym ON taxonomies
                       (provider_acronym)""")
    db_conn.execute("""CREATE INDEX taxonomies_provider_id ON taxonomies
                       (provider_id)""")


def insert_taxonomy(db_conn, record):
    """Insert a record into the taxonomies table."""
    sql = """
        INSERT INTO taxonomies (
                        key,
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
