"""Fix sample IDs.

There are various sources of bad sample IDs. Some of which we can fix and
others we cannot or at least cannot without a great deal of effort.

a) Bar reader machines sometimes read a QR-Code or barcode incorrectly for
   whatever reason. This is fixable if we can trace the code back to what it's
   supposed to be.

b) Double printing of QR-code envelopes. Without meaningful codes -- vs. the
   UUIDs we're using now -- fixing them will be extremely difficult. One
   possible improvement here would be to add information about the museum,
   date, and who took the pictures to the sample ID. Hindsight being 20/20.
   This would make back tracing the issues a little bit easier.

   FYI: I did add checks to Google sheets to combat this particular error but
        people never used these safeguards.

c) Missing sample IDs. We fix them when possible. Someone forgets to log the
   sample ID into the master taxonomy.

d) Sample IDs in the master taxonomy without a corresponding picture. The
   actual sample may still exist.

Actions:
    Because the bad sample ID has already been propagated through the system
    we are forced to track the "bad ID" and link it to a good one, if one exists.

    Given a manual set of corrections:

    1) Add them to the taxonomy_ids table as if the "bad" sample ID was
       already entered into the taxonomy table manually.

    2) Adjust the images table to use the new "bad" sample ID.
"""

import pandas as pd

import lib.db as db
import lib.util as util


def manual_corrections_table():
    """Enter manual corrected sample IDs into the database."""
    cxn = db.connect()

    csv_path = util.RAW_DATA / 'missing_a_taxon.v3.HRK.csv'
    df = pd.read_csv(csv_path)
    df = df.loc[:, ['Note', 'sample_id', 'rapid_source', 'rapid_dest',
                    'local_no', 'well', 'plate_id', 'sci.name',
                    'sampleID_in_MasterTax']]
    df = df.rename(columns={
        'Note': 'note',
        'sample_id': 'bad_sample_id',
        'sci.name': 'sci_name',
        'sampleID_in_MasterTax': 'good_sample_id',
    })

    df.to_sql('manual_corrections', cxn, if_exists='replace', index=False)


def add_taxonomy_ids():
    """Update sample IDs in the database.

    Add them to the taxonomy_ids table as if the "bad" sample ID was already
    entered into the taxonomy table manually.
    """
    cxn = db.connect()
    sql = """
        INSERT INTO taxonomy_ids
        SELECT ti.sci_name, bad_sample_id
          FROM taxonomy_ids AS ti
          JOIN manual_corrections AS mc ON (ti.sample_id = mc.good_sample_id)
         WHERE bad_sample_id NOT IN (SELECT sample_id FROM taxonomy_ids)
      ORDER BY good_sample_id;
    """
    cxn.execute(sql)
    cxn.commit()


def handle_image_records():
    """Add image error records and update the image records."""
    cxn = db.connect()
    sql = """
        SELECT *
          FROM images AS im
          JOIN manual_corrections AS mc ON (im.sample_id = good_sample_id)
         WHERE good_sample_id <> bad_sample_id
           AND bad_sample_id NOT IN (SELECT sample_id FROM images);
    """
    df = pd.read_sql(sql, cxn)

    for _, row in df.iterrows():
        good = row['good_sample_id']
        bad = row['bad_sample_id']
        image = row['image_file']
        cxn.execute(
            'INSERT INTO image_errors (image_file, msg) VALUES (?, ?);',
            (image, f'MANUAL: Changed from {good} to {bad}'))
        cxn.execute(
            'UPDATE images SET sample_id = ? WHERE image_file = ?;',
            (bad, image))
        cxn.commit()


if __name__ == '__main__':
    manual_corrections_table()
    add_taxonomy_ids()
    handle_image_records()
