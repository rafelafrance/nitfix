"""Extract, transform, and load samples sent to Rapid."""

import lib.normal_plate_layout as normal_plate
from lib.util import NORMAL_PLATE_SHEETS

TABLE = 'normal_plate_layout'

if __name__ == '__main__':
    for SHEET in NORMAL_PLATE_SHEETS:
        normal_plate.ingest_normal_plate_layout(SHEET)
    normal_plate.merge_normal_plate_layouts(NORMAL_PLATE_SHEETS, TABLE)
