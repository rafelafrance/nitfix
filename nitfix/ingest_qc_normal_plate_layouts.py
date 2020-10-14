"""Extract, transform, and load samples QC'd by Rapid."""

import lib.normal_plate_layout as normal_plate
from lib.util import QC_NORMAL_PLATE_SHEETS

TABLE = 'qc_normal_plate_layout'

if __name__ == '__main__':
    for SHEET in QC_NORMAL_PLATE_SHEETS:
        normal_plate.ingest_normal_plate_layout(SHEET)
    normal_plate.merge_normal_plate_layouts(QC_NORMAL_PLATE_SHEETS, TABLE)
