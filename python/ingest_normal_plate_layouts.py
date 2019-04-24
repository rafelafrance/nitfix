"""Extract, transform, and load samples sent to Rapid."""

import lib.normal_plate_layout as normal_plate


TABLE = 'normal_plate_layout'

GOOGLE_SHEETS = [
    'FMN_131001_Normal_Plate_Layout',
    'KIB_135801_Normal_Plate_Layout']


if __name__ == '__main__':
    for SHEET in GOOGLE_SHEETS:
        normal_plate.ingest_normal_plate_layout(SHEET)
    normal_plate.merge_normal_plate_layouts(GOOGLE_SHEETS, TABLE)
