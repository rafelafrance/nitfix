"""Extract, transform, and load samples QC'd by Rapid."""

import lib.normal_plate_layout as normal_plate


TABLE = 'QC_Normal_Plate_Layout'

GOOGLE_SHEETS = [
    'FMN_131002_QC_Normal_Plate_Layout',
    'KIB_135802_QC_Normal_Plate_Layout']


if __name__ == '__main__':
    for SHEET in GOOGLE_SHEETS:
        normal_plate.ingest_normal_plate_layout(SHEET)
    normal_plate.merge_normal_plate_layouts(GOOGLE_SHEETS, TABLE)
