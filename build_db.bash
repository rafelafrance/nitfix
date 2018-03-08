#!/bin/bash

set -e
set -x

python src/process_images.py -f 'data/raw/DOE-nitfix_specimen_photos/*.JPG' -c
python src/process_images.py -f 'data/raw/raw/HUH_DOE-nitfix_specimen_photos/*.JPG'
python src/process_images.py -f 'data/raw/OS_DOE-nitfix_specimen_photos/*.JPG'
python src/process_images.py -f 'data/raw/raw/CAS-DOE-nitfix_specimen_photos/*.JPG'

python src/get_master_taxonomy.py
python src/get_uuids.py

# Manually fix UUID
# python src/process_images.py -f 'data/raw/DOE-nitfix_specimen_photos/R0000674.JPG' --set-uuid 3d83f5f0-419f-4aaf-be4d-2773f2683412

# Note error resolutions
python src/resolve_error.py -k data/raw/raw/DOE-nitfix_specimen_photos/R0000149.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000151.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/raw/DOE-nitfix_specimen_photos/R0000158.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000165.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/raw/DOE-nitfix_specimen_photos/R0000674.JPG -r 'OK: Is a duplicate of R0000473'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000835.JPG -r 'OK: Is a duplicate of R0000836'
python src/resolve_error.py -k data/raw/raw/DOE-nitfix_specimen_photos/R0000895.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000937.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0001055.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/HUH_DOE-nitfix_specimen_photos/R0001262.JPG -r 'OK: Is a duplicate of R0001263'
python src/resolve_error.py -k data/raw/HUH_DOE-nitfix_specimen_photos/R0001729.JPG -r 'OK: Is a duplicate of R0001728'
python src/resolve_error.py -k data/raw/OS_DOE-nitfix_specimen_photos/R0000229.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/OS_DOE-nitfix_specimen_photos/R0001835.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/OS_DOE-nitfix_specimen_photos/R0001898.JPG -r 'OK: Genuine duplicate'
# python src/resolve_error.py -k data/raw/OS_DOE-nitfix_specimen_photos/R0001947.JPG -r ''
python src/resolve_error.py -k data/raw/CAS-DOE-nitfix_specimen_photos/R0001361.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/CAS-DOE-nitfix_specimen_photos/R0002349.JPG -r 'OK: Genuine duplicate'

python src/get_sample_plates.py
