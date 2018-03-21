#!/bin/bash

set -e
set -x

python src/process_images.py -f 'data/raw/DOE-nitfix_specimen_photos/*.JPG' -c
python src/process_images.py -f 'data/raw/HUH_DOE-nitfix_specimen_photos/*.JPG'
python src/process_images.py -f 'data/raw/OS_DOE-nitfix_specimen_photos/*.JPG'
python src/process_images.py -f 'data/raw/CAS-DOE-nitfix_specimen_photos/*.JPG'
python src/process_images.py -f 'data/raw/MO-DOE-nitfix_specimen_photos/*.JPG'
python src/process_images.py -f 'data/raw/NY-2-DOE-nitfix_specimen_photos/*.JPG'

# Note error resolutions
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000149.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000151.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000158.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000165.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000674.JPG -r 'OK: Is a duplicate of R0000473'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000835.JPG -r 'OK: Is a duplicate of R0000836'
python src/resolve_error.py -k data/raw/DOE-nitfix_specimen_photos/R0000895.JPG -r 'OK: Genuine duplicate'
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
python src/resolve_error.py -k data/raw/MO-DOE-nitfix_specimen_photos/R0002933.JPG -r 'OK: Genuine duplicate'
python src/resolve_error.py -k data/raw/MO-DOE-nitfix_specimen_photos/R0003226.JPG -r 'OK: Genuine duplicate'

# Manually fix UUID
python src/process_images.py -f 'data/raw/DOE-nitfix_specimen_photos/R0000674.JPG' --set-uuid 2eea159f-3c25-42ef-837d-27ad545a6779

python src/get_uuids.py

python src/get_master_taxonomy.py
python src/get_sample_plates.py
python src/get_picogreen.py
