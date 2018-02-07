#!/bin/bash

set -e
set -x

python python/process_images.py -f 'data/DOE-nitfix_specimen_photos/*.JPG' -c
python python/process_images.py -f 'data/HUH_DOE-nitfix_specimen_photos/*.JPG'
python python/process_images.py -f 'data/OS_DOE-nitfix_specimen_photos/*.JPG'
python python/process_images.py -f 'data/CAS-DOE-nitfix_specimen_photos/*.JPG'

python python/get_master_taxonomy.py
python python/get_uuids.py

# Manually fix UUID
# python python/process_images.py -f 'data/DOE-nitfix_specimen_photos/R0000674.JPG' --set-uuid 3d83f5f0-419f-4aaf-be4d-2773f2683412

# Note error resolutions
python python/resolve_error.py -k data/DOE-nitfix_specimen_photos/R0000149.JPG -r 'OK: Genuine duplicate'
python python/resolve_error.py -k data/DOE-nitfix_specimen_photos/R0000151.JPG -r 'OK: Genuine duplicate'
python python/resolve_error.py -k data/DOE-nitfix_specimen_photos/R0000158.JPG -r 'OK: Genuine duplicate'
python python/resolve_error.py -k data/DOE-nitfix_specimen_photos/R0000165.JPG -r 'OK: Genuine duplicate'
python python/resolve_error.py -k data/DOE-nitfix_specimen_photos/R0000674.JPG -r 'OK: Is a duplicate of R0000473'
python python/resolve_error.py -k data/DOE-nitfix_specimen_photos/R0000835.JPG -r 'OK: Is a duplicate of R0000836'
python python/resolve_error.py -k data/DOE-nitfix_specimen_photos/R0000895.JPG -r 'OK: Genuine duplicate'
python python/resolve_error.py -k data/DOE-nitfix_specimen_photos/R0000937.JPG -r 'OK: Genuine duplicate'
python python/resolve_error.py -k data/DOE-nitfix_specimen_photos/R0001055.JPG -r 'OK: Genuine duplicate'
python python/resolve_error.py -k data/HUH_DOE-nitfix_specimen_photos/R0001262.JPG -r 'OK: Is a duplicate of R0001263'
python python/resolve_error.py -k data/HUH_DOE-nitfix_specimen_photos/R0001729.JPG -r 'OK: Is a duplicate of R0001728'
python python/resolve_error.py -k data/OS_DOE-nitfix_specimen_photos/R0000229.JPG -r 'OK: Genuine duplicate'
python python/resolve_error.py -k data/OS_DOE-nitfix_specimen_photos/R0001835.JPG -r 'OK: Genuine duplicate'
python python/resolve_error.py -k data/OS_DOE-nitfix_specimen_photos/R0001898.JPG -r 'OK: Genuine duplicate'
# python python/resolve_error.py -k data/OS_DOE-nitfix_specimen_photos/R0001947.JPG -r ''

python python/get_sample_plates.py
