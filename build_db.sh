python python/process_images.py -f 'data/DOE-nitfix_specimen_photos/*.JPG' -c
python python/process_images.py -f 'data/HUH_DOE-nitfix_specimen_photos/*.JPG'
python python/process_images.py -f 'data/OS_DOE-nitfix_specimen_photos/*.JPG'
# python python/process_images.py -f 'data/DOE-nitfix_specimen_photos/R0000674.JPG' --set-uuid 000

python python/get_master_taxonomy.py
