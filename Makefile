DATE=`date +%Y-%m-%d`
PROCESSED=./data/processed
DB=nitfix.sqlite.db
SRC="$(PROCESSED)/$(DB)"
DST="$(PROCESSED)/$(basename $(DB))_$(DATE).db"
PYTHON=python

all: image_qr_codes master_taxonomy sample_data plate_report select_samples

image_qr_codes:
	$(PYTHON) ./src/ingest_images.py
	$(PYTHON) ./src/ingest_pilot_data.py
	$(PYTHON) ./src/ingest_corrales_data.py

master_taxonomy:
	$(PYTHON) ./src/ingest_taxonomy.py

sample_data:
	$(PYTHON) ./src/ingest_samples.py

plate_report:
	$(PYTHON) ./src/sample_plate_report.py

select_samples:
	$(PYTHON) ./src/sample_selection.py

backup:
	cp $(SRC) $(DST)
