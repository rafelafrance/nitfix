DATE=`date +%Y-%m-%d`
PROCESSED=./data/processed
DB=nitfix.sqlite.db
DB_NAME="$(PROCESSED)/$(DB)"
DB_BACKUP="$(PROCESSED)/$(basename $(DB))_$(DATE).db"
PYTHON=python
SRC="./src"

all: image_qr_codes master_taxonomy sample_data plate_report select_samples

image_qr_codes:
	$(PYTHON) $(SRC)/ingest_images.py
	$(PYTHON) $(SRC)/ingest_pilot_data.py
	$(PYTHON) $(SRC)/ingest_corrales_data.py

master_taxonomy:
	$(PYTHON) $(SRC)/ingest_taxonomy.py
	$(PYTHON) $(SRC)/ingest_loci_data.py
	$(PYTHON) $(SRC)/ingest_werner_data.py
	$(PYTHON) $(SRC)/ingest_nfn_data.py

sample_data:
	$(PYTHON) $(SRC)/ingest_samples.py

plate_report:
	$(PYTHON) $(SRC)/sample_plate_report.py

select_samples:
	$(PYTHON) $(SRC)/sample_selection.py

backup:
	cp $(DB_NAME) $(DB_BACKUP)
