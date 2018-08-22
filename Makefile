DATE=`date +%Y-%m-%d`
PROCESSED=./data/processed
DB=nitfix.sqlite.db
DB_NAME="$(PROCESSED)/$(DB)"
DB_BACKUP="$(PROCESSED)/$(basename $(DB))_$(DATE).db"
PYTHON=python
SRC="./src"

all: images taxonomy sample_plates plate_report

images:
	$(PYTHON) $(SRC)/ingest_images.py && $(PYTHON) $(SRC)/ingest_pilot_data.py && $(PYTHON) $(SRC)/ingest_corrales_data.py

taxonomy:
	$(PYTHON) $(SRC)/ingest_taxonomy.py && $(PYTHON) $(SRC)/ingest_loci_data.py && $(PYTHON) $(SRC)/ingest_werner_data.py && $(PYTHON) $(SRC)/ingest_nfn_data.py

sample_plates:
	$(PYTHON) $(SRC)/ingest_sample_plates.py && $(PYTHON) $(SRC)/ingest_rapid_qc_data.py && $(PYTHON) $(SRC)/ingest_rapid_reformatting_data.py

plate_report:
	$(PYTHON) $(SRC)/sample_plate_report.py

select_samples:
	$(PYTHON) $(SRC)/sample_selection.py

backup:
	cp $(DB_NAME) $(DB_BACKUP)
