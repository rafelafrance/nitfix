DATE=`date +%Y-%m-%d`
PROCESSED=./data/processed
DB=nitfix.sqlite.db
SRC="$(PROCESSED)/$(DB)"
DST="$(PROCESSED)/$(basename $(DB))_$(DATE).db"
PYTHON=python

all: image_ingest taxonomy_ingest sample_ingest plate_report select_samples

image_ingest:
	$(PYTHON) ./src/01_ingest_images.py

taxonomy_ingest:
	$(PYTHON) ./src/02_ingest_taxonomy.py

sample_ingest:
	$(PYTHON) ./src/03_ingest_samples.py

plate_report:
	$(PYTHON) ./src/04_sample_plate_report.py

select_samples:
	$(PYTHON) ./src/05_sample_selection.py

backup:
	cp $(SRC) $(DST)
