DATE=`date +%Y-%m-%d`
PROCESSED=./data/processed
DB=nitfix.sqlite.db
SRC="$(PROCESSED)/$(DB)"
DST="$(PROCESSED)/$(basename $(DB))_$(DATE).db"
PYTHON=python

all: image_ingest\
		 taxonomy_ingest\
		 sample_ingest\
		 plate_report\
		 select_samples

image_ingest:
	$(PYTHON) ./src/ingest_images.py

taxonomy_ingest:
	$(PYTHON) ./src/ingest_taxonomy.py

sample_ingest:
	$(PYTHON) ./src/ingest_samples.py

plate_report:
	$(PYTHON) ./src/sample_plate_report.py

select_samples:
	$(PYTHON) ./src/sample_selection.py

backup:
	cp $(SRC) $(DST)
