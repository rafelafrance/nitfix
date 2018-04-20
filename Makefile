DATE=`date +%Y-%m-%d`
PROCESSED=./data/processed
DB=nitfix.sqlite.db
SRC="$(PROCESSED)/$(DB)"
DST="$(PROCESSED)/$(basename $(DB))_$(DATE).db"
PYTHON=python

all: images taxonomy samples report

images:
	$(PYTHON) ./src/01_ingest_images.py

taxonomy:
	$(PYTHON) ./src/02_ingest_taxonomy.py

samples:
	$(PYTHON) ./src/03_ingest_samples.py

report:
	$(PYTHON) ./src/04_sample_plate_report.py

backup:
	cp $(SRC) $(DST)

clean: backup
	rm ./data/processed/*
	rm ./data/interim/*
	rm ./output/*
