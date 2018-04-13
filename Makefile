DATE=`date +%Y-%m-%d`
PROCESSED=./data/processed
BACKUP=./data/backup
DB=nitfix.sqlite.db
SRC="$(PROCESSED)/$(DB)"
DST="$(BACKUP)/$(basename $(DB))_$(DATE).db"
PYTHON=python 

all: images taxonomy samples report

images:
	@echo "images not done $(DATE)"

taxonomy:
	$(PYTHON) ./src/02_ingest_taxonomy.py

samples:
	$(PYTHON) ./src/03_ingest_samples.py

report:
	$(PYTHON) ./src/04_sample_plate_report.py

backup:
	cp $(SRC) $(DST)

clean:
	rm ./data/interim/*
