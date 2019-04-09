DATE=`date +%Y-%m-%d`
PROCESSED=./data/processed
DB=nitfix.sqlite.db
DB_NAME="$(PROCESSED)/$(DB)"
DB_BACKUP="$(PROCESSED)/$(basename $(DB))_$(DATE).db"
PYTHON=python
SRC=./python

all: images taxonomy sequencing plate_report select_samples

images:
	$(PYTHON) $(SRC)/ingest_images.py
	$(PYTHON) $(SRC)/ingest_pilot_data.py
	$(PYTHON) $(SRC)/ingest_corrales_data.py

taxonomy:
	$(PYTHON) $(SRC)/ingest_taxonomy.py
	$(PYTHON) $(SRC)/ingest_taxonomy_ts.py
	$(PYTHON) $(SRC)/audit_taxonomy.py
	$(PYTHON) $(SRC)/ingest_loci_data.py
	$(PYTHON) $(SRC)/ingest_werner_data.py
	$(PYTHON) $(SRC)/ingest_nfn_data.py
	$(PYTHON) $(SRC)/ingest_priority_taxa.py
	# $(PYTHON) $(SRC)/ingest_tropicos_data.py

sequencing:
	$(PYTHON) $(SRC)/ingest_sample_plates.py
	$(PYTHON) $(SRC)/ingest_rapid_qc_data.py
	$(PYTHON) $(SRC)/ingest_rapid_reformatting_data.py

plate_report:
	$(PYTHON) $(SRC)/sample_plate_report.py

select_samples:
	$(PYTHON) $(SRC)/sample_selection.py

crawl_tropicos:
	$(PYTHON) $(SRC)/crawl_tropicos_website.py

backup:
	cp $(DB_NAME) $(DB_BACKUP)
