DATE=`date +%Y-%m-%d`
PROCESSED=./data/processed
TEMP=./data/temp
DB=nitfix.sqlite.db
DB_NAME="$(PROCESSED)/$(DB)"
DB_BACKUP="$(PROCESSED)/$(basename $(DB))_$(DATE).db"
PYTHON=python
SRC=./nitfix

all: images taxonomy sequencing plate_report select_samples

images:
	$(PYTHON) $(SRC)/ingest_images.py
	$(PYTHON) $(SRC)/ingest_pilot_data.py
	$(PYTHON) $(SRC)/ingest_corrales_data.py

taxonomy:
	$(PYTHON) $(SRC)/ingest_taxonomies.py
	$(PYTHON) $(SRC)/audit_taxonomy.py
	$(PYTHON) $(SRC)/ingest_loci_data.py
	$(PYTHON) $(SRC)/ingest_sprent_data.py
	$(PYTHON) $(SRC)/ingest_non_fabales_data.py
	$(PYTHON) $(SRC)/ingest_loci_data.py
	$(PYTHON) $(SRC)/ingest_werner_data.py
	$(PYTHON) $(SRC)/ingest_nfn_data.py
	$(PYTHON) $(SRC)/ingest_priority_taxa.py

sequencing:
	$(PYTHON) $(SRC)/ingest_sample_plates.py
	$(PYTHON) $(SRC)/ingest_normal_plate_layouts.py
	$(PYTHON) $(SRC)/ingest_qc_normal_plate_layouts.py
	$(PYTHON) $(SRC)/ingest_reformatting_templates.py
	$(PYTHON) $(SRC)/ingest_sample_sheets.py
	$(PYTHON) $(SRC)/ingest_sequencing_metadata.py

plate_report:
	$(PYTHON) $(SRC)/sample_plate_report.py

select_samples:
	$(PYTHON) $(SRC)/sample_selection.py

clean:
	rm ${TEMP}/*

backup:
	cp $(DB_NAME) $(DB_BACKUP)
