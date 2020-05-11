# Phylogenomic discovery and engineering of nitrogen fixation into the bioenergy woody crop poplar

Utilities supporting this grant.

# Installation

You will need to have Python3 installed, as well as pip, a package manager for python. Run the following code:

```
git clone https://github.com/rafelafrance/nitfix.git
cd path/to/cloned/nitfix
python3 -m pip install -r requirements.txt
```

# Description

The NitFix system is a light-weight Laboratory Management System (LIMS) for handling project data related to this project. It consists of 4 major components as well as numerous minor components.

First, the central component is an SQLite3 database. This is where most of the data is stored. The general design is to ingest all of the raw data, typically Google Sheets, into the database as close as possible to the original format and it is then processed, error corrected, merged, etc. into a usable format. For instance, we ingest a couple of different taxonomies from different labs, so the raw taxonomies are imported into the database as is and then they are merged into a single error corrected and deduplicated taxonomy that is used for further processing.

To prevent this from being a chaotic pile of data, we have a few key values that are tracked through the system and for which we enforce data integrity. The primary key field is the sample ID, described above. It is used to track data from the image from the initial imaging, to DNA extraction, through DNA sequencing. Other important key fields are the taxonomic name and any external IDs assigned by other organizations.

The second central component is made up of the Python scripts that perform the actual processing on the data. Most of the scripts form the backbone of the ingestion and reporting process. They are organized into a Makefile which groups them into sets of scripts. One set is for scripts that scan images for QR-code that are used as sample IDs. Another set is for the ingestion and auditing of the taxonomic data. And so on.

The third major component is made up the JavaScript (Google Apps) Scripts attached to Google Sheets to streamline and audit the data as it is being entered into worksheets. These catch errors, like duplicate sample IDs before they are ingested into the database. There are also templates (macros) for creating structures for data entry, for example for entering 96-well plate data -- i.e. which sample IDs are placed into what wells. Data like this is typically entered using QR-Code or barcode readers.

And the fourth major component is the reporting system. The strategy here was to create standalone interactive HTML reports and CSV files. The HTML reports are single file reports that can be distributed to scientists without using an HTML server. They are used to search, filter, and examine the data. The CSV version of the reports allow scientists to perform their own analyses on the data.