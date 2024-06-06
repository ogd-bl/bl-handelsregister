# BL-HANDELSREGISTER

This repository contains an Apache Airflow DAG to synchronize the [company inventory](https://data.bl.ch/explore/dataset/12480), [company mutations](https://data.bl.ch/explore/dataset/12460), and [company relocations](https://data.bl.ch/explore/dataset/12470) data tables on [data.bl.ch](data.bl.ch). It uses the API from [Zefix](https://www.zefix.ch/de/search/entity/welcome) and other sources.

For more details, refer to the md-Docs in the DAG and the doc-strings of the utility functions.

**Attention**: This process has **many** dependencies. It uses the Zefix-API for retrieving raw data, as well as the [BurWeb](https://www.bfs.admin.ch/bfs/de/home/register/unternehmensregister/betriebs-unternehmensregister/burweb.html)-API, [I14Y](https://www.i14y.admin.ch/de/home), the [sms.bfs.admin](https://sms.bfs.admin.ch)-API, and the [kGWR](https://data.bl.ch/explore/dataset/12180/table/?disjunctive.gemeindename)-Data to enrich and validate the data. Because some of the APIs are not very performant, intermediary results are stored in a PostgreSQL database that also contains a manually curated list of address corrections. Also, because regex-Functions are used to extract company names from free text, it is not guaranteed, that the solution translates well for non-german-speaking cantons. It is therefore very likely that with a correct local setup and an adapted config file, the process may still not run satisfactorily. However, this repository is a good entry point to tackle the topic and contains valuable resources to connect to data related to the Zefix-Data.

## Getting Started

### Project Setup
#### Airflow and PostgreSQL DB
Start an Airflow service, e.g., by using a Docker image. The current DAG runs with the image `apache/airflow:2.8.1` and `postgres:14-alpine`. Also set up a PostgreSQL database to store the tables and install the Python libraries in the `requirements.txt` file. 

#### Environment Variables
Rename `.env-example` to `.env` and configure the parameters.

#### Hardcoded Links
`ogd_handelsregister_config.json` contains the parameters used for the tables on [data.bl.ch](data.bl.ch). Adjust the parameters as needed.

#### DB Tables
```sql
CREATE TABLE ogd_data.zefix_unternehmen (
	legalseatid int4 NULL,
	legalseat text NULL,
	uid text NOT NULL,
	name text NULL,
	zusatz text NULL,
	strassenbezeichnung text NULL,
	strassenbezeichnung_loc text NULL,
	eingangsnummer_gebaeude text NULL,
	eingangsnummer_gebaeude_loc text NULL,
	postleitzahl text NULL,
	postleitzahl_loc text NULL,
	ort text NULL,
	gemeindename_loc text NULL,
	noga_codes text NULL,
	rechtsform_code text NULL,
	rechtsform text NULL,
	status text NULL,
	noga_abschnitt_code text NULL,
	noga_abschnitt text NULL,
	noga_abteilung text NULL,
	noga_gruppe text NULL,
	noga_klasse text NULL,
	noga_art text NULL,
	purpose text NULL,
	firmensitz_bezirk_nr text NULL,
	firmensitz_bezirk text NULL,
	zefix_web_eintrag text NULL,
	egid_loc text NULL,
	e_eingangskoordinate_loc text NULL,
	n_eingangskoordinate_loc text NULL,
	koordinaten text NULL,
	sogcdate date NULL,
	matching_style text NULL,
	"comment" text NULL,
	last_update timestamp NULL,
	registry_link text NULL
);
```

```sql
CREATE TABLE ogd_data.zefix_mutationen (
	kategorie text NULL,
	publikationsdatum_shab date NULL,
	journaldatum_handelsregister date NULL,
	id_shab text NULL,
	firmensitz_code text NULL,
	firmensitz text NULL,
	meldung text NULL,
	uid text NULL,
	firmenname text NULL,
	rechtsform_code text NULL,
	rechtsform text NULL,
	noga_code text NULL,
	noga text NULL,
	noga_abschnitt_code text NULL,
	noga_abschnitt text NULL,
	noga_abteilung text NULL
);
```

```sql
CREATE TABLE ogd_data.zefix_adressaenderungen (
	kategorie text NULL,
	publikationsdatum_shab date NULL,
	journaldatum_handelsregister date NULL,
	id_shab text NULL,
	firmensitz_neu_code text NULL,
	firmensitz_neu text NULL,
	firmensitz_neu_canton text NULL,
	firmensitz_bisher_code text NULL,
	firmensitz_bisher text NULL,
	firmensitz_bisher_canton text NULL,
	meldung text NULL,
	uid text NULL,
	firmenname text NULL,
	rechtsform_code text NULL,
	rechtsform text NULL,
	noga_code text NULL,
	noga text NULL,
	noga_abschnitt_code text NULL,
	noga_abschnitt text NULL,
	noga_abteilung text NULL
);
```

```sql
CREATE TABLE ogd_data.zefix_adresskorrekturen (
	uid varchar(50) NULL,
	strassenbezeichnung varchar(50) NULL,
	eingangsnummer_gebaeude varchar(50) NULL,
	postleitzahl int4 NULL
);
```

## Contact
If you have any troubles or questions, please don't hesitate to contact us at <ogd@bl.ch>.