# LIMS2DB

A package used by the National Genomics Infrastructure to fetch data from a Basespace (previously Genologics) clarity LIMS database and push it to a CouchDB database. The code is designed to suit our business logic and re-usability is unfortunately not prioritised.

## Installation

```
conda create -n lims2db_dev python=2.7
conda activate lims2db_dev
```

LIMS2DB is highly dependent on the [statusdb](https://github.com/SciLifeLab/statusdb), [genologics](https://github.com/scilifelab/genologics), [genologics_sql](https://github.com/scilifelab/genologics_sql) packages. The two latter are available on pypi and can be installed with pip. However, it might still be a good idea to install all three of these manually to be sure you get the latest version:

```
git clone repo
cd repo
python setup.py install  # (or develop)
```

Then fork the LIMS2DB repository and clone it to your own computer.

```
cd LIMS2DB
conda install psycopg2
pip install -r requirements.txt
python setup.py develop
```

### Config files

To be able to run any of the LIMS2DB scripts, you will need two config files:

#### LIMS2DB

```
statusdb:
    url: couchdb_url
    username: some_name
    password: some_password
```

#### `~/.genolosql.yaml`

```
username: some_username
password: some_password
url: clarity_URL
db: DB_name
```
