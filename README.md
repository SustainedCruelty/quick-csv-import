# quick-csv-import
quick way to import csv files into an sap hana database

## usage

```shell
usage: main.py [-h] [--address ADDRESS] [--port PORT] [--user USER] [--password PASSWORD] [--database DATABASE] [--drop-duplicates] csv sql

import csvs into hana (quickly)

positional arguments:
  csv                  csv file to import
  sql                  sql file to execute

options:
  -h, --help           show this help message and exit
  --address ADDRESS    hana address
  --port PORT          hana port
  --user USER          hana user
  --password PASSWORD  hana password
  --database DATABASE  hana database
  --drop-duplicates    drop duplicates
```