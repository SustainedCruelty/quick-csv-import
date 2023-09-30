#!/usr/bin/python3
import re
import pandas as pd
import numpy as np
from hdbcli import dbapi
import argparse
import logging

log_format = '[%(asctime)s] [%(levelname)s] - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format)

parser = argparse.ArgumentParser(description = "import csvs into hana (quickly)")

parser.add_argument("--address", help = "hana address", default = "")
parser.add_argument("--port", help = "hana port", default = "")
parser.add_argument("--user", help = "hana user", default = "")
parser.add_argument("--password", help = "hana password", default = "")
parser.add_argument("--database", help = "hana database", default = "")

subparsers = parser.add_subparsers(dest = "command")

parser_import = subparsers.add_parser("import", help = "import a csv into hana")

parser_import.add_argument("csv", help = "csv file to import")
parser_import.add_argument("sql", help = "table creation script to execute")
parser_import.add_argument("--drop-duplicates", help = "drop duplicates", action = "store_true", default = True)
parser_import.add_argument("--separator", help = "csv separator", default = ",")

parser_exec = subparsers.add_parser("exec", help = "execute a sql file")
parser_exec.add_argument("sql", help = "sql file to execute")

args = parser.parse_args()

hana_to_pandas = {
    "TINYINT": np.int8,
    "SMALLINT": np.int16,
    "INTEGER": np.int32,
    "INT": np.int32,
    "BIGINT": np.int64,
    "DOUBLE": np.double,
    "NVARCHAR": object,
    "VARCHAR": object,
    "BOOLEAN": np.uint8,
    "TIMESTAMP": "datetime64[s]",
    "REAL": np.float32,
}

conn = dbapi.connect(
    address=args.address,
    port=args.port,
    user=args.user,
    password=args.password,
    databaseName=args.database,
)

cursor = conn.cursor()

with open(args.sql, "r") as f:
    script = f.read()

if args.command == "exec":
    cursor.execute(script)
    logging.info("executed the script")

elif args.command == "import":

    table_pattern = r'(CREATE\s+(ROW|COLUMN)\s+TABLE\s+"([A-Za-z]*)"."([A-Za-z]*)"\s*\((.*)\);)'

    matches = re.findall(table_pattern, script, re.S)

    assert len(matches) == 1, "every sql script needs one table creation statement"

    create_table, _, schema, table, _ = matches[0]
    column_pattern = r'"([^"]+)"\s+([A-Z]+(\([0-9]*\)\s*)?)\s+([^,\s]+(?:\s+[^,\s]+)*)[^,]*(?:,|$)'

    matches = re.findall(column_pattern, create_table)

    all_columns = []
    pandas_types = {}

    for match in matches:
        col_name, col_type, col_length, col_null = match
        col_type = col_type.strip(col_length)

        if col_type.strip() in hana_to_pandas.keys():
            pandas_types[col_name] = hana_to_pandas[col_type.strip()]
        else:
            raise Exception("unknown type  " + col_type)

        all_columns.append(col_name)

    df = pd.read_csv(args.csv, sep=args.separator)
    logging.info(f"loaded {args.csv} with {len(df)} rows and {len(df.columns)} columns")
    df.rename(inplace=True, columns = str.upper)
    df = df[all_columns]

    df = df.astype(pandas_types, errors='raise')

    if args.drop_duplicates:
        df = df.drop_duplicates()

    logging.info("inserting the following columns: " + ', '.join(all_columns))


    cursor.execute(script)
    logging.info("executed the script")

    params = ','.join(['?'] * len(all_columns))
    sql = f'INSERT INTO "{schema}"."{table}" VALUES ({params})'

    cursor.executemany(sql, list(df.itertuples(index = False, name = None)))
    logging.info("inserted the rows")

cursor.close()
conn.commit()
conn.close()
