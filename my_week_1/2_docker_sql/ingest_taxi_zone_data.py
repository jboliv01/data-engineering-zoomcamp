#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
import os
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time, sleep


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url
    csv_name = 'taxi+_zones_lookup.csv'
    
    os.system(f'wget {url} -O {csv_name}')

    # postgresql://root:root@localhost:5432/ny_taxi
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    
    df = pd.read_csv(csv_name)
    #print(trips.head(10))
    
     # Data Definition Language (DDL): defines the schema
    print(pd.io.sql.get_schema(df, name='taxi_zone_data', con=engine))

    chunk_size = 100
    chunk_count = 0

    for chunk in pd.read_csv(csv_name, chunksize=chunk_size):
        chunk_count += chunk.shape[0]
        t_start = time()
        chunk.to_sql(name=table, con=engine, if_exists='append', index=False)
        t_end = time()
        print(f'inserted another chunk, took {(t_end-t_start):.3f} seconds. {chunk_count} rows inserted.') 

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    #password, host, port, database name, table name, url of the csv

    parser.add_argument('--user', help='username')
    parser.add_argument('--password', help='password')
    parser.add_argument('--host', help='hostname')
    parser.add_argument('--port', help='port number')
    parser.add_argument('--db', help='database name')
    parser.add_argument('--table', help='name of the tabler')
    parser.add_argument('--url', help='url of the csv')                   

    args = parser.parse_args()

    main(args)
    