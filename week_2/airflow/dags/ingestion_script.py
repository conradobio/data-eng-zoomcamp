import pandas as pd
import logging
from sqlalchemy import create_engine
from time import time
import os

def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    logging.info(f'connected to {db}')

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)
    df = next(df_iter)
    logging.info(f'df shape {df.shape}')
    logging.info(f'df columns {df.columns}')

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    logging.info('starting to ingest data')
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    logging.info('Insert the first chunk')
    while True: 
        #try:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()

        logging.info('inserted another chunk...., took %.3f seconds' % (t_end - t_start))
        #except StopIteration:
        #    logging.info("Finished ingesting data into the postgres database")
        #    break
