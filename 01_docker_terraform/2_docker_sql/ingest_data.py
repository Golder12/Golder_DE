import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
import os
import gzip

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}.gz")

    # Unzip the compressed file
    with gzip.open(f"{csv_name}.gz", 'rb') as f_in:
    # Read the uncompressed data
        uncompressed_data = f_in.read()

    # Write the uncompressed data to the output CSV file
    with open(csv_name, 'wb') as f_out:
        f_out.write(uncompressed_data)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name,con=engine, if_exists='replace')


    df.to_sql(name=table_name,con=engine,if_exists='append')

    while True:
        t_start = time()
        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
        df.to_sql(name='green_taxi_trips',con=engine,if_exists='append')

        t_end = time()
        print("inserted another chunk...took %.3f second" % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

#user, password, host, port, database name, table name,
# url of the csv

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)
