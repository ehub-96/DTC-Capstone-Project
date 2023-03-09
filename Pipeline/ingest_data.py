import argparse
import os
import pandas as pd
from sqlalchemy import create_engine

def main():
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = '5432'
    db = 'project-db'
    table_name =  'radioactive_waste'
    url = 'https://raw.githubusercontent.com/ehub-96/Project-Dataset/main/open-data-a-fin-2021.csv'

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    print(f"Downloading {url} to {csv_name}")
    os.system(f"wget {url} -O {csv_name}")

    if url.endswith('.csv.gz'):
        print(f"Uncompressing {csv_name}")
        os.system(f"gzip -d {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, encoding='latin-1', sep=';')

    df = next(df_iter)

    print(f"Creating table {table_name}")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    print(f"Inserting chunk 0 into table {table_name}")
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    for i, df in enumerate(df_iter):
        print(f"Inserting chunk {i+1} into table {table_name}")
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    print("Data successfully loaded into Postgres!")

if __name__ == '__main__':
    main()







