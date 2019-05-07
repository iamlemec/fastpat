import argparse
import sqlite3
import pandas as pd

# parse input arguments
parser = argparse.ArgumentParser(description='Compustat file parser.')
parser.add_argument('target', type=str, help='path of file to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
args = parser.parse_args()

# read frame into memory
datf = pd.read_csv(args.target, error_bad_lines=False, skiprows=1, names=[
    'gvkey', 'datadate', 'year', 'name', 'assets', 'capx', 'cash', 'cogs',
    'shares', 'deprec', 'income', 'employ', 'intan', 'debt', 'prefstock',
    'revenue', 'sales', 'rnd', 'fcost', 'price', 'naics', 'sic'
])

# clean up data
datf['mktval'] = datf['shares']*datf['price']
datf = datf.drop(['datadate', 'shares', 'prefstock', 'price'], axis=1)
datf = datf.dropna(subset=['gvkey', 'year', 'name'])
datf['name'] = datf['name'].str.lower()
datf['naics'] = datf['naics'].fillna(0).astype(int).map(lambda x: f'{x:<06d}')

# write to sql
with sqlite3.connect(args.db) as con:
    datf.to_sql('compustat', con, if_exists='replace')
    con.commit()
