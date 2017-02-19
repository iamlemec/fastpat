import argparse
import sqlite3
import pandas as pd

# parse input arguments
parser = argparse.ArgumentParser(description='Compustat file parser.')
parser.add_argument('target', type=str, help='path of file to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
args = parser.parse_args()

# connect to compustat db
con = sqlite3.connect(args.db)
cur = con.cursor()

# read frame into memory
datf = pd.read_csv(args.target, error_bad_lines=False, skiprows=1,
                   names=['gvkey', 'datadate', 'year', 'name', 'assets', 'capx', 'cash',
                          'cogs', 'shares', 'deprec', 'income', 'employ', 'intan', 'debt',
                          'prefstock', 'revenue', 'sales', 'rnd', 'fcost', 'price', 'naics',
                          'sic', 'acquire', 'acquire_income'])

# clean up data
datf['mktval'] = datf['shares']*datf['price']
datf = datf.drop(['datadate', 'shares', 'prefstock', 'price'], axis=1)
datf = datf.fillna({'naics': 0})
datf['naics'] = datf['naics'].map(lambda x: int('{:<6.0f}'.format(x).replace(' ', '0')))
datf = datf[~((datf['naics']>=520000)&(datf['naics']<530000))] # remove financial firms

# write to sql
datf.to_sql('compustat', con, if_exists='replace')

# clean up and generate primary key on firmyear
cur.execute("""delete from compustat where year=''""")
cur.execute("""delete from compustat where rowid not in (select min(rowid) from compustat group by gvkey,year)""")
cur.execute("""delete from compustat where name is null""")
cur.execute("""create unique index firmyear_idx on compustat(gvkey asc, year asc)""")

# close db
con.commit()
con.close()
