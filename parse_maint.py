import argparse
import sqlite3
import numpy as np
import pandas as pd

# parse input arguments
parser = argparse.ArgumentParser(description='USPTO maintenance file parser.')
parser.add_argument('target', type=str, help='path of file to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
args = parser.parse_args()

# import to dataframe
print('Reading table')

datf = pd.read_fwf(args.target,
                   colspecs=[(0,7),(8,16),(17,18),(19,27),(28,36),(37,45),(46,51)],
                   usecols=[0,2,6],names=['patnum','is_small','event_code'])

# drop non utility patents
datf = datf.ix[datf['patnum'].apply(str.isdigit)]
datf['patnum'] = datf['patnum'].astype(np.int)

# clean up data
print('Data cleanup')

m4 = ['M1551','M170','M173','M183','M2551','M273','M283']
m8 = ['M1552','M171','M174','M184','M2552','M274','M284']
m12 = ['M1553','M172','M175','M185','M2553','M275','M285']
mmap = [(m,4) for m in m4]+[(m,8) for m in m8]+[(m,12) for m in m12]
codes = pd.DataFrame(mmap,columns=['code','lag']).set_index('code')

datf = datf.join(codes,on='event_code',how='inner').drop('event_code',axis=1)
datf['is_small'] = datf['is_small'] == 'Y'
pat_groups = datf.groupby('patnum')
last_maint = pat_groups['lag'].max()
ever_large = ~pat_groups['is_small'].min()
dpat = pd.DataFrame({'last_maint':last_maint,'ever_large':ever_large})

# commit to sql
print('Writing table')

con = sqlite3.connect(args.db)
cur = con.cursor()
dpat.to_sql('maint',con,if_exists='replace')
cur.execute('create unique index maint_idx on maint(patnum)')
con.commit()
con.close()
