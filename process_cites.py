# match citation data with aggregated firm data (to be run before firm_merge.py)

import argparse
import sqlite3
import numpy as np
import pandas as pd

# parse input arguments
parser = argparse.ArgumentParser(description='Merge patent citation data.')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
args = parser.parse_args()

# load in a lot of data
con = sqlite3.connect(args.db)

print('Loading from database')

datf_cite = pd.read_sql('select src,dst from cite', con)
datf_grant = pd.read_sql('select grant.patnum,firm_num,fileyear from grant left join grant_firm on grant.patnum=grant_firm.patnum', con)

print('Matching with patents')

# match citations to firms with patnum
datf_cite = datf_cite.rename(columns={'src': 'citer_pnum', 'dst': 'citee_pnum'})
datf_cite = datf_cite.merge(datf_grant, how='left', left_on='citer_pnum', right_on='patnum', suffixes=('', '_citer'))
datf_cite = datf_cite.drop(['patnum'], axis=1).rename(columns={'firm_num': 'citer_fnum', 'fileyear': 'cite_year'})
datf_cite = datf_cite.merge(datf_grant[['patnum', 'firm_num']], how='left', left_on='citee_pnum', right_on='patnum', suffixes=('', '_citee'))
datf_cite = datf_cite.drop(['patnum'], axis=1).rename(columns={'firm_num': 'citee_fnum'})
datf_cite['self_cite'] = (datf_cite['citer_fnum'] == datf_cite['citee_fnum'])

print('Aggregating together')

# patent level statistics
datf_cite_stats = pd.DataFrame({
    'n_cited': datf_cite.groupby('citer_pnum').size(),
    'n_citing': datf_cite.groupby('citee_pnum').size(),
    'n_self_cited': datf_cite.groupby('citer_pnum')['self_cite'].sum()
}).rename_axis('patnum')
datf_cite_stats = datf_cite_stats.fillna(0).astype(np.int)

print('Writing to database')

# store in sql
datf_cite_stats.to_sql('cite_stats', con, if_exists='replace')

# close out
con.commit()
con.close()
