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
cur = con.cursor()

print('Loading from database')

datf_cite = pd.read_sql('select src,dst from cite',con)
datf_grant = pd.read_sql('select patnum,firm_num,fileyear from patent_basic',con)
# datf_trans = pd.read_sql('select assignid,patnum,source_fn,dest_fn,execyear from assignment_info',con)

print('Matching with patents')

# match citations to firms with patnum
datf_cite['src'] = pd.to_numeric(datf_cite['src'],errors='coerce')
datf_cite['dst'] = pd.to_numeric(datf_cite['dst'],errors='coerce')
datf_cite = datf_cite.dropna().astype(np.int)
datf_cite = datf_cite.rename(columns={'src':'citer_pnum','dst':'citee_pnum'})
datf_cite = datf_cite.merge(datf_grant,how='left',left_on='citer_pnum',right_on='patnum',suffixes=('','_citer'))
datf_cite = datf_cite.drop(['patnum'],axis=1).rename(columns={'firm_num':'citer_fnum','fileyear':'cite_year'})
datf_cite = datf_cite.merge(datf_grant[['patnum','firm_num']],how='left',left_on='citee_pnum',right_on='patnum',suffixes=('','_citee'))
datf_cite = datf_cite.drop(['patnum'],axis=1).rename(columns={'firm_num':'citee_fnum'})
datf_cite['self_cite'] = (datf_cite['citer_fnum'] == datf_cite['citee_fnum'])

print('Aggregating together')

# patent level statistics
n_cited = datf_cite.groupby('citer_pnum').size()
n_citing = datf_cite.groupby('citee_pnum').size()
n_self_cited = datf_cite.groupby('citer_pnum')['self_cite'].sum()
datf_cite_stats = pd.DataFrame({'n_cited':n_cited,'n_citing':n_citing,'n_self_cited':n_self_cited})
datf_cite_stats.index.rename('patnum',inplace=True)
datf_cite_stats = datf_cite_stats.fillna(0).astype(np.int)

print('Writing to database')

# store in sql
datf_cite_stats.to_sql('cite_stats',con,if_exists='replace')
cur.execute('create unique index cite_stats_idx on cite_stats(patnum)')

# close out
con.commit()
con.close()

## firm level citation stats

# firm level statistics
# datf_cite_year = datf_cite.groupby(['citer_fnum','citee_fnum','cite_year']).size().reset_index(name='ncites')
# first_cite_agg = pd.Series(datf_cite_year.groupby(['citer_fnum','citee_fnum'])['cite_year'].min(),name='first_cite')
# ncites_agg = pd.Series(datf_cite_year.groupby(['citer_fnum','citee_fnum'])['ncites'].sum(),name='ncites')
# datf_cite_agg = pd.concat([first_cite_agg,ncites_agg],axis=1).reset_index()

# determine whether acquiring firm cites selling firm
# datf_trans_firm = datf_trans.merge(datf_cite_agg[['citer_fnum','citee_fnum','first_cite','ncites']],how='left',left_on=['dest_fn','source_fn'],right_on=['citer_fnum','citee_fnum'])
# datf_trans_firm = datf_trans_firm.drop(['citer_fnum','citee_fnum'],axis=1).rename(columns={'first_cite':'dest_first_cite','ncites':'dest_ncites'})
# datf_trans_firm = datf_trans_firm.merge(datf_cite_agg[['citer_fnum','citee_fnum','first_cite','ncites']],how='left',left_on=['source_fn','dest_fn'],right_on=['citer_fnum','citee_fnum'])
# datf_trans_firm = datf_trans_firm.drop(['citer_fnum','citee_fnum'],axis=1).rename(columns={'first_cite':'source_first_cite','ncites':'source_ncites'})
# datf_trans_firm = datf_trans_firm.fillna({'dest_ncites':0,'source_ncites':0})

# determine whether patent was cited by acquiring firm
# datf_trans_pat = datf_trans.merge(datf_cite[['citer_fnum','citer_pnum','citee_pnum','cite_year']],how='left',left_on=['dest_fn','patnum'],right_on=['citer_fnum','citee_pnum'])
# datf_trans_pat = datf_trans_pat.drop(['citer_fnum','citee_pnum'],axis=1)
# ncites_before = pd.Series(datf_trans_pat[datf_trans_pat['cite_year']<=datf_trans_pat['execyear']].groupby('assignid').size(),name='ncites_before')
# ncites_after = pd.Series(datf_trans_pat[datf_trans_pat['cite_year']>datf_trans_pat['execyear']].groupby('assignid').size(),name='ncites_after')
# datf_trans_pat = datf_trans_pat.drop(['citer_pnum','cite_year'],axis=1).drop_duplicates()
# datf_trans_pat = datf_trans_pat.join(ncites_before,on='assignid')
# datf_trans_pat = datf_trans_pat.join(ncites_after,on='assignid')
# datf_trans_pat = datf_trans_pat.fillna({'ncites_before':0,'ncites_after':0})

## patent level citations stats

# save frames back to sql - need to to this one at a time for memory reasons
# datf_cite.to_sql('firm_cite',con,if_exists='replace')
# datf_cite_year.to_sql('firm_cite_year',con,if_exists='replace')
# datf_cite_agg.to_sql('firm_cite_agg',con,if_exists='replace')
# datf_trans_firm.to_sql('trans_cite_firm',con,if_exists='replace')
# datf_trans_pat.to_sql('trans_cite_pat',con,if_exists='replace')
