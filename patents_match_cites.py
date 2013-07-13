# match citation data with aggregated firm data (to be run after patents_match_merge.py)

import sqlite3
import pandas.io.sql as sqlio
import pandas as pd

# load in a lot of data
db_fname_within = 'store/within.db'
db_fname_cites = 'store/citations.db'

con_within = sqlite3.connect(db_fname_within)
con_cites = sqlite3.connect(db_fname_cites)

datf_cite = sqlio.read_frame('select citer,citee from citation',con_cites)
datf_grant = sqlio.read_frame('select patnum,firm_num,fileyear from grant_basic',con_within)
datf_trans = sqlio.read_frame('select assign_id,patnum,source_fn,dest_fn,execyear from assign_info',con_within)

# match citations to firms with patnum
datf_cite.rename(columns={'citer':'citer_pnum','citee':'citee_pnum'},inplace=True)
datf_firm_cite = datf_cite.merge(datf_grant,how='left',left_on='citer_pnum',right_on='patnum',suffixes=('','_citer'))
datf_firm_cite = datf_firm_cite.drop(['patnum'],axis=1).rename(columns={'firm_num':'citer_fnum','fileyear':'cite_year'})
datf_firm_cite = datf_firm_cite.merge(datf_grant.filter(['patnum','firm_num']),how='left',left_on='citee_pnum',right_on='patnum',suffixes=('','_citee'))
datf_firm_cite = datf_firm_cite.drop(['patnum'],axis=1).rename(columns={'firm_num':'citee_fnum'})
datf_firm_cite['self_cite'] = (datf_firm_cite['citer_fnum'] == datf_firm_cite['citee_fnum'])

# patent level statistics
n_cited = datf_firm_cite.groupby('citer_pnum').size()
n_citing = datf_firm_cite.groupby('citee_pnum').size()
n_self_cited = datf_firm_cite.groupby('citer_pnum')['self_cite'].sum()
datf_cite_stats = pd.concat([pd.Series(n_cited,name='n_cited'),pd.Series(n_citing,name='n_citing'),pd.Series(n_self_cited,name='n_self_cited')],axis=1).reset_index().rename(columns={'index':'patnum'})

# firm level statistics
datf_firm_cite_year = datf_firm_cite.groupby(['citer_fnum','citee_fnum','cite_year']).size().reset_index(name='ncites')
first_cite_agg = pd.Series(datf_firm_cite_year.groupby(['citer_fnum','citee_fnum'])['cite_year'].min(),name='first_cite')
ncites_agg = pd.Series(datf_firm_cite_year.groupby(['citer_fnum','citee_fnum'])['ncites'].sum(),name='ncites')
datf_firm_cite_agg = pd.concat([first_cite_agg,ncites_agg],axis=1).reset_index()

# determine whether acquiring firm cites selling firm
datf_trans_firm = datf_trans.merge(datf_firm_cite_agg.filter(['citer_fnum','citee_fnum','first_cite','ncites']),how='left',left_on=['dest_fn','source_fn'],right_on=['citer_fnum','citee_fnum'])
datf_trans_firm = datf_trans_firm.drop(['citer_fnum','citee_fnum'],axis=1).rename(columns={'first_cite':'dest_first_cite','ncites':'dest_ncites'})
datf_trans_firm = datf_trans_firm.merge(datf_firm_cite_agg.filter(['citer_fnum','citee_fnum','first_cite','ncites']),how='left',left_on=['source_fn','dest_fn'],right_on=['citer_fnum','citee_fnum'])
datf_trans_firm = datf_trans_firm.drop(['citer_fnum','citee_fnum'],axis=1).rename(columns={'first_cite':'source_first_cite','ncites':'source_ncites'})
datf_trans_firm = datf_trans_firm.fillna({'dest_ncites':0,'source_ncites':0})

# determine whether patent was cited by acquiring firm
datf_trans_pat = datf_trans.merge(datf_firm_cite.filter(['citer_fnum','citer_pnum','citee_pnum','cite_year']),how='left',left_on=['dest_fn','patnum'],right_on=['citer_fnum','citee_pnum'])
datf_trans_pat = datf_trans_pat.drop(['citer_fnum','citee_pnum'],axis=1)
ncites_before = pd.Series(datf_trans_pat[datf_trans_pat['cite_year']<=datf_trans_pat['execyear']].groupby('assign_id').size(),name='ncites_before')
ncites_after = pd.Series(datf_trans_pat[datf_trans_pat['cite_year']>datf_trans_pat['execyear']].groupby('assign_id').size(),name='ncites_after')
datf_trans_pat = datf_trans_pat.drop(['citer_pnum','cite_year'],axis=1).drop_duplicates()
datf_trans_pat = datf_trans_pat.join(ncites_before,on='assign_id')
datf_trans_pat = datf_trans_pat.join(ncites_after,on='assign_id')
datf_trans_pat = datf_trans_pat.fillna({'ncites_before':0,'ncites_after':0})

# save frames back to sql
sqlio.write_frame(datf_firm_cite,'firm_cite',con_cites,if_exists='replace')
sqlio.write_frame(datf_cite_stats,'cite_stats',con_cites,if_exists='replace')
sqlio.write_frame(datf_firm_cite_year,'firm_cite_year',con_cites,if_exists='replace')
sqlio.write_frame(datf_firm_cite_agg,'firm_cite_agg',con_cites,if_exists='replace')
sqlio.write_frame(datf_trans_firm,'trans_cite_firm',con_cites,if_exists='replace')
sqlio.write_frame(datf_trans_pat,'trans_cite_pat',con_cites,if_exists='replace')

# clean up
con_cites.commit()
con_cites.close()
con_within.close()
