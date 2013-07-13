import numpy as np
import sqlite3
import pandas as pd
import sys
import itertools

# execution state
if len(sys.argv) == 1:
  stage = 0
else:
  stage = int(sys.argv[1])

# load in data from db
#def main(stage=0):
run0 = True
if stage <= 0 and run0:
    # load data
    print 'Loading data'

    # load firm data
    # firm_life starts a firm when they file for their first patent and ends after their last file
    conn = sqlite3.connect('store/within.db')
    cur = conn.cursor()
    datf = pd.DataFrame(cur.execute('select firm_num,year,source_nbulk,source_pnum,dest_nbulk,dest_pnum,file_pnum,income,revenue,rnd,naics from firmyear_info where year>=1950').fetchall(),columns=['fnum','year','source_bulk','source','dest_bulk','dest','file','income','revenue','rnd','naics'])
    firm_info = pd.DataFrame(data=cur.execute('select firm_num,year_min,year_max,life_span,high_tech from firm_life').fetchall(),columns=['fnum','zero_year','max_year','life_span','high_tech'])
    datf_trans = pd.DataFrame(cur.execute('select patnum,execyear,source_fn,dest_fn,grantyear,fileyear from assign_info where execyear>=1950').fetchall(),columns=['patnum','year','source_fn','dest_fn','grantyear','fileyear'],dtype=np.int)
    conn.close()

    # extra stats
    datf_trans['patage'] = datf_trans['year'] - datf_trans['fileyear']
    datf_trans['patage_grant'] = datf_trans['year'] - datf_trans['grantyear']

    # make index
    print 'Reindexing'

    fnum_set = firm_info['fnum']
    zero_year = firm_info['zero_year']
    max_year = firm_info['max_year']+1
    life_span = firm_info['life_span']+1
    all_fnums = np.array(list(itertools.chain.from_iterable([[fnum]*life for (fnum,life) in zip(fnum_set,life_span)])),dtype=np.int)
    all_years = np.array(list(itertools.chain.from_iterable([xrange(x,y+1) for (x,y) in zip(zero_year,max_year)])),dtype=np.int)
    fy_all = pd.DataFrame(data={'fnum': all_fnums, 'year': all_years})
    datf_idx = fy_all.merge(datf,how='left',on=['fnum','year']).fillna(value={'file':0,'dest':0,'source':0,'source_bulk':0,'dest_bulk':0},inplace=True)

    # patent expiry (file + 17)
    datf_idx['year_17p'] = datf_idx['year'] + 17
    datf_idx = datf_idx.merge(datf_idx.filter(['fnum','year_17p','file','dest']),how='left',left_on=['fnum','year'],right_on=['fnum','year_17p'],suffixes=('','_expire'))
    datf_idx = datf_idx.drop(['year_17p','year_17p_expire'],axis=1)
    datf_idx = datf_idx.fillna({'file_expire':0,'dest_expire':0})

    # derivative columns
    datf_idx = datf_idx.merge(firm_info.filter(['fnum','zero_year','max_year','life_span','has_comp','has_revn','has_rnd','has_pats','pats_tot','high_tech']),how='left',on='fnum')
    datf_idx['age'] = datf_idx['year']-datf_idx['zero_year']
    datf_idx['trans'] = datf_idx['source']+datf_idx['dest']
    datf_idx['ht_bin'] = datf_idx['high_tech'] > 0.5

    #### select only high tech firms ####
    #datf_trans = datf_trans[datf_trans['ht_bin']]

run1 = True
if stage <= 1 and run1:
    # construct patent stocks
    print 'Constructing patent stocks'

    datf_idx['patnet'] = datf_idx['file'] + datf_idx['dest'] - datf_idx['source'] - datf_idx['file_expire'] - datf_idx['dest_expire']
    firm_groups = datf_idx.groupby('fnum')
    datf_idx['stock'] = firm_groups['patnet'].cumsum() - datf_idx['patnet']
    #datf_idx = datf_idx[datf_idx['stock']>0]

# selections and groupings
run2 = True
if stage <= 2 and run2:
    # calculate transfer statistics
    print 'Calculating transfer statistics'

    # group by year
    all_year_groups = datf_idx.groupby('year')

    # normalize stock/age into yearly ranks
    rankify = lambda s: np.argsort(s).astype(np.float)/(len(s)-1)
    datf_idx['stock_rank'] = all_year_groups['stock'].apply(rankify)
    datf_idx['age_rank'] = all_year_groups['age'].apply(rankify)

    # group by size and year
    median_stock_vec = all_year_groups['stock'].quantile(0.8)
    median_stock = median_stock_vec[datf_idx['year']]
    datf_idx['size_bin'] = datf_idx['stock'] > median_stock

    # group by age and year
    median_age_vec = all_year_groups['age'].quantile(0.5)
    median_age = median_age_vec[datf_idx['year']]
    datf_idx['age_bin'] = datf_idx['age'] > median_age

    # merge in transfers
    trans_cols = ['size_bin','age_bin','stock_rank','age_rank','stock','age']
    datf_idx_sub = datf_idx.filter(['fnum','year']+trans_cols)
    datf_trans_merge = pd.merge(datf_trans,datf_idx_sub,how='left',left_on=['dest_fn','year'],right_on=['fnum','year'])
    datf_trans_merge = datf_trans_merge.rename(columns=dict([(s,s+'_dest') for s in ['fnum']+trans_cols]))
    datf_trans_merge = pd.merge(datf_trans_merge,datf_idx_sub,how='left',left_on=['source_fn','year'],right_on=['fnum','year'])
    datf_trans_merge = datf_trans_merge.rename(columns=dict([(s,s+'_source') for s in ['fnum']+trans_cols]))

    # three groups - no_match(0),match_small(1),match_large(2)
    datf_trans_merge['size_bin_source'] += 1
    datf_trans_merge['size_bin_dest'] += 1
    datf_trans_merge['age_bin_source'] += 1
    datf_trans_merge['age_bin_dest'] += 1
    datf_trans_merge.fillna({'size_bin_source':0,'size_bin_dest':0,'age_bin_source':0,'age_bin_dest':0},inplace=True)

    trans_size_up = (datf_trans_merge['stock_dest'] > datf_trans_merge['stock_source']).astype(np.float)
    trans_size_up[datf_trans_merge['stock_dest'].isnull()|datf_trans_merge['stock_source'].isnull()] = np.nan
    trans_age_up = (datf_trans_merge['age_dest'] > datf_trans_merge['age_source']).astype(np.float)
    trans_age_up[datf_trans_merge['age_dest'].isnull()|datf_trans_merge['age_source'].isnull()] = np.nan
    datf_trans_merge['trans_size_up'] = trans_size_up
    datf_trans_merge['trans_age_up'] = trans_age_up

    # group by year
    trans_year_groups = datf_trans_merge.groupby('year')
    trans_year_sums = trans_year_groups.size()
    trans_year_size_up = trans_year_groups['trans_size_up'].mean()
    trans_year_age_up = trans_year_groups['trans_age_up'].mean()

    # group by size transition type
    trans_size_year_groups = datf_trans_merge.groupby(('size_bin_source','size_bin_dest','year'))
    trans_size_year_sums = trans_size_year_groups.size()
    trans_size_year_fracs = trans_size_year_sums.astype(np.float)/trans_year_sums.reindex(trans_size_year_sums.index,level='year')

    # group by age transition type
    trans_age_year_groups = datf_trans_merge.groupby(('age_bin_source','age_bin_dest','year'))
    trans_age_year_sums = trans_age_year_groups.size()
    trans_age_year_fracs = trans_age_year_sums.astype(np.float)/trans_year_sums.reindex(trans_age_year_sums.index,level='year')

#if __name__ == "__main__":
#  # execution state
#  if len(sys.argv) == 1:
#    stage = 0
#  else:
#    stage = int(sys.argv[1])
#
#  main(stage)

