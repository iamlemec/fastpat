import sys
import itertools
import numpy as np
import sqlite3
import pandas as pd
import pandas.io.sql as sqlio

# options
run_flags = [True,True,True]

# execution state
if len(sys.argv) == 1:
  stage_min = 0
  stage_max = sys.maxint
elif len(sys.argv) == 2:
  stage_min = int(sys.argv[1])
  stage_max = sys.maxint
else:
  stage_min = int(sys.argv[1])
  stage_max = int(sys.argv[2])

for i in range(len(run_flags)): run_flags[i] &= (stage_min <= i) & (stage_max >= i)

if run_flags[0]:
    # load data
    print 'Loading data'

    # load firm data
    con = sqlite3.connect('store/patents.db')
    datf_idx = sqlio.read_sql('select * from firmyear_index',con)
    firm_info = sqlio.read_sql('select * from firm_life',con)
    trans_info = sqlio.read_sql('select * from assignment_info where execyear!=\'\'',con)
    firm_cite_year = sqlio.read_sql('select * from firm_cite_year',con)
    con.close()

if run_flags[1]:
    # calculate transfer statistics
    print 'Calculating transfer statistics'

    # extra stats
    trans_info['patage'] = trans_info['execyear'] - trans_info['fileyear']
    trans_info['patage_grant'] = trans_info['execyear'] - trans_info['grantyear']

    datf_idx['trans'] = datf_idx['source_pnum']+datf_idx['dest_pnum']
    datf_idx['ht_bin'] = datf_idx['high_tech'] > 0.5

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
    datf_idx_sub = datf_idx[['firm_num','year']+trans_cols]
    trans_merge = pd.merge(trans_info,datf_idx_sub,how='left',left_on=['dest_fn','execyear'],right_on=['firm_num','year'])
    trans_merge = trans_merge.rename(columns=dict([(s,s+'_dest') for s in ['firm_num']+trans_cols]))
    trans_merge = pd.merge(trans_merge,datf_idx_sub,how='left',left_on=['source_fn','execyear'],right_on=['firm_num','year'])
    trans_merge = trans_merge.rename(columns=dict([(s,s+'_source') for s in ['firm_num']+trans_cols]))

    # three groups - no_match(0),match_small(1),match_large(2)
    trans_merge['size_bin_source'] += 1
    trans_merge['size_bin_dest'] += 1
    trans_merge['age_bin_source'] += 1
    trans_merge['age_bin_dest'] += 1
    trans_merge.fillna({'size_bin_source':0,'size_bin_dest':0,'age_bin_source':0,'age_bin_dest':0},inplace=True)

    trans_size_up = (trans_merge['stock_dest'] > trans_merge['stock_source']).astype(np.float)
    trans_size_up[trans_merge['stock_dest'].isnull()|trans_merge['stock_source'].isnull()] = np.nan
    trans_age_up = (trans_merge['age_dest'] > trans_merge['age_source']).astype(np.float)
    trans_age_up[trans_merge['age_dest'].isnull()|trans_merge['age_source'].isnull()] = np.nan
    trans_merge['trans_size_up'] = trans_size_up
    trans_merge['trans_age_up'] = trans_age_up

    # group by year
    trans_year_groups = trans_merge.groupby('execyear')
    trans_year_sums = trans_year_groups.size()
    trans_year_size_up = trans_year_groups['trans_size_up'].mean()
    trans_year_age_up = trans_year_groups['trans_age_up'].mean()

    # group by size transition type
    trans_size_year_groups = trans_merge.groupby(('size_bin_source','size_bin_dest','execyear'))
    trans_size_year_sums = trans_size_year_groups.size()
    trans_size_year_fracs = trans_size_year_sums.astype(np.float)/trans_year_sums.reindex(trans_size_year_sums.index,level='execyear')

    # group by age transition type
    trans_age_year_groups = trans_merge.groupby(('age_bin_source','age_bin_dest','execyear'))
    trans_age_year_sums = trans_age_year_groups.size()
    trans_age_year_fracs = trans_age_year_sums.astype(np.float)/trans_year_sums.reindex(trans_age_year_sums.index,level='execyear')

if run_flags[2]:
    base_year = 1995
    period_len = 5
    top_year = base_year + period_len

    # panel of all firms in year range
    firm_info_panel = firm_info[(firm_info['year_max']>=base_year)&(firm_info['year_min']<top_year)]

    # panel of all citing firm pairs in year range
    firm_cite_panel = firm_cite_year[(firm_cite_year['cite_year']>=base_year)&(firm_cite_year['cite_year']<top_year)]
    firm_cite_merge = firm_cite_panel.merge(firm_info[['firm_num','mode_class']],how='left',left_on='citer_fnum',right_on='firm_num').drop('firm_num',axis=1).rename(columns={'mode_class':'citer_mode_class'})
    firm_cite_merge = firm_cite_merge.merge(firm_info[['firm_num','mode_class']],how='left',left_on='citee_fnum',right_on='firm_num').drop('firm_num',axis=1).rename(columns={'mode_class':'citee_mode_class'})
    firm_cite_within = firm_cite_merge[firm_cite_merge['citer_mode_class']==firm_cite_merge['citee_mode_class']].drop('citee_mode_class',axis=1).rename(columns={'citer_mode_class':'mode_class'})

    # citation rates
    firm_panel_class_count = firm_info_panel.groupby('mode_class').size()
    within_cite_class_count = firm_cite_within.groupby('mode_class').size()
    cite_pair_class_frac = within_cite_class_count.astype(np.float)/(firm_panel_class_count**2)
    cite_pair_class_agg = within_cite_class_count.sum().astype(np.float)/(firm_panel_class_count**2).sum()
