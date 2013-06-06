import numpy as np
import sqlite3
import pandas as pd
import sys
import itertools
import scipy.stats as stats
import data_tools as dt

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

run0 = True
run1 = True
run2 = True
run3 = False
run4 = True

# load in data from db
if stage_min <= 0 and stage_max >= 0 and run0:
    # load data
    print 'Loading data'

    conn = sqlite3.connect('store/within.db')
    cur = conn.cursor()

    # load firm data
    datf = pd.DataFrame(cur.execute('select firm_num,year,source_nbulk,source_pnum,dest_nbulk,dest_pnum,file_pnum,grant_pnum,income,revenue,rnd,employ,naics from firmyear_info where year>=1950').fetchall(),columns=['fnum','year','source_bulk','source','dest_bulk','dest','file','grant','income','revenue','rnd','employ','naics'])
    firm_info = pd.DataFrame(data=cur.execute('select firm_num,year_min,year_max,life_span,high_tech from firm_life').fetchall(),columns=['fnum','zero_year','max_year','life_span','high_tech'])
    # firm_life starts a firm when they file for their first patent and ends when they file for their last

    # close db
    conn.close()

    # make index
    print 'Reindexing'

    fnum_set = firm_info['fnum']
    zero_year = firm_info['zero_year']
    max_year = firm_info['max_year']
    life_span = firm_info['life_span']
    all_fnums = np.array(list(itertools.chain.from_iterable([[fnum]*life for (fnum,life) in zip(fnum_set,life_span)])),dtype=np.int)
    all_years = np.array(list(itertools.chain.from_iterable([xrange(x,y+1) for (x,y) in zip(zero_year,max_year)])),dtype=np.int)
    fy_all = pd.DataFrame(data={'fnum': all_fnums, 'year': all_years})
    datf_idx = fy_all.merge(datf,how='left',on=['fnum','year']).fillna(value={'file':0,'dest':0,'source':0,'dest_bulk':0,'source_bulk':0},inplace=True)

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
    datf_idx['count'] = 1.0

    #### select only high tech firms ####
    #datf_idx = datf_idx[datf_idx['ht_bin']]

    # next period values
    #next_year = pd.DataFrame(data={'fnum': datf_idx['fnum'], 'year': datf_idx['year']+1})
    #next_info = next_year.merge(datf_idx.filter(['fnum','year','file','source','dest','revenue','income']),how='left',on=['fnum','year'])

if stage_min <= 1 and stage_max >= 1 and run1:
    # construct patent stocks
    print 'Constructing patent stocks'

    datf_idx['patnet'] = datf_idx['file'] + datf_idx['dest'] - datf_idx['source'] - datf_idx['file_expire'] - datf_idx['dest_expire']
    firm_groups = datf_idx.groupby('fnum')
    datf_idx['stock'] = firm_groups['patnet'].cumsum() - datf_idx['patnet']
    datf_idx = datf_idx[datf_idx['stock']>0]
    datf_idx['patneti'] = datf_idx['patnet']/datf_idx['stock']

if stage_min <= 2 and stage_max >= 2 and run2:
    # patent fractions
    print 'Firm statistics'

    #### select only large-ish firms ####
    #datf_idx = datf_idx[datf_idx['stock']>=10]
    datf_idx = datf_idx[(datf_idx['year']>=1980)&(datf_idx['year']<=2008)]

    # basic stats
    datf_idx['file_frac'] = dt.noinf(datf_idx['file']/datf_idx['stock'])
    datf_idx['dest_frac'] = dt.noinf(datf_idx['dest']/datf_idx['stock'])
    datf_idx['source_frac'] = dt.noinf(datf_idx['source']/datf_idx['stock'])
    datf_idx['dest_share'] = dt.noinf(datf_idx['dest']/(datf_idx['dest']+datf_idx['file']))
    datf_idx['source_share'] = dt.noinf(datf_idx['source']/(datf_idx['source']+datf_idx['file']))

    # group by year
    all_year_groups = datf_idx.groupby('year')

    # group by size-year
    qcut_size = 0.8
    median_stock_vec = all_year_groups['stock'].quantile(qcut_size)
    median_stock = median_stock_vec[datf_idx['year']]
    datf_idx['stock_bin'] = datf_idx['stock'] > median_stock

    size_year_groups = datf_idx.groupby(('stock_bin','year'))
    size_groups = datf_idx.groupby(('stock_bin'))

    mean_stock_vec = all_year_groups['stock'].mean()
    mean_stock = mean_stock_vec[datf_idx['year']].values
    datf_idx['stock_year_norm'] = datf_idx['stock']/mean_stock

    # group by age-year

    #qcut_age = 0.5
    #median_age_vec = all_year_groups['age'].quantile(qcut_age)
    #median_age = median_age_vec[datf_idx['year']]
    #datf_idx['age_bin'] = datf_idx['age'] > median_age
    age_cut_1 = 5
    age_cut_2 = 10
    datf_idx['age_bin'] = (datf_idx['age']>age_cut_1).astype(np.int) + (datf_idx['age']>age_cut_2).astype(np.int)
    #datf_idx['age_bin'] = datf_idx['age'] > 10

    age_year_groups = datf_idx.groupby(('age_bin','year'))
    age_groups = datf_idx.groupby(('age_bin'))

    mean_age_vec = all_year_groups['age'].mean()
    mean_age = mean_age_vec[datf_idx['year']].values
    datf_idx['age_year_norm'] = datf_idx['age']/mean_age

    # start aggregating
    year_sums = all_year_groups.sum()
    year_means = all_year_groups.mean()

    size_year_sums = size_year_groups.sum()
    size_year_means = size_year_groups.mean()
    size_year_fracs = size_year_sums/year_sums.reindex(size_year_sums.index,level='year')
    means_by_size = size_year_means.unstack(0)
    sums_by_size = size_year_sums.unstack(0)
    fracs_by_size = size_year_fracs.unstack(0)

    age_year_sums = age_year_groups.sum()
    age_year_means = age_year_groups.mean()
    age_year_fracs = age_year_sums/year_sums.reindex(age_year_sums.index,level='year')
    means_by_age = age_year_means.unstack(0)
    sums_by_age = age_year_sums.unstack(0)
    fracs_by_age = age_year_fracs.unstack(0)

if stage_min <= 3 and stage_max >= 3 and run3:
    # firm level analysis
    print 'Firm level analysis'

    # period 1
    base_year = 2000
    period_len = 5
    top_year = base_year + period_len
    columns = ['fnum','year','stock','age','file','source','dest','trans','income','revenue','stock_bin','age_bin','ht_bin']

    firm_panel = datf_idx.filter(columns)
    firm_panel = firm_panel[(firm_panel['year']>=base_year)&(firm_panel['year']<top_year)]
    firm_panel['net_dest'] = firm_panel['dest'] > firm_panel['source']
    firm_panel['pos_source'] = firm_panel['source'] > 0
    firm_panel['pos_dest'] = firm_panel['dest'] > 0
    firm_panel['pos_trans'] = firm_panel['trans'] > 0
    firm_panel['pos_both'] = firm_panel['pos_source'] & firm_panel['pos_dest']

    firm_groups = firm_panel.groupby('fnum')
    firm_totals = firm_groups.sum().filter(['file','source','dest','income','revenue','employ','net_dest','pos_source','pos_dest','pos_trans','pos_both'])
    firm_totals['obs_years'] = firm_groups['fnum'].count()
    firm_totals['zero_year'] = firm_groups['year'].apply(lambda df: df.irow(0))
    firm_totals['age'] = firm_groups['age'].apply(lambda df: df.irow(0))
    firm_totals['stock'] = firm_groups['stock'].apply(lambda df: df.irow(0))
    firm_totals['stock_bin'] = firm_groups['stock_bin'].aggregate(lambda df: df.irow(0))
    firm_totals['age_bin'] = firm_groups['age_bin'].aggregate(lambda df: df.irow(0))
    firm_totals['ht_bin'] = firm_groups['ht_bin'].aggregate(lambda df: df.irow(0))
    firm_totals = firm_totals[firm_totals['zero_year']==base_year]
    firm_totals = firm_totals.drop(['zero_year'],axis=1)

    firm_totals['profit'] = dt.noinf(firm_totals['income']/firm_totals['revenue'])
    firm_totals['file_frac'] = dt.noinf(firm_totals['file']/firm_totals['stock'])
    firm_totals['source_frac'] = dt.noinf(firm_totals['source']/firm_totals['stock'])
    firm_totals['dest_frac'] = dt.noinf(firm_totals['dest']/firm_totals['stock'])
    firm_totals['dest_share'] = dt.noinf(firm_totals['dest']/(firm_totals['dest']+firm_totals['file']))
    firm_totals['source_share'] = dt.noinf(firm_totals['source']/(firm_totals['source']+firm_totals['file']))
    firm_totals['file_frac_bin'] = firm_totals['file_frac'] > firm_totals['file_frac'].median()

    total_means = firm_totals.mean()
    total_medians = firm_totals.median()

    sbin_groups = firm_totals.groupby('stock_bin')
    sbin_means = sbin_groups.mean()
    sbin_medians = sbin_groups.median()
    sbin_counts = sbin_groups['stock_bin'].count()

    abin_groups = firm_totals.groupby('age_bin')
    abin_means = abin_groups.mean()
    abin_medians = abin_groups.median()
    abin_counts = abin_groups['age_bin'].count()

    tbin_groups = firm_totals.groupby('ht_bin')
    tbin_means = tbin_groups.mean()
    tbin_medians = tbin_groups.median()
    tbin_counts = tbin_groups['ht_bin'].count()

    ibin_groups = firm_totals.groupby('file_frac_bin')
    ibin_means = ibin_groups.mean()
    ibin_medians = ibin_groups.median()
    ibin_counts = ibin_groups['file_frac_bin'].count()

if stage_min <= 4 and stage_max >= 4 and run4:
    # high growth firms
    print 'High growth firm analysis'

    age_cut_0 = 5
    age_cut_1 = 10
    age_cut_2 = 15

    datf_age_0 = datf_idx[(datf_idx['age']>age_cut_0)&(datf_idx['age']<=age_cut_1)]
    datf_age_1 = datf_idx[(datf_idx['age']>age_cut_1)&(datf_idx['age']<=age_cut_2)]

    firm_groups_age_0 = datf_age_0.groupby('fnum')
    firm_groups_age_1 = datf_age_1.groupby('fnum')

    file_frac_0 = dt.noinf(firm_groups_age_0.apply(lambda df: df['file'].sum()/df['stock'].irow(0)))
    file_frac_1 = dt.noinf(firm_groups_age_1.apply(lambda df: df['file'].sum()/df['stock'].irow(0)))
    dest_frac_1 = dt.noinf(firm_groups_age_1.apply(lambda df: df['dest'].sum()/df['stock'].irow(0)))
    source_frac_1 = dt.noinf(firm_groups_age_1.apply(lambda df: df['source'].sum()/df['stock'].irow(0)))
    dest_sum_1 = firm_groups_age_1['dest'].sum()
    file_sum_1 = firm_groups_age_1['file'].sum()
    source_sum_1 = firm_groups_age_1['source'].sum()
    dest_pos_1 = dest_sum_1 > 0
    file_pos_1 = file_sum_1 > 0
    source_pos_1 = source_sum_1 > 0

    mid_growth_0 = (file_frac_0 >= file_frac_0.quantile(0.5)) & (file_frac_0 < file_frac_0.quantile(0.9))
    high_growth_0 = file_frac_0 >= file_frac_0.quantile(0.9)
    firm_info = pd.concat([mid_growth_0,high_growth_0,dest_frac_1,dest_pos_1,file_frac_1,file_pos_1,source_frac_1,source_pos_1],axis=1)
    col_names = ['mid_growth_0','high_growth_0','dest_frac_1','dest_pos_1','file_frac_1','file_pos_1','source_frac_1','source_pos_1']
    firm_info = firm_info.rename(columns=dict(enumerate(col_names)))
    firm_info['mid_growth_0'] = firm_info['mid_growth_0'].fillna(False).astype(np.bool)
    firm_info['high_growth_0'] = firm_info['high_growth_0'].fillna(False).astype(np.bool)

    for col in col_names[2:]:
      mid_stat = firm_info[col][firm_info['mid_growth_0']].mean()
      high_stat = firm_info[col][firm_info['high_growth_0']].mean()
      print col + ': %f, %f' % (mid_stat,high_stat)

    mid_exit = firm_info['dest_pos_1'][firm_info['mid_growth_0']].isnull().mean()
    high_exit = firm_info['dest_pos_1'][firm_info['high_growth_0']].isnull().mean()
    print 'exit: %f, %f' % (mid_exit,high_exit)






