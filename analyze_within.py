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
run3 = True
run4 = False
run5 = False


# load in data from db
if stage_min <= 0 and stage_max >= 0 and run0:
    # load data
    print 'Loading data'

    conn = sqlite3.connect('store/within.db')
    cur = conn.cursor()

    # load firm data
    datf = pd.DataFrame(cur.execute('select firm_num,year,source_nbulk,source_pnum,dest_nbulk,dest_pnum,file_pnum,grant_pnum,income,revenue,rnd,employ,cash,naics,sic from firmyear_info where year>=1950').fetchall(),columns=['fnum','year','source_bulk','source','dest_bulk','dest','file','grant','income','revenue','rnd','employ','cash','naics','sic'],dtype=np.int)
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
    datf_idx = fy_all.merge(datf,how='left',on=['fnum','year']).fillna(value={'file':0,'grant':0,'dest':0,'source':0,'dest_bulk':0,'source_bulk':0},inplace=True)

    # patent expiry (file + 17)
    datf_idx['year_17p'] = datf_idx['year'] + 17
    datf_idx = datf_idx.merge(datf_idx.filter(['fnum','year_17p','file','dest']),how='left',left_on=['fnum','year'],right_on=['fnum','year_17p'],suffixes=('','_expire'))
    datf_idx = datf_idx.drop(['year_17p','year_17p_expire'],axis=1)
    datf_idx = datf_idx.fillna({'file_expire':0,'dest_expire':0})

    # derivative columns
    datf_idx = datf_idx.merge(firm_info.filter(['fnum','zero_year','max_year','life_span','has_comp','has_revn','has_rnd','has_pats','pats_tot','high_tech']),how='left',on='fnum')
    datf_idx['age'] = datf_idx['year']-datf_idx['zero_year']
    datf_idx['trans'] = datf_idx['source']+datf_idx['dest']
    datf_idx['trans_net'] = datf_idx['dest']-datf_idx['source']
    datf_idx['ht_bin'] = datf_idx['high_tech'] > 0.9
    datf_idx['count'] = 1.0

    datf_idx['profit'] = dt.noinf(datf_idx['income']/datf_idx['revenue'])
    datf_idx['prod'] = dt.noinf(datf_idx['income']/datf_idx['employ'])
    datf_idx['rndi'] = dt.noinf(datf_idx['rnd']/datf_idx['revenue'])
    datf_idx['naics'] = datf_idx['naics'].replace({None:0,np.nan:0,'':0}).astype(np.int)
    datf_idx['naics3'] = datf_idx['naics']/1000 # surprisingly, this works as desired
    datf_idx['naics2'] = datf_idx['naics3']/10

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
    datf_idx['file_cum'] = firm_groups['file'].cumsum() - datf_idx['file']
    datf_idx = datf_idx[datf_idx['stock']>0]
    datf_idx['patneti'] = datf_idx['patnet']/datf_idx['stock']

if stage_min <= 2 and stage_max >= 2 and run2:
    # patent fractions
    print 'Firm statistics'

    #### select only large-ish firms ####
    #datf_idx = datf_idx[datf_idx['stock']>=10]
    datf_idx = datf_idx[(datf_idx['year']>=1994)&(datf_idx['year']<=2006)] # default

    # basic stats
    datf_idx['file_frac'] = dt.noinf(datf_idx['file']/datf_idx['stock'])
    datf_idx['dest_frac'] = dt.noinf(datf_idx['dest']/datf_idx['stock'])
    datf_idx['source_frac'] = dt.noinf(datf_idx['source']/datf_idx['stock'])
    datf_idx['trans_net_frac'] = dt.noinf(datf_idx['trans_net']/datf_idx['stock'])
    datf_idx['dest_share'] = dt.noinf(datf_idx['dest']/(datf_idx['dest']+datf_idx['grant']))
    datf_idx['source_share'] = dt.noinf(datf_idx['source']/(datf_idx['source']+datf_idx['grant']))
    datf_idx['trans_net_share'] = dt.noinf(datf_idx['trans_net']/datf_idx['grant']) 
    datf_idx['growth'] = dt.noinf(datf_idx['file']/(datf_idx['stock']+0.5*datf_idx['patnet']))

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
    #age_cut_1 = 5
    #age_cut_2 = 10
    #datf_idx['age_bin'] = (datf_idx['age']>age_cut_1).astype(np.int) + (datf_idx['age']>age_cut_2).astype(np.int)
    datf_idx['age_bin'] = datf_idx['age'] > 10

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
    size_year_medians = size_year_groups.median()
    size_year_fracs = size_year_sums/year_sums.reindex(size_year_sums.index,level='year')
    means_by_size = size_year_means.unstack(0)
    sums_by_size = size_year_sums.unstack(0)
    fracs_by_size = size_year_fracs.unstack(0)

    age_year_sums = age_year_groups.sum()
    age_year_means = age_year_groups.mean()
    age_year_medians = age_year_groups.median()
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
    int_cols = ['fnum','year','naics2','naics3']
    float_cols = ['income','revenue','employ','rnd','stock','age','file','grant','source','dest','trans']
    bool_cols = ['stock_bin','age_bin','ht_bin']
    columns = int_cols + float_cols + bool_cols

    # select our target data
    firm_panel = datf_idx.filter(columns)
    firm_panel = firm_panel[(firm_panel['year']>=base_year)&(firm_panel['year']<top_year)]

    # top level aggregate
    firm_groups = firm_panel.groupby('fnum')
    firm_totals = firm_groups.sum().filter(['file','grant','source','dest','income','revenue','employ','rnd'])
    firm_totals['obs_years'] = firm_groups['fnum'].count()

    firm_start = firm_groups.apply(lambda df: df.irow(0))
    firm_start[int_cols] = firm_start[int_cols].astype(np.int)
    firm_start[float_cols] = firm_start[float_cols].astype(np.float)
    firm_start[bool_cols] = firm_start[bool_cols].astype(np.bool)

    firm_end = firm_groups.apply(lambda df: df.irow(-1))
    firm_end[int_cols] = firm_end[int_cols].astype(np.int)
    firm_end[float_cols] = firm_end[float_cols].astype(np.float)
    firm_end[bool_cols] = firm_end[bool_cols].astype(np.bool)

    # firm characteristics at start of window
    firm_totals['zero_year'] = firm_start['year']
    firm_totals = pd.concat([firm_totals,firm_start.filter(['age','stock','stock_bin','age_bin','ht_bin','naics2','naics3'])],axis=1)

    # growth rates
    for col in ['employ','revenue','income']:
        firm_totals[col+'_start'] = firm_start[col]
        firm_totals[col+'_end'] = firm_end[col]
        firm_totals[col+'_change'] = firm_end[col] - firm_start[col]
        firm_totals[col+'_lgrowth'] = dt.noinf(np.log(firm_totals[col+'_end']/firm_totals[col+'_start'])/(firm_totals['obs_years']-1))

    # select those that exist at start of window
    firm_totals = firm_totals[firm_totals['zero_year']==base_year]
    firm_totals = firm_totals.drop(['zero_year'],axis=1)

    firm_totals['profit'] = dt.noinf(firm_totals['income']/firm_totals['revenue'])
    firm_totals['file_frac'] = dt.noinf(firm_totals['file']/firm_totals['stock'])
    firm_totals['source_frac'] = dt.noinf(firm_totals['source']/firm_totals['stock'])
    firm_totals['dest_frac'] = dt.noinf(firm_totals['dest']/firm_totals['stock'])
    firm_totals['dest_share'] = dt.noinf(firm_totals['dest']/(firm_totals['dest']+firm_totals['grant']))
    firm_totals['source_share'] = dt.noinf(firm_totals['source']/(firm_totals['source']+firm_totals['grant']))
    firm_totals['file_frac_bin'] = firm_totals['file_frac'] > firm_totals['file_frac'].median()
    firm_totals['profit'] = dt.noinf(firm_totals['income']/firm_totals['revenue'])

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
    print 'High growth firm analysis (1)'

    # type one analysis
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

if stage_min <= 5 and stage_max >= 5 and run5:
    print 'High growth firm analysis (2)'

    # type two analysis
    datf_hg = datf_idx.filter(['fnum','year','file','grant','source','dest','source_bulk','dest_bulk','source_share','dest_share','age','stock','file_cum','file_frac','dest_frac','source_frac','trans_net','trans_net_frac','high_tech','count'])
    growth_window = 5
    datf_hg['year_5m'] = datf_hg['year'] - growth_window
    datf_hg = datf_hg.merge(datf_hg.filter(['fnum','year_5m','file_cum']),how='left',left_on=['fnum','year'],right_on=['fnum','year_5m'],suffixes=('','_5year'))
    datf_hg = datf_hg.drop(['year_5m','year_5m_5year'],axis=1)
    #datf_idx = datf_idx.fillna({'file_cum_5year':0})
    datf_hg['file_5growth'] = dt.noinf((datf_hg['file_cum_5year']-datf_hg['file_cum'])/datf_hg['stock'])
    datf_hg['high_growth'] = (datf_hg['file_5growth']>1.0) & (datf_hg['stock']>=10)
    datf_spurts = datf_hg[datf_hg['high_growth']]
    first_spurt = datf_spurts.groupby('fnum')['age'].min()
    datf_hg = datf_hg.join(first_spurt,on='fnum',rsuffix='_spurt')
    datf_hg['since_spurt'] = datf_hg['age']-datf_hg['age_spurt']+1
    datf_hg['firm_hg'] = ~datf_hg['age_spurt'].isnull()

    high_firms = datf_hg[datf_hg['high_growth']]['fnum'].unique()
    is_high_firm = datf_hg['fnum'].isin(high_firms)
    is_not_tiny = datf_hg['stock'] >= 10
    min_years = datf_hg.groupby('fnum')['year'].min()
    min_years.name = 'min_year'
    datf_hg = datf_hg.join(min_years,on='fnum')
    born_here = datf_hg['min_year'] > 1981

    datf_allf = datf_hg[is_not_tiny&born_here]
    datf_best = datf_hg[is_high_firm&is_not_tiny&born_here]
    datf_norm = datf_hg[~is_high_firm&is_not_tiny&born_here]

    stock_spurt = datf_best[datf_best['age']==datf_best['age_spurt']].filter(['fnum','stock']).set_index('fnum').ix[datf_best['fnum']]
    datf_best['stock_spurt'] = stock_spurt.values
    datf_best['dest_spurt'] = datf_best['dest']/datf_best['stock_spurt']
    datf_best['source_spurt'] = datf_best['source']/datf_best['stock_spurt']
    datf_best['file_spurt'] = datf_best['file']/datf_best['stock_spurt']
    datf_best['grant_spurt'] = datf_best['grant']/datf_best['stock_spurt']

    allf_age_groups = datf_allf.groupby('age')
    best_age_groups = datf_best.groupby('age')
    norm_age_groups = datf_norm.groupby('age')

    best_spurt_groups = datf_best.groupby('since_spurt')


