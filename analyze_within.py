import numpy as np
import sqlite3
import pandas as pd
import sys
import itertools
import scipy.stats as stats
import data_tools as dt
import pandas.io.sql as sqlio

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

# load in data from db
if stage_min <= 0 and stage_max >= 0 and run0:
    # load data
    print 'Loading data'

    # load firm data
    # firm_life starts a firm when they file for their first patent and ends when they file for their last
    con = sqlite3.connect('store/within.db')
    firmyear_info = sqlio.read_frame('select * from firmyear_info',con)
    firm_info = sqlio.read_frame('select * from firm_life',con)
    grant_info = sqlio.read_frame('select * from grant_info',con)
    trans_info = sqlio.read_frame('select * from assign_info',con)
    con.close()

    # make index
    print 'Reindexing'

    fnum_set = firm_info['firm_num']
    year_min = firm_info['year_min']
    year_max = firm_info['year_max']
    life_span = firm_info['life_span']
    all_fnums = np.array(list(itertools.chain.from_iterable([[fnum]*life for (fnum,life) in zip(fnum_set,life_span)])),dtype=np.int)
    all_years = np.array(list(itertools.chain.from_iterable([xrange(x,y+1) for (x,y) in zip(year_min,year_max)])),dtype=np.int)
    fy_all = pd.DataFrame(data={'firm_num': all_fnums, 'year': all_years})
    datf_idx = fy_all.merge(firmyear_info,how='left',on=['firm_num','year'])
    datf_idx.fillna(value={'file_pnum':0,'grant_pnum':0,'dest_pnum':0,'source_pnum':0,'dest_nbulk':0,'source_nbulk':0},inplace=True)

    # compustat masks
    datf_idx['has_comp'] = ~datf_idx['revenue'].isnull()
    #datf_idx[datf_idx['has_comp']] = datf_idx[datf_idx['has_comp']].fillna({'rnd':0.0,'acquire':0.0})

    # derivative columns
    datf_idx = datf_idx.merge(firm_info,how='left',on='firm_num')
    datf_idx['age'] = datf_idx['year']-datf_idx['year_min']
    datf_idx['naics'] = datf_idx['naics'].fillna(0).astype(np.int)
    datf_idx['naics3'] = datf_idx['naics']/1000 # surprisingly, this works as desired
    datf_idx['naics2'] = datf_idx['naics3']/10

    # top level patent class info
    print 'Toplevel industry grant info'

    grant_class_groups = grant_info.groupby('classone')
    grant_class_born = grant_class_groups['fileyear'].quantile(0.01)
    grant_class_base = pd.DataFrame({'class_born':grant_class_born})
    grant_class_base['class_age'] = 2013 - grant_class_base['class_born']

    # construct patent stocks
    print 'Constructing patent stocks'

    datf_idx['patnet'] = datf_idx['file_pnum'] - datf_idx['expire_pnum']
    firm_groups = datf_idx.groupby('firm_num')
    datf_idx['stock'] = firm_groups['patnet'].cumsum() - datf_idx['patnet']
    datf_idx['file_cum'] = firm_groups['file_pnum'].cumsum() - datf_idx['file_pnum']
    datf_idx = datf_idx[datf_idx['stock']>0]
    datf_idx['patneti'] = datf_idx['patnet']/datf_idx['stock']

if stage_min <= 1 and stage_max >= 1 and run1:
    # patent fractions
    print 'Firm statistics'

    # basic stats
    datf_idx['trans_pnum'] = datf_idx['source_pnum']+datf_idx['dest_pnum']
    datf_idx['trans_net'] = datf_idx['dest_pnum']-datf_idx['source_pnum']
    datf_idx['ht_bin'] = datf_idx['high_tech'] > 0.9
    datf_idx['profit'] = dt.noinf(datf_idx['income']/datf_idx['revenue'])
    datf_idx['prod'] = dt.noinf(datf_idx['income']/datf_idx['employ'])
    datf_idx['rndi'] = dt.noinf(datf_idx['rnd']/datf_idx['revenue'])
    datf_idx['file_frac'] = dt.noinf(datf_idx['file_pnum']/datf_idx['stock'])
    datf_idx['dest_frac'] = dt.noinf(datf_idx['dest_pnum']/datf_idx['stock'])
    datf_idx['source_frac'] = dt.noinf(datf_idx['source_pnum']/datf_idx['stock'])
    datf_idx['trans_net_frac'] = dt.noinf(datf_idx['trans_net']/datf_idx['stock'])
    datf_idx['dest_share'] = dt.noinf(datf_idx['dest_pnum']/(datf_idx['dest_pnum']+datf_idx['grant_pnum']))
    datf_idx['source_share'] = dt.noinf(datf_idx['source_pnum']/(datf_idx['source_pnum']+datf_idx['grant_pnum']))
    datf_idx['trans_net_share'] = dt.noinf(datf_idx['trans_net']/datf_idx['grant_pnum'])
    datf_idx['growth'] = dt.noinf(datf_idx['file_pnum']/(datf_idx['stock']+0.5*datf_idx['patnet']))

    # group by year
    all_year_groups = datf_idx.groupby('year')

    # group by size-year
    qcut_size = 0.8
    median_stock_vec = all_year_groups['stock'].quantile(qcut_size)
    median_stock = median_stock_vec[datf_idx['year']]
    datf_idx['stock_bin'] = datf_idx['stock'] > median_stock

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

    mean_age_vec = all_year_groups['age'].mean()
    mean_age = mean_age_vec[datf_idx['year']].values
    datf_idx['age_year_norm'] = datf_idx['age']/mean_age

    # start aggregating
    #### select only large-ish firms ####
    #datf_idx = datf_idx[datf_idx['stock']>=10]
    datf_stats = datf_idx[(datf_idx['year']>=1994)&(datf_idx['year']<=2006)] # default

    size_year_groups = datf_stats.groupby(('stock_bin','year'))
    size_groups = datf_stats.groupby(('stock_bin'))

    age_year_groups = datf_stats.groupby(('age_bin','year'))
    age_groups = datf_stats.groupby(('age_bin'))

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

if stage_min <= 2 and stage_max >= 2 and run2:
    # firm level analysis
    print 'Firm/industry level analysis'

    # period 1
    base_year = 1995
    period_len = 5
    top_year = base_year + period_len
    type_int_cols = ['firm_num','year','naics2','naics3','mode_class','age']
    type_float_cols = ['employ','revenue','income','stock','mktval','intan','assets','mode_frac','high_tech']
    type_bool_cols = ['stock_bin','age_bin','ht_bin']
    sum_float_cols = ['assets','capx','cash','cogs','deprec','intan','debt','employ','income','revenue','sales','rnd','fcost','mktval','acquire',
                      'file_pnum','grant_pnum','source_pnum','dest_pnum','trans_pnum','expire_pnum','n_cited','n_self_cited','n_citing']
    type_cols = type_int_cols + type_float_cols + type_bool_cols
    pure_type_cols = list(set(type_cols)-set(sum_float_cols))
    all_cols = list(set(type_cols+sum_float_cols))

    # select our target data
    firm_panel = datf_idx[all_cols]
    firm_panel = firm_panel[(firm_panel['year']>=base_year)&(firm_panel['year']<top_year)]

    # top level aggregate
    firm_groups = firm_panel.groupby('firm_num')
    firm_totals = firm_groups.sum()[sum_float_cols]
    firm_totals['obs_years'] = firm_groups.size()

    firm_start = firm_groups.apply(lambda df: df.irow(0))
    firm_start[type_int_cols] = firm_start[type_int_cols].astype(np.int)
    firm_start[type_float_cols] = firm_start[type_float_cols].astype(np.float)
    firm_start[type_bool_cols] = firm_start[type_bool_cols].astype(np.bool)

    firm_end = firm_groups.apply(lambda df: df.irow(-1))
    firm_end[type_int_cols] = firm_end[type_int_cols].astype(np.int)
    firm_end[type_float_cols] = firm_end[type_float_cols].astype(np.float)
    firm_end[type_bool_cols] = firm_end[type_bool_cols].astype(np.bool)

    # firm characteristics at start of window
    firm_totals['start_year'] = firm_start['year']
    firm_totals['end_year'] = firm_end['year']
    firm_totals[pure_type_cols] = firm_start[pure_type_cols]

    # exit rates
    firm_totals['entered'] = firm_totals['start_year'] > base_year
    firm_totals['exited'] = (firm_totals['obs_years'] < period_len) & ~firm_totals['entered']

    # growth rates
    for col in ['employ','revenue','income','stock','mktval','intan','assets']:
        firm_totals[col+'_start'] = firm_start[col]
        firm_totals[col+'_end'] = firm_end[col]
        firm_totals[col+'_change'] = firm_end[col] - firm_start[col]
        firm_totals[col+'_lgrowth'] = dt.noinf(np.log(firm_totals[col+'_end']/firm_totals[col+'_start'])/(firm_totals['obs_years']-1))
        firm_totals[col+'_lgrowth'].ix[firm_totals['entered']|firm_totals['exited']] = np.nan
        firm_totals[col+'_abs'] = np.abs(firm_totals[col+'_lgrowth'])

    # general firm statistics
    firm_totals['file_frac'] = dt.noinf(firm_totals['file_pnum']/firm_totals['stock_start'])
    firm_totals['source_frac'] = dt.noinf(firm_totals['source_pnum']/firm_totals['stock_start'])
    firm_totals['dest_frac'] = dt.noinf(firm_totals['dest_pnum']/firm_totals['stock_start'])
    firm_totals['dest_share'] = dt.noinf(firm_totals['dest_pnum']/(firm_totals['dest_pnum']+firm_totals['grant_pnum']))
    firm_totals['source_share'] = dt.noinf(firm_totals['source_pnum']/(firm_totals['source_pnum']+firm_totals['grant_pnum']))
    firm_totals['file_frac_bin'] = firm_totals['file_frac'] > firm_totals['file_frac'].median()

    firm_totals['cost'] = firm_totals['revenue'] - firm_totals['income']
    firm_totals['prod'] = dt.noinf(firm_totals['income']/firm_totals['employ'])
    firm_totals['rndi'] = dt.noinf(firm_totals['rnd']/firm_totals['revenue'])
    firm_totals['rndi_cost'] = dt.noinf(firm_totals['rnd']/firm_totals['cost'])
    firm_totals['profit'] = dt.noinf(firm_totals['income']/firm_totals['revenue'])
    firm_totals['rnd_prod'] = dt.noinf(firm_totals['file_pnum']/firm_totals['rnd'])
    firm_totals['markup'] = dt.noinf(firm_totals['revenue']/firm_totals['cogs'])
    firm_totals['cashi'] = dt.noinf(firm_totals['cash']/firm_totals['assets'])
    firm_totals['cost'] = firm_totals['fcost'] + firm_totals['cogs']
    firm_totals['fcost_frac'] = dt.noinf(firm_totals['fcost']/firm_totals['cost'])
    firm_totals['capx_frac'] = dt.noinf(firm_totals['capx']/firm_totals['assets'])
    firm_totals['intan_frac'] = dt.noinf(firm_totals['intan']/firm_totals['assets'])
    firm_totals['debt_frac'] = dt.noinf(firm_totals['debt']/firm_totals['assets'])
    firm_totals['tobinq'] = dt.noinf(firm_totals['mktval']/(firm_totals['assets']-firm_totals['debt']))
    firm_totals['capital'] = firm_totals['assets'] - firm_totals['intan']
    firm_totals['invest_cap'] = dt.noinf(firm_totals['capx']/firm_totals['capital'])
    firm_totals['invest_rnd'] = dt.noinf(firm_totals['rnd']/firm_totals['intan'])
    firm_totals['acquire_int'] = dt.noinf(np.log1p(firm_totals['acquire']/firm_totals['assets']))
    firm_totals['pos_dest'] = firm_totals['dest_pnum'] > 0
    firm_totals['pos_source'] = firm_totals['source_pnum'] > 0
    firm_totals['pos_trans'] = firm_totals['trans_pnum'] > 0
    firm_totals['pos_acquire'] = firm_totals['acquire'] > 0.0

    # # overall stats
    # total_means = firm_totals.mean()
    # total_medians = firm_totals.median()

    # # aggregation by various classifications
    # sbin_groups = firm_totals.groupby('stock_bin')
    # sbin_means = sbin_groups.mean()
    # sbin_medians = sbin_groups.median()
    # sbin_counts = sbin_groups.size()

    # abin_groups = firm_totals.groupby('age_bin')
    # abin_means = abin_groups.mean()
    # abin_medians = abin_groups.median()
    # abin_counts = abin_groups.size()

    # tbin_groups = firm_totals.groupby('ht_bin')
    # tbin_means = tbin_groups.mean()
    # tbin_medians = tbin_groups.median()
    # tbin_counts = tbin_groups.size()

    # ibin_groups = firm_totals.groupby('file_frac_bin')
    # ibin_means = ibin_groups.mean()
    # ibin_medians = ibin_groups.median()
    # ibin_counts = ibin_groups.size()

if stage_min <= 3 and stage_max >= 3 and run3:
    # modal classone group stats
    class_groups = firm_totals.groupby('mode_class')
    class_sums = class_groups.sum()
    class_means = class_groups.mean()
    class_medians = class_groups.median()
    class_stds = class_groups.std()
    class_skews = class_groups.skew()
    class_cvars = dt.noinf(class_stds/class_means)
    class_sfracs = dt.noinf(class_sums.apply(lambda df: df/class_sums['stock_start']))
    class_ffracs = dt.noinf(class_sums.apply(lambda df: df/class_sums['file_pnum']))
    class_lmeans = class_groups.apply(lambda df: np.log1p(df.astype(np.float)).mean())
    class_lstds = class_groups.apply(lambda df: np.log1p(df.astype(np.float)).std())
    class_counts = class_groups.size()

    # aggregate into mecha-df
    firm_class_info = dt.stack_frames([class_sums,class_means,class_medians,class_stds,class_skews,class_cvars,class_sfracs,class_ffracs,class_lmeans,class_lstds],postfixes=['_sum','_mean','_median','_std','_skew','_cvar','_sfrac','_ffrac','_lmean','_lstd'])
    firm_class_info['stock_skew'] = class_groups['stock_start'].apply(lambda s: np.log1p(s).std())
    firm_class_info['self_cited_frac'] = dt.noinf(class_sums['n_self_cited']/class_sums['n_cited'])
    firm_class_info['cites_per_patent'] = dt.noinf(class_sums['n_cited']/class_sums['file_pnum'])
    firm_class_info['agg_profit'] = dt.noinf(class_sums['income']/class_sums['revenue'])
    firm_class_info['agg_cashi'] = dt.noinf(class_sums['cash']/class_sums['revenue'])
    firm_class_info['agg_markup'] = dt.noinf(class_sums['revenue']/class_sums['cogs'])
    firm_class_info['agg_rndi'] = dt.noinf(class_sums['rnd']/class_sums['revenue'])
    firm_class_info['agg_invest'] = dt.noinf(class_sums['capx']/class_sums['capital'])
    firm_class_info['agg_rndprod'] = dt.noinf(class_sums['file_pnum']/class_sums['rnd'])
    firm_class_info['agg_acquire_int'] = dt.noinf(class_sums['acquire']/class_sums['assets'])
    firm_class_info['agg_dest_ffrac'] = dt.noinf(class_sums['dest_pnum']/class_sums['file_pnum'])
    firm_class_info['class_size'] = class_counts

    # pure patent stats
    firm_grants = grant_info[(grant_info['fileyear']>=base_year)&(grant_info['fileyear']<top_year)]
    firm_grants['pos_trans'] = firm_grants['ntrans'] > 0
    firm_grants['self_cited_frac'] = dt.noinf(firm_grants['n_self_cited'].astype(np.float)/firm_grants['n_cited'].astype(np.float))
    firm_grants['lots_self_cite'] = firm_grants['self_cited_frac'] >= 0.5
    firm_grants['expire_six'] = firm_grants['life_grant'] <= 4
    firm_grants['grant_lag'] = firm_grants['grantyear'] - firm_grants['fileyear']
    firm_grants['trans_lag'] = firm_grants['first_trans'] - firm_grants['fileyear']
    firm_grants['n_ext_cited'] = firm_grants['n_cited'] - firm_grants['n_self_cited']

    grant_class_groups = firm_grants.groupby('classone')
    grant_class_means = grant_class_groups.mean().rename(columns=dt.prefixer('grant_')).rename(columns=dt.postfixer('_mean')).drop(-1)
    grant_class_info = grant_class_means.join(grant_class_base)
    grant_class_info['class_number'] = grant_class_info.index

    # merge both levels
    datf_class = firm_class_info.join(grant_class_info)
