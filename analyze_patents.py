import sys
import itertools
import json
import argparse
import sqlite3
import numpy as np
import pandas as pd
import scipy.stats as stats

# parse input arguments
parser = argparse.ArgumentParser(description='Merge firm patent data.')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--stage_min', type=int, default=0, help='min stage to run')
parser.add_argument('--stage_max', type=int, default=sys.maxsize, help='max stage to run')
parser.add_argument('--base_year', type=int, default=1990, help='start year of window')
parser.add_argument('--period_len', type=int, default=10, help='length (years) of window')
parser.add_argument('--pclass', type=str, default='class', help='patent classification scheme')
args = parser.parse_args()

# translate into flags
run_flags = [True,True,True,True,True,True]
for i in range(len(run_flags)): run_flags[i] &= (args.stage_min <= i) & (args.stage_max >= i)

# firm panel years
base_year = args.base_year
period_len = args.period_len
top_year = base_year + period_len
pclass = args.pclass
mode_pclass = 'mode_' + pclass

# utils
def noinf(s):
    s[np.isinf(s)] = np.nan
    return s

def stack_frames(dfs,prefixes=None,suffixes=None):
    if prefixes is None:
        prefixes = len(dfs)*['']
    elif type(prefixes) not in (tuple,list):
        prefixes = len(dfs)*[prefixes]
    if suffixes is None:
        suffixes = len(dfs)*['']
    elif type(suffixes) not in (tuple,list):
        suffixes = len(dfs)*[suffixes]
    return pd.concat([df.add_prefix(pre).add_suffix(suf) for (df,pre,suf) in zip(dfs,prefixes,suffixes)],axis=1)

# stages
if run_flags[0]:
    print('Loading data')

    # load firm data
    # firm_life starts a firm when they file for their first patent and ends when they file for their last
    con = sqlite3.connect(args.db)
    datf_idx = pd.read_sql('select * from firmyear_index',con)
    firm_info = pd.read_sql('select * from firm_life',con)
    grant_info = pd.read_sql('select * from patent_info',con)
    trans_info = pd.read_sql('select * from assign_info where execyear!=\'\'',con)
    con.close()

    # basic stats
    datf_idx['naics'] = datf_idx['naics'].fillna(0).astype(np.int)
    datf_idx['naics3'] = datf_idx['naics']/1000 # surprisingly, this works as desired
    datf_idx['naics2'] = datf_idx['naics3']/10
    datf_idx['trans_pnum'] = datf_idx['source_pnum']+datf_idx['dest_pnum']
    datf_idx['trans_net'] = datf_idx['dest_pnum']-datf_idx['source_pnum']
    datf_idx['ht_bin'] = datf_idx['high_tech'] > 0.9
    datf_idx['cost'] = datf_idx['revenue'] - datf_idx['income']
    datf_idx['profit'] = noinf(datf_idx['income']/datf_idx['cost'])
    datf_idx['prod'] = noinf(datf_idx['income']/datf_idx['employ'])
    datf_idx['rndi'] = noinf(datf_idx['rnd']/datf_idx['revenue'])
    datf_idx['file_frac'] = noinf(datf_idx['file_pnum']/datf_idx['stock'])
    datf_idx['dest_frac'] = noinf(datf_idx['dest_pnum']/datf_idx['stock'])
    datf_idx['source_frac'] = noinf(datf_idx['source_pnum']/datf_idx['stock'])
    datf_idx['trans_net_frac'] = noinf(datf_idx['trans_net']/datf_idx['stock'])
    datf_idx['dest_share'] = noinf(datf_idx['dest_pnum']/(datf_idx['dest_pnum']+datf_idx['grant_pnum']))
    datf_idx['source_share'] = noinf(datf_idx['source_pnum']/(datf_idx['source_pnum']+datf_idx['grant_pnum']))
    datf_idx['trans_net_share'] = noinf(datf_idx['trans_net']/datf_idx['grant_pnum'])
    datf_idx['n_ext_cited'] = datf_idx['n_cited'] - datf_idx['n_self_cited']
    datf_idx['growth'] = noinf(datf_idx['file_pnum']/(datf_idx['stock']+0.5*datf_idx['patnet']))

    # group by year
    all_year_groups = datf_idx.groupby('year')

    # group by size-year
    qcut_size = 0.8
    median_stock_vec = all_year_groups['stock'].quantile(qcut_size)
    median_stock = median_stock_vec[datf_idx['year']].values
    datf_idx['size_bin'] = datf_idx['stock'] > median_stock

    mean_stock_vec = all_year_groups['stock'].mean()
    mean_stock = mean_stock_vec[datf_idx['year']].values
    datf_idx['size_year_norm'] = datf_idx['stock']/mean_stock

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

    # normalize stock/age into yearly ranks
    rankify = lambda s: np.argsort(s).astype(np.float)/(len(s)-1)
    datf_idx['size_rank'] = all_year_groups['stock'].apply(rankify)
    datf_idx['age_rank'] = all_year_groups['age'].apply(rankify)

if run_flags[1]:
    print('Firm/industry level analysis')

    # data selection parameters
    index_cols = ['firm_num']
    type_int_cols = ['year','naics2','naics3','age']
    type_float_cols = ['employ','revenue','income','stock','mktval','intan','assets','mode_class_frac','mode_ipc_frac','high_tech']
    type_bool_cols = ['size_bin','age_bin','ht_bin']
    type_str_cols = ['mode_class','mode_ipc']
    sum_float_cols = ['assets','capx','cash','cogs','deprec','intan','debt','employ','income','revenue','sales','rnd','fcost','mktval','acquire',
                      'file_pnum','grant_pnum','source_pnum','dest_pnum','trans_pnum','expire_pnum','n_cited','n_self_cited','n_ext_cited','n_citing']
    type_cols = type_int_cols + type_float_cols + type_bool_cols + type_str_cols
    pure_type_cols = list(set(type_cols)-set(sum_float_cols))
    all_cols = list(set(index_cols+type_cols+sum_float_cols))

    # select our target data
    firm_panel = datf_idx[all_cols]
    firm_panel = firm_panel[(firm_panel['year']>=base_year)&(firm_panel['year']<top_year)]

    # top level aggregate
    firm_groups = firm_panel.groupby('firm_num')
    firm_totals = firm_groups.sum()[sum_float_cols]
    firm_totals['obs_years'] = firm_groups.size()

    firm_start = firm_groups.apply(lambda df: df.iloc[0])
    firm_start[type_int_cols] = firm_start[type_int_cols].astype(np.int)
    firm_start[type_float_cols] = firm_start[type_float_cols].astype(np.float)
    firm_start[type_bool_cols] = firm_start[type_bool_cols].astype(np.bool)

    firm_end = firm_groups.apply(lambda df: df.iloc[-1])
    firm_end[type_int_cols] = firm_end[type_int_cols].astype(np.int)
    firm_end[type_float_cols] = firm_end[type_float_cols].astype(np.float)
    firm_end[type_bool_cols] = firm_end[type_bool_cols].astype(np.bool)

    # firm characteristics at start of window
    firm_totals['count'] = 1
    firm_totals['start_year'] = firm_start['year']
    firm_totals['end_year'] = firm_end['year']
    firm_totals[pure_type_cols] = firm_start[pure_type_cols]

    # exit rates
    firm_totals['entered'] = firm_totals['start_year'] > base_year
    firm_totals['exited'] = (firm_totals['obs_years'] < period_len) & ~firm_totals['entered']

    # growth rates
    for col in ['employ','revenue','income','stock','mktval','intan','assets']:
        firm_totals[col+'_start'] = firm_start[col]
        firm_totals[col+'_start'][firm_totals['entered']] = np.nan # entrants have no start state
        firm_totals[col+'_end'] = firm_end[col]
        firm_totals[col+'_change'] = firm_end[col] - firm_start[col]
        firm_totals[col+'_lgrowth'] = noinf(np.log(firm_totals[col+'_end']/firm_totals[col+'_start'])/(firm_totals['obs_years']-1))
        firm_totals[col+'_lgrowth'].ix[firm_totals['entered']|firm_totals['exited']] = np.nan
        firm_totals[col+'_abs'] = np.abs(firm_totals[col+'_lgrowth'])

    # general firm statistics
    firm_totals['file_frac'] = noinf(firm_totals['file_pnum']/firm_totals['stock_start'])
    firm_totals['source_frac'] = noinf(firm_totals['source_pnum']/firm_totals['stock_start'])
    firm_totals['dest_frac'] = noinf(firm_totals['dest_pnum']/firm_totals['stock_start'])
    firm_totals['dest_share'] = noinf(firm_totals['dest_pnum']/(firm_totals['dest_pnum']+firm_totals['grant_pnum']))
    firm_totals['source_share'] = noinf(firm_totals['source_pnum']/(firm_totals['source_pnum']+firm_totals['grant_pnum']))
    firm_totals['file_frac_bin'] = firm_totals['file_frac'] > firm_totals['file_frac'].median()
    firm_totals['file_stock_frac'] = noinf(np.log1p(firm_totals['file_pnum']/firm_totals['stock_start']))/(period_len-1.0)

    firm_totals['cost'] = firm_totals['revenue'] - firm_totals['income']
    firm_totals['prod'] = noinf(firm_totals['income']/firm_totals['employ'])
    firm_totals['rndi'] = noinf(np.log1p(firm_totals['rnd']/firm_totals['revenue']))
    firm_totals['rndi_cost'] = noinf(np.log1p(firm_totals['rnd']/firm_totals['cost']))
    firm_totals['ros'] = noinf(firm_totals['income']/firm_totals['revenue'])
    firm_totals['lros'] = noinf(np.log(firm_totals['income']/firm_totals['revenue']))
    firm_totals['profit'] = noinf((firm_totals['income']+firm_totals['rnd'])/firm_totals['revenue'])
    firm_totals['rnd_prod'] = noinf(firm_totals['file_pnum']/firm_totals['rnd'])
    firm_totals['markup'] = noinf(firm_totals['revenue']/firm_totals['cogs'])-1.0
    firm_totals['profit_cost'] = noinf(firm_totals['revenue']/firm_totals['cost'])-1.0
    firm_totals['markup_capx'] = noinf(firm_totals['revenue']/(firm_totals['cogs']+firm_totals['capx']))-1.0
    firm_totals['cashi'] = noinf(firm_totals['cash']/firm_totals['assets'])
    firm_totals['cost'] = firm_totals['fcost'] + firm_totals['cogs']

    firm_totals['fcost_frac'] = noinf(firm_totals['fcost']/firm_totals['cost'])
    firm_totals['capx_frac'] = noinf(firm_totals['capx']/firm_totals['assets'])
    firm_totals['intan_frac'] = noinf(firm_totals['intan']/firm_totals['assets'])
    firm_totals['debt_frac'] = noinf(firm_totals['debt']/firm_totals['assets'])
    firm_totals['tobinq'] = noinf(firm_totals['mktval']/(firm_totals['assets']-firm_totals['debt']))
    firm_totals['capital'] = firm_totals['assets'] - firm_totals['intan']
    firm_totals['invest_cap'] = noinf(firm_totals['capx']/firm_totals['capital'])
    firm_totals['invest_rnd'] = noinf(firm_totals['rnd']/firm_totals['intan'])
    firm_totals['acquire_int'] = noinf(np.log1p(firm_totals['acquire']/firm_totals['assets']))
    firm_totals['pos_dest'] = firm_totals['dest_pnum'] > 0
    firm_totals['pos_source'] = firm_totals['source_pnum'] > 0
    firm_totals['pos_trans'] = firm_totals['trans_pnum'] > 0
    firm_totals['pos_acquire'] = firm_totals['acquire'] > 0.0

if run_flags[2]:
    print('Firm type breakdowns')

    firm_incumbents = firm_totals[~firm_totals['entered']]
    firm_incumbents = firm_incumbents.drop(['entered', 'mode_class', 'mode_ipc'], axis=1)

    firm_size_groups = firm_incumbents.groupby(('size_bin'))
    firm_age_groups = firm_incumbents.groupby(('age_bin'))

    firm_all_sums = firm_incumbents.sum()

    firm_size_sums = firm_size_groups.sum()
    firm_size_means = firm_size_groups.mean()
    firm_size_medians = firm_size_groups.median()
    firm_size_fracs = firm_size_sums/firm_all_sums
    firm_means_by_size = firm_size_means.unstack(0)
    firm_sums_by_size = firm_size_sums.unstack(0)
    firm_fracs_by_size = firm_size_fracs.unstack(0)

    firm_age_sums = firm_age_groups.sum()
    firm_age_means = firm_age_groups.mean()
    firm_age_medians = firm_age_groups.median()
    firm_age_fracs = firm_age_sums/firm_all_sums
    firm_means_by_age = firm_age_means.unstack(0)
    firm_sums_by_age = firm_age_sums.unstack(0)
    firm_fracs_by_age = firm_age_fracs.unstack(0)

    # cumulative fractions sorted by initial stock, excludes entrants obvi
    firm_size_cumsum = firm_incumbents[['count','stock_start','file_pnum','source_pnum','dest_pnum']].dropna().sort_values(by='stock_start').cumsum().apply(lambda s: s.astype(np.float)/s.iloc[-1])
    firm_size_cumsum = firm_size_cumsum.set_index(np.linspace(0.0,1.0,len(firm_size_cumsum)))
    firm_age_cumsum = firm_incumbents[['age','count','stock_start','file_pnum','source_pnum','dest_pnum']].dropna().sort_values(by='age').drop('age',axis=1).cumsum().apply(lambda s: s.astype(np.float)/s.iloc[-1])
    firm_age_cumsum = firm_age_cumsum.set_index(np.linspace(0.0,1.0,len(firm_age_cumsum)))

if run_flags[3]:
    print('Transfer size/age stats')

    # merge in transfers
    trans_cols = ['size_bin','age_bin','size_rank','age_rank','stock','age']
    datf_idx_sub = datf_idx[['firm_num','year']+trans_cols]
    trans_merge = pd.merge(trans_info,grant_info[['patnum','class','ipc','high_tech','fileyear','grantyear']],how='left',left_on='patnum',right_on='patnum')
    trans_merge = pd.merge(trans_merge,datf_idx_sub,how='left',left_on=['dest_fn','execyear'],right_on=['firm_num','year'])
    trans_merge = trans_merge.rename(columns=dict([(s,s+'_dest') for s in ['firm_num']+trans_cols]))
    trans_merge = pd.merge(trans_merge,datf_idx_sub,how='left',left_on=['source_fn','execyear'],right_on=['firm_num','year'])
    trans_merge = trans_merge.rename(columns=dict([(s,s+'_source') for s in ['firm_num']+trans_cols]))

    # three groups - no_match(0),match_small(1),match_large(2)
    # unmatched firm number means the firm has no grants
    trans_merge['size_bin_source'] += 1
    trans_merge['size_bin_dest'] += 1
    trans_merge['age_bin_source'] += 1
    trans_merge['age_bin_dest'] += 1
    trans_merge.fillna({'size_bin_source':0,'size_bin_dest':0,'age_bin_source':0,'age_bin_dest':0},inplace=True)

    trans_merge['size_up'] = (trans_merge['stock_dest'] > trans_merge['stock_source']).astype(np.float)
    trans_merge['size_up'][trans_merge['stock_dest'].isnull()|trans_merge['stock_source'].isnull()] = np.nan
    trans_merge['age_up'] = (trans_merge['age_dest'] > trans_merge['age_source']).astype(np.float)
    trans_merge['age_up'][trans_merge['age_dest'].isnull()|trans_merge['age_source'].isnull()] = np.nan

    trans_merge['size_rank_diff'] = trans_merge['size_rank_dest'] - trans_merge['size_rank_source']
    trans_merge['age_rank_diff'] = trans_merge['age_rank_dest'] - trans_merge['age_rank_source']

    # select data years
    trans_stats = trans_merge[(trans_merge['execyear']>=base_year)&(trans_merge['execyear']<top_year)]

    # group by patent class
    trans_class_groups = trans_stats.groupby(pclass)
    trans_class_means = trans_class_groups.mean().add_prefix('trans_').add_suffix('_mean')

    # panel of all firms in year range
    # firm_info_panel = firm_info[(firm_info['year_max']>=base_year)&(firm_info['year_min']<top_year)]

    # panel of all citing firm pairs in year range
    # firm_cite_panel = firm_cite_year[(firm_cite_year['cite_year']>=base_year)&(firm_cite_year['cite_year']<top_year)]
    # firm_cite_merge = firm_cite_panel.merge(firm_info[['firm_num','mode_class']],how='left',left_on='citer_fnum',right_on='firm_num').drop('firm_num',axis=1).rename(columns={'mode_class':'citer_mode_class'})
    # firm_cite_merge = firm_cite_merge.merge(firm_info[['firm_num','mode_class']],how='left',left_on='citee_fnum',right_on='firm_num').drop('firm_num',axis=1).rename(columns={'mode_class':'citee_mode_class'})
    # firm_cite_within = firm_cite_merge[firm_cite_merge['citer_mode_class']==firm_cite_merge['citee_mode_class']].drop('citee_mode_class',axis=1).rename(columns={'citer_mode_class':'mode_class'})

    # citation rates
    # firm_panel_class_count = firm_info_panel.groupby('mode_class').size()
    # within_cite_class_count = firm_cite_within.groupby('mode_class').size()
    # cite_pair_class_frac = within_cite_class_count.astype(np.float)/(firm_panel_class_count**2)
    # cite_pair_class_agg = within_cite_class_count.sum().astype(np.float)/(firm_panel_class_count**2).sum()

if run_flags[4]:
    print('Toplevel industry info')

    grant_class_groups = grant_info.groupby(pclass)
    grant_class_born = grant_class_groups['fileyear'].quantile(0.01)
    grant_class_base = pd.DataFrame({'class_born':grant_class_born})
    grant_class_base['class_age'] = base_year - grant_class_base['class_born']

    # modal class group stats
    other_code = 'ipc' if pclass == 'class' else 'class'
    other_mode = 'mode_' + other_code
    class_groups = firm_totals.drop([other_mode],axis=1).groupby(mode_pclass)
    class_sums = class_groups.sum()
    class_means = class_groups.mean()
    class_medians = class_groups.median()
    class_stds = class_groups.std()
    class_skews = class_groups.skew()
    class_cvars = noinf(class_stds/class_means)
    class_sfracs = noinf(class_sums.apply(lambda df: df/class_sums['stock_start']))
    class_ffracs = noinf(class_sums.apply(lambda df: df/class_sums['file_pnum']))
    class_lmeans = class_groups.apply(lambda df: np.log1p(df.astype(np.float)).mean())
    class_lstds = class_groups.apply(lambda df: np.log1p(df.astype(np.float)).std())
    class_counts = class_groups.size()

    # aggregate into mecha-df
    firm_class_info = stack_frames([class_sums,class_means,class_medians,class_stds,class_skews,class_cvars,class_sfracs,class_ffracs,class_lmeans,class_lstds],suffixes=['_sum','_mean','_median','_std','_skew','_cvar','_sfrac','_ffrac','_lmean','_lstd'])
    firm_class_info['stock_skew'] = class_groups['stock_start'].apply(lambda s: np.log1p(s).std())
    firm_class_info['self_cited_frac'] = noinf(class_sums['n_self_cited']/class_sums['n_cited'])
    firm_class_info['cites_per_patent'] = noinf(class_sums['n_cited']/class_sums['file_pnum'])
    firm_class_info['agg_profit'] = noinf(class_sums['income']/class_sums['cost'])
    firm_class_info['agg_ros'] = noinf(class_sums['income']/class_sums['revenue'])
    firm_class_info['agg_cashi'] = noinf(class_sums['cash']/class_sums['revenue'])
    firm_class_info['agg_markup'] = noinf(class_sums['revenue']/class_sums['cogs'])
    firm_class_info['agg_rndi'] = noinf(class_sums['rnd']/class_sums['revenue'])
    firm_class_info['agg_invest'] = noinf(class_sums['capx']/class_sums['capital'])
    firm_class_info['agg_rndprod'] = noinf(class_sums['file_pnum']/class_sums['rnd'])
    firm_class_info['agg_acquire_int'] = noinf(class_sums['acquire']/class_sums['assets'])
    firm_class_info['agg_dest_ffrac'] = noinf(class_sums['dest_pnum']/class_sums['file_pnum'])
    firm_class_info['class_size'] = class_counts
    firm_class_info['herfindahl'] = noinf(class_groups['employ'].apply(lambda s: s.sort_values().dropna()[-1::-1][:np.ceil(0.1*s.count()).astype(np.int)].sum()/s.sum()))
    firm_class_info['herfindahl'][firm_class_info['class_size']<10] = np.nan
    firm_class_info['entrant_stock_frac'] = class_groups.apply(lambda df: df['stock_end'][df['entered']].sum()/df['stock_end'].sum())
    firm_class_info['entrant_employ_frac'] = class_groups.apply(lambda df: df['employ_end'][df['entered']].sum()/df['employ_end'].sum())

    # pure patent stats
    firm_grants = grant_info[(grant_info['fileyear']>=base_year)&(grant_info['fileyear']<top_year)]
    firm_grants['pos_trans'] = firm_grants['ntrans'] > 0
    firm_grants['pos_self_cited'] = firm_grants['n_self_cited'] > 0
    firm_grants['self_cited_frac'] = noinf(firm_grants['n_self_cited'].astype(np.float)/firm_grants['n_cited'].astype(np.float))
    firm_grants['lots_self_cite'] = firm_grants['self_cited_frac'] >= 0.5
    firm_grants['n_ext_cited'] = firm_grants['n_cited'] - firm_grants['n_self_cited']
    firm_grants['ext_cited_frac'] = noinf(firm_grants['n_ext_cited'].astype(np.float)/firm_grants['n_cited'].astype(np.float))
    firm_grants['expire_four'] = firm_grants['life_grant'] <= 4
    firm_grants['expire_eight'] = firm_grants['life_grant'] <= 8
    firm_grants['expire_twelve'] = firm_grants['life_grant'] <= 12
    firm_grants['grant_lag'] = firm_grants['grantyear'] - firm_grants['fileyear']
    firm_grants['trans_lag'] = pd.to_numeric(firm_grants['first_trans']) - firm_grants['fileyear']
    firm_grants['trans_3yr'] = firm_grants['trans_lag'] <= 3

    grant_class_groups = firm_grants.groupby(pclass)
    grant_class_size = grant_class_groups.size()
    grant_class_sums = grant_class_groups.sum()
    grant_class_means = grant_class_groups.mean()
    grant_class_medians = grant_class_groups.median()
    grant_class_ffracs = grant_class_sums.apply(lambda df: noinf(df.astype(np.float)/grant_class_groups.size()))
    grant_class_info = stack_frames([grant_class_base,grant_class_means,grant_class_medians,grant_class_ffracs],prefixes=['grant_','grant_','grant_','grant_','grant_'],suffixes=['','_mean','_median','_ffrac'])
    grant_class_info['grant_class_number'] = grant_class_info.index
    grant_class_info['grant_class_size'] = grant_class_size

    # merge both levels
    datf_class = firm_class_info.join(grant_class_info).join(trans_class_means)

if run_flags[5]:
    print('Aggregate industry stats')

    # generate target values
    targ_model = pd.Series().rename('value').rename_axis('name')
    targ_model['median_markup'] = firm_totals['markup'].median()
    targ_model['median_profit'] = firm_totals['profit'].median()
    targ_model['mean_markup'] = firm_totals['markup'].mean()-1.0
    targ_model['mean_profit'] = firm_totals['profit'].mean()
    targ_model['agg_markup'] = firm_totals['revenue'].sum()/firm_totals['cogs'].sum()-1.0
    targ_model['agg_profit'] = firm_totals['income'].sum()/firm_totals['cost'].sum()
    targ_model['entry_rate_5year'] = firm_totals['entered'].mean()
    targ_model['entrant_stock_frac'] = firm_totals['stock_end'][firm_totals['entered']].sum()/firm_totals['stock_end'].sum()
    targ_model['internal_cite_frac'] = (firm_totals['n_self_cited']>0.0*firm_totals['n_cited']).mean()
    targ_model['ind_ptrans_mean'] = datf_class['grant_pos_trans_mean'].mean()
    targ_model['ind_ptrans_std'] = datf_class['grant_pos_trans_mean'].std()
    targ_model['ptrans_3year'] = firm_grants['trans_3yr'].mean()/firm_grants['pos_trans'].mean()
    targ_model['trans_younger_prob'] = 1.0 - trans_merge['age_up'].mean()
    targ_model['trans_smaller_prob'] = 1.0 - trans_merge['size_up'].mean()
    targ_model['trans_young_frac'] = firm_fracs_by_age[('dest_pnum',False)]
    targ_model['trans_small_frac'] = firm_fracs_by_size[('dest_pnum',False)]
    targ_model['stock_young_frac'] = firm_totals['stock_start'][firm_totals['age_bin']==0].sum().astype(np.float)/firm_totals['stock_start'].sum()
    targ_model['stock_small_frac'] = firm_totals['stock_start'][firm_totals['size_bin']==0].sum().astype(np.float)/firm_totals['stock_start'].sum()
    targ_model['stock_lgrowth_mean'] = firm_totals['stock_lgrowth'].mean()
    targ_model['stock_lgrowth_young'] = firm_totals['stock_lgrowth'][firm_totals['age_bin']==0].mean()
    targ_model['stock_lgrowth_small'] = firm_totals['stock_lgrowth'][firm_totals['size_bin']==0].mean()
    targ_model['employ_lgrowth_mean'] = firm_totals['employ_lgrowth'].mean()
    targ_model['employ_lgrowth_young'] = firm_totals['employ_lgrowth'][firm_totals['age_bin']==0].mean()
    targ_model['employ_lgrowth_small'] = firm_totals['employ_lgrowth'][firm_totals['size_bin']==0].mean()
    targ_model['rnd_intensity_mean'] = firm_totals['rndi'].median()
    targ_model['rnd_intensity_young'] = firm_totals['rndi'][firm_totals['age_bin']==0].median()
    targ_model['rnd_intensity_small'] = firm_totals['rndi'][firm_totals['size_bin']==0].median()
    targ_model['file_stock_mean'] = firm_totals['file_stock_frac'].mean()
    targ_model['file_stock_young'] = firm_totals['file_stock_frac'][firm_totals['age_bin']==0].mean()
    targ_model['file_stock_small'] = firm_totals['file_stock_frac'][firm_totals['size_bin']==0].mean()
    targ_model.to_csv('data/targets.csv',header=True)

    # distributions for paper
    markup_bins = np.linspace(0.0,1.0,16)
    markup_data = firm_totals['markup'].dropna()
    markup_mass = np.histogram(markup_data,bins=markup_bins)[0].astype(np.float)/len(markup_data)
    markup_vals = 0.5*(markup_bins[:-1]+markup_bins[1:])
    pd.DataFrame({
        'bin_min': markup_bins[:-1],
        'bin_max': markup_bins[1:],
        'bin_mid': markup_vals,
        'mass': markup_mass
    }).to_csv('data/markup_dist.csv', index=False)

    translag_bins = np.linspace(0.0,20.0,20)
    translag_data = (trans_stats['execyear']-trans_stats['fileyear']).dropna()
    translag_mass = np.histogram(translag_data,bins=translag_bins)[0].astype(np.float)/len(translag_data)
    translag_vals = 0.5*(translag_bins[:-1]+translag_bins[1:])
    pd.DataFrame({
        'bin_min': translag_bins[:-1],
        'bin_max': translag_bins[1:],
        'bin_mid': translag_vals,
        'mass': translag_mass
    }).to_csv('data/translag_dist.csv', index=False)

