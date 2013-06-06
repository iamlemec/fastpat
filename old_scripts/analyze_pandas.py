import numpy as np
import sqlite3
import pandas
import sys

# execution state
if len(sys.argv) == 1:
  stage = 0
else:
  stage = int(sys.argv[1])

# load in data from db
#def main(stage=0):
run0 = True
if stage <= 0 and run0:
    # globals
    size_stat = 'revenue'
    age_stat = 'age'

    # load patent data
    con_trans = sqlite3.connect('store/transfers.db')
    cur_trans = con_trans.cursor()
    datf_trans = pandas.DataFrame(cur_trans.execute('select patnum,execyear,ifnull(assignor_gvkey,0),ifnull(assignee_gvkey,0) from transfer').fetchall(),columns=['patnum','year','assignor_gvkey','assignee_gvkey'],dtype=np.int)
    con_trans.close()

    # load compustat data
    con_comp = sqlite3.connect('store/compustat.db')
    cur_comp = con_comp.cursor()
    datf_comp = pandas.DataFrame(cur_comp.execute('select * from firmyear_final where revenue>0').fetchall(),columns=['gvkey','year','income','revenue','rnd','naics','source','dest','grant'])
    con_comp.close()

    # derivative columns
    naics3 = np.array([int('{:0<6}'.format(nv)[:3]) for nv in datf_comp['naics']])    
    naics4 = np.array([int('{:0<6}'.format(nv)[:4]) for nv in datf_comp['naics']])
    zero_year = datf_comp.groupby('gvkey')['year'].min()[datf_comp['gvkey']]
    age = datf_comp['year'].values-zero_year.values
    posinc = datf_comp['income'] > 0.0
    profit = datf_comp['income'].values/datf_comp['revenue'].values
    profit[np.isinf(profit)] = np.nan
    rnd_int = datf_comp['rnd']/datf_comp['revenue']
    trans = datf_comp['source']+datf_comp['dest']

    datf_next = pandas.DataFrame({'gvkey' : datf_comp['gvkey'], 'year' : datf_comp['year'], 'year_plus' : datf_comp['year']+1, 'revenue' : datf_comp['revenue']})
    datf_growth = pandas.merge(datf_next,datf_comp[['gvkey','year','revenue']],how='left',left_on=['gvkey','year_plus'],right_on=['gvkey','year'],suffixes=('','_next'))
    revenue_growth = (datf_growth['revenue_next']-datf_growth['revenue'])/datf_growth['revenue']

    # add to table now
    datf_comp.insert(len(datf_comp.columns),'profit',profit)
    datf_comp.insert(len(datf_comp.columns),'naics3',naics3)
    datf_comp.insert(len(datf_comp.columns),'naics4',naics4)
    datf_comp.insert(len(datf_comp.columns),'posinc',posinc)
    datf_comp.insert(len(datf_comp.columns),'age',age)
    datf_comp.insert(len(datf_comp.columns),'revenue_next',datf_growth['revenue_next'])
    datf_comp.insert(len(datf_comp.columns),'revenue_growth',revenue_growth)
    datf_comp.insert(len(datf_comp.columns),'rnd_int',rnd_int)
    datf_comp.insert(len(datf_comp.columns),'trans',trans)

    # for use later
    datf_comp.insert(len(datf_comp.columns),'profit_adj',np.nan)
    datf_comp.insert(len(datf_comp.columns),'posinc_adj',np.nan)
    datf_comp.insert(len(datf_comp.columns),'revn_growth_adj',np.nan)
    datf_comp.insert(len(datf_comp.columns),'rnd_int_adj',np.nan)
    datf_comp.insert(len(datf_comp.columns),'size_bin',np.nan)
    datf_comp.insert(len(datf_comp.columns),'age_bin',np.nan)

# selections and groupings
run1 = True
if stage <= 1 and run1:
    # group by year
    all_year_groups = datf_comp.groupby('year')

    # group by size and year
    median_revn_vec = all_year_groups[size_stat].quantile(0.8)
    median_revn = median_revn_vec[datf_comp['year']]
    datf_comp['size_bin'] = datf_comp[size_stat] > median_revn
    size_year_groups = datf_comp.groupby(('size_bin','year'))

    # group by age and year
    median_age = datf_comp[age_stat].quantile(0.5)
    datf_comp['age_bin'] = datf_comp[age_stat] > median_age
    age_year_groups = datf_comp.groupby(('age_bin','year'))

    # merge in transfers
    trans_cols = ['size_bin','age_bin','revenue','age']
    datf_comp_sub = datf_comp[['gvkey','year']+trans_cols]
    datf_trans_merge = pandas.merge(datf_trans,datf_comp_sub,how='left',left_on=['assignee_gvkey','year'],right_on=['gvkey','year'])
    datf_trans_merge = datf_trans_merge.rename(columns=dict([(s,s+'_assignee') for s in ['gvkey']+trans_cols]))
    datf_trans_merge = pandas.merge(datf_trans_merge,datf_comp_sub,how='left',left_on=['assignor_gvkey','year'],right_on=['gvkey','year'])
    datf_trans_merge = datf_trans_merge.rename(columns=dict([(s,s+'_assignor') for s in ['gvkey']+trans_cols]))

    # three groups - no_match(0),match_small(1),match_large(2)
    datf_trans_merge['size_bin_assignor'] += 1
    datf_trans_merge['size_bin_assignee'] += 1
    datf_trans_merge['size_bin_assignor'] = datf_trans_merge['size_bin_assignor'].fillna(0)
    datf_trans_merge['size_bin_assignee'] = datf_trans_merge['size_bin_assignee'].fillna(0)
    datf_trans_merge['age_bin_assignor'] += 1
    datf_trans_merge['age_bin_assignee'] += 1
    datf_trans_merge['age_bin_assignor'] = datf_trans_merge['age_bin_assignor'].fillna(0)
    datf_trans_merge['age_bin_assignee'] = datf_trans_merge['age_bin_assignee'].fillna(0)

    trans_size_up = (datf_trans_merge['revenue_assignee'] > datf_trans_merge['revenue_assignor']).astype(np.float)
    trans_size_up[datf_trans_merge['revenue_assignee'].isnull()|datf_trans_merge['revenue_assignor'].isnull()] = np.nan
    trans_age_up = (datf_trans_merge['age_assignee'] > datf_trans_merge['age_assignor']).astype(np.float)
    trans_age_up[datf_trans_merge['age_assignee'].isnull()|datf_trans_merge['age_assignor'].isnull()] = np.nan
    datf_trans_merge['trans_size_up'] = trans_size_up
    datf_trans_merge['trans_age_up'] = trans_age_up

    # group by year
    trans_year_groups = datf_trans_merge.groupby('year')
    trans_year_sums = trans_year_groups.size()
    trans_year_size_up = trans_year_groups['trans_size_up'].mean()
    trans_year_age_up = trans_year_groups['trans_age_up'].mean()

    # group by size transition type
    trans_size_year_groups = datf_trans_merge.groupby(('size_bin_assignor','size_bin_assignee','year'))
    trans_size_year_sums = trans_size_year_groups.size()
    trans_size_year_fracs = trans_size_year_sums.astype(np.float)/trans_year_sums.reindex(trans_size_year_sums.index,level='year')

    # group by age transition type
    trans_age_year_groups = datf_trans_merge.groupby(('age_bin_assignor','age_bin_assignee','year'))
    trans_age_year_sums = trans_age_year_groups.size()
    trans_age_year_fracs = trans_age_year_sums.astype(np.float)/trans_year_sums.reindex(trans_age_year_sums.index,level='year')

# patent count statistics
run2 = True
if stage <= 2 and run2:
    # patent counts by and size
    datf_size_year_sums = size_year_groups.sum()
    datf_year_sums = datf_size_year_sums.sum(level='year')
    datf_size_year_fracs = datf_size_year_sums/datf_year_sums.reindex(datf_size_year_sums.index,level='year')

    # firm level pats/revenue
    firm_grant_per_revenue = datf_comp['grant']/datf_comp[size_stat]
    firm_source_per_revenue = datf_comp['source']/datf_comp[size_stat]
    firm_dest_per_revenue = datf_comp['dest']/datf_comp[size_stat]

    # firm level pats/rnd
    firm_grant_per_rnd = datf_comp['grant']/datf_comp['rnd']
    firm_source_per_rnd = datf_comp['source']/datf_comp['rnd']
    firm_dest_per_rnd = datf_comp['dest']/datf_comp['rnd']

    # as fractions of grants
    dest_year_frac = datf_size_year_fracs['dest']/datf_year_sums['grant']
    source_year_frac = datf_size_year_fracs['source']/datf_year_sums['grant']
    trans_year_frac = datf_size_year_fracs['trans']/datf_year_sums['grant']

# profit statistics
run3 = False
if stage <= 3 and run3:
    # year-industry level aggregates
    naics_level = 'naics4'
    yrind_groups = datf_comp.groupby(['year',naics_level])
    yrind_stats = pandas.DataFrame({
      'profit_med'      : yrind_groups['profit'].median(),
      'profit_qlo'      : yrind_groups['profit'].quantile(0.25),
      'profit_qhi'      : yrind_groups['profit'].quantile(0.75),
      'posinc_frac'     : yrind_groups['posinc'].mean(),
      'revn_growth_med' : yrind_groups['revenue_growth'].median(),
      'rnd_int_med'     : yrind_groups['rnd_int'].median(),
      'trans_frac'      : yrind_groups['trans'].sum()/yrind_groups['grant'].sum(),
      'trans_pos'       : (yrind_groups['trans'].sum()>0),
      'profit_agg'      : yrind_groups['income'].sum()/yrind_groups['revenue'].sum(),
      'rnd_agg'         : yrind_groups['rnd'].sum()/yrind_groups['revenue'].sum()
    })
    yrind_stats.insert(0,'profit_iqr',yrind_stats['profit_qhi']-yrind_stats['profit_qlo'])

    # calculate firm level adjusted stats
    datf_yrind_stats = datf_comp[['year',naics_level]].join(yrind_stats,on=['year',naics_level])
    datf_comp['profit_adj'] = (datf_comp['profit'] - datf_yrind_stats['profit_med'])/datf_yrind_stats['profit_iqr']
    datf_comp['posinc_adj'] = datf_comp['posinc'] - datf_yrind_stats['posinc_frac']
    datf_comp['revn_growth_adj'] = datf_comp['revenue_growth'] - datf_yrind_stats['revn_growth_med']
    datf_comp['rnd_int_adj'] = datf_comp['rnd_int'] - datf_yrind_stats['rnd_int_med']

    # profit stats
    prof_med_adj = size_year_groups['profit_adj'].median()
    prof_qhi_adj = size_year_groups['profit_adj'].quantile(0.75)
    prof_qlo_adj = size_year_groups['profit_adj'].quantile(0.25)
    prof_mean_adj = size_year_groups['profit_adj'].mean()
    posinc_mean_adj = size_year_groups['posinc_adj'].mean()
    revn_growth_med_adj = size_year_groups['revn_growth_adj'].median()
    rnd_int_med_adj = size_year_groups['rnd_int_adj'].median()

    prof_med = size_year_groups['profit'].median()
    prof_qhi = size_year_groups['profit'].quantile(0.75)
    prof_qlo = size_year_groups['profit'].quantile(0.25)
    prof_mean = size_year_groups['profit'].mean()
    posinc_mean = size_year_groups['posinc'].mean()
    revn_growth_med = size_year_groups['revenue_growth'].median()
    rnd_int_med = size_year_groups['rnd_int'].median()

    # mean iqr over industries
    median_ind_iqr = yrind_stats['profit_iqr'].median(level='year')

    # within industry revn/rnd ratios
    yrind_size_groups = datf_comp.groupby(['year','naics4','size_bin'])
    yrind_revn_sums = yrind_size_groups['revenue'].sum().reorder_levels([2,0,1]).sort_index('size_bin')
    yrind_rnd_sums = yrind_size_groups['rnd'].sum().reorder_levels([2,0,1]).sort_index('size_bin')
    yrind_revn_ratio = yrind_revn_sums[False]/(yrind_revn_sums[False]+yrind_revn_sums[True])
    yrind_rnd_ratio = yrind_rnd_sums[False]/(yrind_rnd_sums[False]+yrind_rnd_sums[True])
    yrind_revn_ratio_mean = yrind_revn_ratio.mean(level='year')
    yrind_rnd_ratio_mean = yrind_rnd_ratio.mean(level='year')
    yrind_rndrevn_ratio = yrind_rnd_ratio_mean/yrind_revn_ratio_mean

#if __name__ == "__main__":
#  # execution state
#  if len(sys.argv) == 1:
#    stage = 0
#  else:
#    stage = int(sys.argv[1])
#
#  main(stage)

