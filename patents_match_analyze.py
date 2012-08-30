import numpy as np
import sqlite3
import pandas
import sys
import itertools
import scipy.stats

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

    conn = sqlite3.connect('store/within.db')
    cur = conn.cursor()

    # load firm data
    datf = pandas.DataFrame(cur.execute('select firm_num,year,source_exec_pnum,dest_exec_pnum,file_pnum,grant_pnum,income,revenue,rnd,naics from firmyear_info where year>=1950').fetchall(),columns=['fnum','year','source','dest','file','grant','income','revenue','rnd','naics'])
    firm_info = pandas.DataFrame(data=cur.execute('select firm_num,year_min,year_max,life_span,has_comp,has_revn,has_rnd,has_pats from firm_life').fetchall(),columns=['fnum','zero_year','max_year','life_span','has_comp','has_revn','has_rnd','has_pats'])
    # firm_life starts a firm when they file for their first patent and ends 4 years after their last file

    # load transfer data
    #datf_trans = pandas.DataFrame(cur.execute('select assign_id,source_fn,dest_fn,recyear from assign_info').fetchall(),columns=['assign_id','source_fnum','dest_fnum','year'],dtype=np.int)

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
    fy_all = pandas.DataFrame(data={'fnum': all_fnums, 'year': all_years})
    datf_idx = fy_all.merge(datf,how='left',on=['fnum','year']).fillna(value={'file':0,'grant':0,'dest':0,'source':0},inplace=True)

    # derivative columns
    datf_idx = datf_idx.merge(firm_info.filter(['fnum','zero_year','max_year','life_span','has_comp','has_revn','has_rnd','has_pats']),how='left',on='fnum')
    age = datf_idx['year']-datf_idx['zero_year']
    max_age = age.max()
    trans = datf_idx['source']+datf_idx['dest']

    # next period values
    next_year = pandas.DataFrame(data={'fnum': datf_idx['fnum'], 'year': datf_idx['year']+1})
    next_info = next_year.merge(datf_idx.filter(['fnum','year','file','grant','source','dest']),how='left',on=['fnum','year'])

    # add to table
    datf_idx['age'] = age
    datf_idx['trans'] = trans
    datf_idx['file_next'] = next_info['file']
    datf_idx['grant_next'] = next_info['grant']
    datf_idx['source_next'] = next_info['source']
    datf_idx['dest_next'] = next_info['dest']

run1 = True
if stage <= 1 and run1:
    # construct patent stocks
    print 'Constructing patent stocks'

    depr = 0.9
    datf_idx['stock'] = 0.0
    sel_curr = (datf_idx['age']==0).values
    datf_idx['stock'][sel_curr] = datf_idx['file'][sel_curr].values
    for ai in range(1,max_age):
        sel_last = ((datf_idx['age']==ai-1)&(datf_idx['life_span']>ai)).values
        sel_curr = (datf_idx['age']==ai).values 
        datf_idx['stock'][sel_curr] = depr*datf_idx['stock'][sel_last].values + datf_idx['file'][sel_curr].values

run2 = False
if stage <= 2 and run2:
    # patent fractions
    print 'Firm statistics'

    datf_idx['file_frac'] = datf_idx['file']/datf_idx['stock']
    datf_idx['file_frac'][np.isinf(datf_idx['file_frac'])] = np.nan

    datf_idx['file_next_frac'] = datf_idx['file_next']/datf_idx['stock']
    datf_idx['file_next_frac'][np.isinf(datf_idx['file_next_frac'])] = np.nan

    datf_idx['grant_frac'] = datf_idx['grant']/datf_idx['stock']
    datf_idx['grant_frac'][np.isinf(datf_idx['grant_frac'])] = np.nan

    datf_idx['grant_next_frac'] = datf_idx['grant_next']/datf_idx['stock']
    datf_idx['grant_next_frac'][np.isinf(datf_idx['grant_next_frac'])] = np.nan

    datf_idx['dest_frac'] = datf_idx['dest']/datf_idx['stock']
    datf_idx['dest_frac'][np.isinf(datf_idx['dest_frac'])] = np.nan

    datf_idx['dest_next_frac'] = datf_idx['dest_next']/datf_idx['stock']
    datf_idx['dest_next_frac'][np.isinf(datf_idx['dest_next_frac'])] = np.nan

    datf_idx['source_frac'] = datf_idx['source']/datf_idx['stock']
    datf_idx['source_frac'][np.isinf(datf_idx['source_frac'])] = np.nan

    datf_idx['source_next_frac'] = datf_idx['source_next']/datf_idx['stock']
    datf_idx['source_next_frac'][np.isinf(datf_idx['source_next_frac'])] = np.nan

    # group by year
    all_year_groups = datf_idx.groupby('year')
    good_year_groups = datf_idx.groupby('year')

    # group by size-year
    median_stock_vec = good_year_groups['stock'].quantile(0.9)
    median_stock = median_stock_vec[datf_idx['year']]
    datf_idx['size_bin'] = datf_idx['stock'] > median_stock
    size_year_groups = datf_idx.groupby(('size_bin','year'))
    size_groups = datf_idx.groupby(('size_bin'))

    # group by age-year
    median_age_vec = good_year_groups['age'].quantile(0.9)
    median_age = median_age_vec[datf_idx['year']]
    datf_idx['age_bin'] = datf_idx['age'] > median_age
    age_year_groups = datf_idx.groupby(('age_bin','year'))
    age_groups = datf_idx.groupby(('age_bin'))

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

run3 = False
if stage <= 3 and run3:
    # merge into transfer data
    print 'Merge firm and transfer data'

    datf_sub = datf_idx.icol([0,1,8,11,17,18])
    datf_merge = datf_trans
    datf_merge = datf_merge.merge(datf_sub,how='inner',left_on=['source_fnum','year'],right_on=['fnum','year'])
    datf_merge = datf_merge.merge(datf_sub,how='inner',left_on=['dest_fnum','year'],right_on=['fnum','year'],suffixes=['_source','_dest'])
    datf_merge = datf_merge.drop(['fnum_source','fnum_dest'],axis=1)

    # group by year
    trans_year_groups = datf_merge.groupby('year')
    trans_year_sums = trans_year_groups['year'].count()

    # group by size_source-size_dest-year
    trans_size_groups = datf_merge.groupby(('size_bin_source','size_bin_dest','year'))
    trans_size_sums = trans_size_groups['year'].count()
    trans_size_fracs = trans_size_sums.astype(np.float)/trans_year_sums.reindex(trans_size_sums.index,level='year').astype(np.float)

    # group by age_source-age_dest-year
    trans_age_groups = datf_merge.groupby(('age_bin_source','age_bin_dest','year'))
    trans_age_sums = trans_age_groups['year'].count()
    trans_age_fracs = trans_age_sums.astype(np.float)/trans_year_sums.reindex(trans_age_sums.index,level='year').astype(np.float)

run4 = True
if stage <= 4 and run4:
    print 'Firm level data'

    # select on compustat firms and having patents
    sel_comp = datf_idx['has_comp'].astype(np.bool)
    datf_comp = datf_idx[sel_comp]

    sel_pats = datf_idx['has_pats'].astype(np.bool)
    datf_pats = datf_idx[sel_pats]

    # get naics codes
    def get_naics_func(n):
      return lambda i: int('{:<06d}'.format(i)[:n])

    datf_comp['naics'] = datf_comp['naics'].fillna(0).astype(np.int)
    datf_comp['naics2'] = np.array(map(get_naics_func(2),datf_comp['naics']))
    datf_comp['naics3'] = np.array(map(get_naics_func(3),datf_comp['naics']))

    naics2_set = list(np.unique(datf_comp['naics2']).values.astype(np.int))
    naics3_set = list(np.unique(datf_comp['naics3']).values.astype(np.int))

    # select on having R&D
    sel_rnd = datf_comp['has_rnd'].astype(np.bool)
    datf_rnd = datf_comp[sel_rnd]

#if __name__ == "__main__":
#  # execution state
#  if len(sys.argv) == 1:
#    stage = 0
#  else:
#    stage = int(sys.argv[1])
#
#  main(stage)

