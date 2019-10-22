import numpy as np
import pandas as pd
from itertools import chain
from parse_tools import astype

def merge_grants(output):
    print('Merging all grant data')

    dtypes = {'appnum': 'str', 'appdate': 'str', 'pubdate': 'str', 'first_trans': 'str', 'last_maint': 'str'}
    grant = pd.read_csv(f'{output}/grant_grant.csv', index_col='patnum', dtype=dtypes)
    firm = pd.read_csv(f'{output}/grant_firm.csv', index_col='patnum')
    cite = pd.read_csv(f'{output}/cite_stats.csv', index_col='patnum')
    assign = pd.read_csv(f'{output}/assign_stats.csv', index_col='patnum', dtype=dtypes)
    maint = pd.read_csv(f'{output}/maint.csv', index_col='patnum')

    grant = grant.join(firm)
    grant = grant.join(cite)
    grant = grant.join(assign)
    grant = grant.join(maint)

    fill_cols = ['n_cited', 'n_citing', 'n_self_cited', 'n_trans', 'claims']
    grant[fill_cols] = grant[fill_cols].fillna(0).astype(np.int)

    int_cols = ['firm_num', 'ever_large']
    grant[int_cols] = grant[int_cols].astype('Int64')

    grant.to_csv(f'{output}/grant_info.csv')

def generate_firmyear(con):
    print('Generating all firm-years')

    # patent applications
    apply = pd.read_sql('select appnum,appdate from apply', con)
    apply_firm = pd.read_sql('select * from apply_firm', con, index_col='appnum')
    apply = apply.join(apply_firm, on='appnum', how='inner')
    apply['appyear'] = apply['appdate'].str.slice(0, 4).astype(np.int)

    apply_fy = apply.groupby(['firm_num', 'appyear']).size().rename('n_apply')
    apply_fy = apply_fy.rename_axis(index={'appyear': 'year'})

    # patent grants
    grant = pd.read_sql('select patnum,pubdate,n_cited,n_citing,n_self_cited from grant_info', con)
    grant_firm = pd.read_sql('select * from grant_firm', con, index_col='patnum')
    grant = grant.join(grant_firm, on='patnum', how='inner')
    grant['pubyear'] = grant['pubdate'].str.slice(0, 4).astype(np.int)

    grant_groups = grant.groupby(['firm_num', 'pubyear'])
    grant_fy = grant_groups[['n_cited', 'n_citing', 'n_self_cited']].sum()
    grant_fy['n_grant'] = grant_groups.size()
    grant_fy = grant_fy.rename_axis(index={'pubyear': 'year'})

    # patent assignments
    assign = pd.read_sql('select assignid,execdate from assign_use', con)
    assignor_firm = pd.read_sql('select * from assignor_firm', con, index_col='assignid')
    assignee_firm = pd.read_sql('select * from assignee_firm', con, index_col='assignid')
    assign = assign.join(assignor_firm.add_prefix('assignor_'), on='assignid', how='inner')
    assign = assign.join(assignee_firm.add_prefix('assignee_'), on='assignid', how='inner')
    assign = assign[assign['execdate']!='']
    assign['execyear'] = assign['execdate'].str.slice(0, 4).astype(np.int)

    assignor_fy = assign.groupby(['assignor_firm_num', 'execyear']).size().rename('n_source')
    assignor_fy = assignor_fy.rename_axis(index={'assignor_firm_num': 'firm_num', 'execyear': 'year'})

    assignee_fy = assign.groupby(['assignee_firm_num', 'execyear']).size().rename('n_dest')
    assignee_fy = assignee_fy.rename_axis(index={'assignee_firm_num': 'firm_num', 'execyear': 'year'})

    # compustat firms
    compu = pd.read_sql('select * from compustat', con)
    compu_firm = pd.read_sql('select * from compustat_firm', con, index_col='compid')
    compu = compu.join(compu_firm, on='compid', how='inner')

    compu_fy = compu.groupby(['firm_num', 'year'])[['assets', 'capx', 'cash', 'cogs', 'deprec', 'income', 'employ', 'intan', 'debt', 'revenue', 'sales', 'rnd', 'fcost', 'mktval']].sum()
    ind_info = compu.groupby(['firm_num', 'year'])[['naics', 'sic']].first()
    compu_fy = compu_fy.join(ind_info)

    # comprehensive
    total = pd.concat([apply_fy, grant_fy, assignor_fy, assignee_fy, compu_fy], axis=1).reset_index()
    int_cols = ['n_apply', 'n_grant', 'n_cited', 'n_citing', 'n_self_cited', 'n_source', 'n_dest']
    total[int_cols] = total[int_cols].astype('Int64')

    total.to_sql('firmyear_info', con, index=False, if_exists='replace')
    con.commit()

def firm_statistics(con):
    print('Finding firm statistics')

    # firm history statistics
    firmyear = pd.read_sql('select firm_num,year,n_grant,naics,sic from firmyear_info', con)
    firm_groups = firmyear.groupby('firm_num')
    firm_life = pd.DataFrame({
        'year_min': firm_groups['year'].min(),
        'year_max': firm_groups['year'].max(),
        'tot_pats': firm_groups['n_grant'].sum(),
        'naics': firm_groups['naics'].first(),
        'sic': firm_groups['sic'].first()
    })
    firm_life['tot_pats'] = firm_life['tot_pats'].fillna(0).astype(np.int)
    firm_life['life_span'] = firm_life['year_max'] - firm_life['year_min'] + 1

    # load in ipc info
    grant = pd.read_sql('select firm_num,ipc from grant_info', con)
    grant = grant.dropna(subset=['firm_num'])
    grant['firm_num'] = grant['firm_num'].astype('Int64')
    grant['ipc4'] = grant['ipc'].str.slice(0, 4)

    # get modal ipc4 info
    mode_ipc = grant.groupby('firm_num')['ipc4'].apply(pd.Series.mode).rename('ipc4_mode')
    mode_ipc = mode_ipc.rename_axis(['firm_num', 'mode_ipc4_count'], axis=0).reset_index(level=1)
    firm_life = firm_life.join(mode_ipc)
    firm_life['mode_ipc4_frac'] = firm_life['mode_ipc4_count']/firm_life['tot_pats']
    firm_life = firm_life.drop('mode_ipc4_count', axis=1)

    firm_life.to_sql('firm_life', con, if_exists='replace')
    con.commit()

def patent_stocks(con):
    print('Constructing patent stocks')

    # load firm data
    firmyear_info = pd.read_sql('select * from firmyear_info', con)
    firm_info = pd.read_sql('select firm_num,year_min,year_max,life_span from firm_life', con)

    # make (firm_num, year) index
    fnum_set = firm_info['firm_num']
    year_min = firm_info['year_min']
    year_max = firm_info['year_max']
    life_span = firm_info['life_span']
    all_fnums = np.array(list(chain(*[[fnum]*life for fnum, life in zip(fnum_set, life_span)])), dtype=np.int)
    all_years = np.array(list(chain(*[range(x, y+1) for x, y in zip(year_min, year_max)])), dtype=np.int)
    fy_all = pd.DataFrame({
        'firm_num': all_fnums,
        'year': all_years
    })

    datf_idx = fy_all.merge(firmyear_info, how='left', on=['firm_num', 'year'])
    int_cols = ['n_apply', 'n_grant', 'n_citing', 'n_cited', 'n_self_cited', 'n_source', 'n_dest']
    datf_idx = datf_idx.fillna({c: 0 for c in int_cols})
    datf_idx[int_cols] = datf_idx[int_cols].astype(np.int)

    # merge in overall firm info
    datf_idx = datf_idx.merge(firm_info[['firm_num', 'year_min']], how='left', on='firm_num')
    datf_idx['age'] = datf_idx['year'] - datf_idx['year_min']
    datf_idx = datf_idx.drop('year_min', axis=1)

    # aggregate stocks
    firm_groups = datf_idx.groupby('firm_num')
    datf_idx['stock'] = firm_groups['n_apply'].cumsum()

    # write new frame to disk
    datf_idx.to_sql('firmyear_index', con, index=False, if_exists='replace')
    con.commit()

if __name__ == "__main__":
    import argparse

    # parse input arguments
    parser = argparse.ArgumentParser(description='Merge firm patent data.')
    parser.add_argument('--output', type=str, default='tables', help='directory to operate on')
    args = parser.parse_args()

    # go through steps
    merge_grants(args.output)
    merge_firmyear(args.output)
    firm_statistics(args.output)
    patent_stocks(args.output)
