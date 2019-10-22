import numpy as np
import pandas as pd
from itertools import chain
from parse_tools import astype

def merge_grants(output):
    print('Merging all grant data')

    dtypes = {'appnum': 'str', 'appdate': 'str', 'pubdate': 'str', 'first_trans': 'str'}
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

    int_cols = ['firm_num', 'last_maint']
    grant[int_cols] = grant[int_cols].astype('Int64')

    grant.drop('abstract', axis=1).to_csv(f'{output}/grant_info.csv')
    grant[['title', 'abstract']].to_csv(f'{output}/grant_text.csv')

def generate_firmyear(output):
    print('Generating all firm-years')

    # patent applications
    dtypes = {'appnum': 'str', 'appdate': 'str', 'pubdate': 'str', 'execdate': 'str', 'naics': 'Int64', 'sic': 'Int64'}
    apply = pd.read_csv(f'{output}/apply_apply.csv', usecols=['appnum', 'appdate'], dtype=dtypes)
    apply_firm = pd.read_csv(f'{output}/apply_firm.csv', dtype=dtypes).set_index('appnum')
    apply = apply.join(apply_firm, on='appnum', how='inner')
    apply['appyear'] = apply['appdate'].str.slice(0, 4).astype(np.int)

    apply_fy = apply.groupby(['firm_num', 'appyear']).size().rename('n_apply')
    apply_fy = apply_fy.rename_axis(index={'appyear': 'year'})

    # patent grants
    grant = pd.read_csv(f'{output}/grant_info.csv', usecols=['patnum', 'pubdate', 'n_cited', 'n_citing', 'n_self_cited'], dtype=dtypes)
    grant_firm = pd.read_csv(f'{output}/grant_firm.csv', index_col='patnum')
    grant = grant.join(grant_firm, on='patnum', how='inner')
    grant['pubyear'] = grant['pubdate'].str.slice(0, 4).astype(np.int)

    grant_groups = grant.groupby(['firm_num', 'pubyear'])
    grant_fy = grant_groups[['n_cited', 'n_citing', 'n_self_cited']].sum()
    grant_fy['n_grant'] = grant_groups.size()
    grant_fy = grant_fy.rename_axis(index={'pubyear': 'year'})

    # patent assignments
    assign = pd.read_csv(f'{output}/assign_use.csv', usecols=['assignid', 'execdate'], dtype=dtypes)
    assignor_firm = pd.read_csv(f'{output}/assignor_firm.csv', index_col='assignid')
    assignee_firm = pd.read_csv(f'{output}/assignee_firm.csv', index_col='assignid')
    assign = assign.join(assignor_firm.add_prefix('assignor_'), on='assignid', how='inner')
    assign = assign.join(assignee_firm.add_prefix('assignee_'), on='assignid', how='inner')
    assign['execyear'] = assign['execdate'].str.slice(0, 4).astype(np.int)

    assignor_fy = assign.groupby(['assignor_firm_num', 'execyear']).size().rename('n_source')
    assignor_fy = assignor_fy.rename_axis(index={'assignor_firm_num': 'firm_num', 'execyear': 'year'})

    assignee_fy = assign.groupby(['assignee_firm_num', 'execyear']).size().rename('n_dest')
    assignee_fy = assignee_fy.rename_axis(index={'assignee_firm_num': 'firm_num', 'execyear': 'year'})

    # compustat firms
    compu = pd.read_csv(f'{output}/compustat.csv', dtype=dtypes)
    compu_firm = pd.read_csv(f'{output}/compustat_firm.csv', index_col='compid')
    compu = compu.join(compu_firm, on='compid', how='inner')

    compu_fy = compu.groupby(['firm_num', 'year'])[['assets', 'capx', 'cash', 'cogs', 'deprec', 'income', 'employ', 'intan', 'debt', 'revenue', 'sales', 'rnd', 'fcost', 'mktval']].sum()
    ind_info = compu.groupby(['firm_num', 'year'])[['naics', 'sic']].first()
    compu_fy = compu_fy.join(ind_info)

    # comprehensive
    total = pd.concat([apply_fy, grant_fy, assignor_fy, assignee_fy, compu_fy], axis=1).reset_index()
    int_cols = ['n_apply', 'n_grant', 'n_cited', 'n_citing', 'n_self_cited', 'n_source', 'n_dest']
    total[int_cols] = total[int_cols].astype('Int64')

    total.to_csv(f'{output}/firmyear_info.csv', index=False)

def firm_statistics(output):
    print('Finding firm statistics')

    # firm history statistics
    dtypes = {'naics': 'str', 'sic': 'str'}
    firmyear = pd.read_csv(f'{output}/firmyear_info.csv', usecols=['firm_num', 'year', 'n_grant', 'naics', 'sic'], dtype=dtypes)
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
    grant = pd.read_csv(f'{output}/grant_info.csv', usecols=['firm_num', 'ipc'])
    grant = grant.dropna(subset=['firm_num'])
    grant['firm_num'] = grant['firm_num'].astype('Int64')
    grant['ipc4'] = grant['ipc'].str.slice(0, 4)

    # get modal ipc4 info
    count_ipc = grant.groupby(['firm_num', 'ipc4']).size().rename('count_ipc4')
    firm_ipc = count_ipc.reset_index(level='firm_num').groupby('firm_num')['count_ipc4']
    mode_ipc = firm_ipc.idxmax().rename('mode_ipc4')
    mode_ipc_count = firm_ipc.max().rename('mode_ipc4_count').astype('Int64')
    all_ipc_count = firm_ipc.sum().rename('all_ipc4_count').astype('Int64')
    firm_life = firm_life.join(mode_ipc)
    firm_life = firm_life.join(mode_ipc_count)
    firm_life = firm_life.join(all_ipc_count)
    firm_life['mode_ipc4_frac'] = firm_life['mode_ipc4_count']/firm_life['all_ipc4_count']
    # firm_life = firm_life.drop('mode_ipc4_count', axis=1)

    firm_life.to_csv(f'{output}/firm_life.csv')

def patent_stocks(output):
    print('Constructing patent stocks')

    # load firm data
    firmyear_info = pd.read_csv(f'{output}/firmyear_info.csv')
    firm_info = pd.read_csv(f'{output}/firm_life.csv', usecols=['firm_num', 'year_min', 'year_max', 'life_span'])

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
    datf_idx.to_csv(f'{output}/firmyear_index.csv', index=False)

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
