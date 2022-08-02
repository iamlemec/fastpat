import numpy as np
import pandas as pd
from itertools import chain

from ..tools.tables import read_csv

def merge_grants(output):
    print('Merging all grant data')

    grant = read_csv(f'{output}/grant_grant.csv').set_index('patnum')
    firm = read_csv(f'{output}/grant_firm.csv').set_index('patnum')
    cite = read_csv(f'{output}/cite_stats.csv').set_index('patnum')
    assign = read_csv(f'{output}/assign_stats.csv').set_index('patnum')
    maint = read_csv(f'{output}/maint_maint.csv').set_index('patnum')

    grant = grant.join(firm)
    grant = grant.join(cite)
    grant = grant.join(assign)
    grant = grant.join(maint)

    fill_cols = ['n_cited', 'n_citing', 'n_self_cited', 'n_self_citing', 'n_trans', 'claims']
    grant[fill_cols] = grant[fill_cols].fillna(0).astype(np.int)

    int_cols = ['firm_num', 'last_maint']
    grant[int_cols] = grant[int_cols].astype('Int64')

    grant.drop('abstract', axis=1).to_csv(f'{output}/grant_info.csv')
    grant[['title', 'abstract']].to_csv(f'{output}/grant_text.csv')

def generate_firmyear(output, compustat=False):
    print('Generating all firm-years')

    total = []

    # patent applications
    apply = read_csv(f'{output}/apply_apply.csv', usecols=['appnum', 'appdate'])
    apply_firm = read_csv(f'{output}/apply_firm.csv').set_index('appnum')
    apply = apply.join(apply_firm, on='appnum', how='inner')
    apply['appyear'] = apply['appdate'].str.slice(0, 4).astype(np.int)

    apply_fy = apply.groupby(['firm_num', 'appyear']).size().rename('n_apply')
    apply_fy = apply_fy.rename_axis(index={'appyear': 'year'})
    total.append(apply_fy)

    # patent grants
    grant = read_csv(f'{output}/grant_info.csv', usecols=['patnum', 'pubdate', 'n_cited', 'n_citing', 'n_self_cited'])
    grant_firm = read_csv(f'{output}/grant_firm.csv').set_index('patnum')
    grant = grant.dropna(subset=['pubdate'], axis=0)
    grant['pubyear'] = grant['pubdate'].str.slice(0, 4).astype(np.int)
    grant = grant.join(grant_firm, on='patnum', how='inner')

    grant_groups = grant.groupby(['firm_num', 'pubyear'])
    grant_fy = grant_groups[['n_cited', 'n_citing', 'n_self_cited']].sum()
    grant_fy['n_grant'] = grant_groups.size()
    grant_fy = grant_fy.rename_axis(index={'pubyear': 'year'})
    total.append(grant_fy)

    # patent assignments
    assign = read_csv(f'{output}/assign_use.csv', usecols=['assignid', 'execdate'])
    assignor_firm = read_csv(f'{output}/assignor_firm.csv').set_index('assignid')
    assignee_firm = read_csv(f'{output}/assignee_firm.csv').set_index('assignid')
    assign = assign.join(assignor_firm.add_prefix('assignor_'), on='assignid', how='inner')
    assign = assign.join(assignee_firm.add_prefix('assignee_'), on='assignid', how='inner')
    assign['execyear'] = assign['execdate'].str.slice(0, 4).astype(np.int)

    assignor_fy = assign.groupby(['assignor_firm_num', 'execyear']).size().rename('n_source')
    assignor_fy = assignor_fy.rename_axis(index={'assignor_firm_num': 'firm_num', 'execyear': 'year'})
    total.append(assignor_fy)

    assignee_fy = assign.groupby(['assignee_firm_num', 'execyear']).size().rename('n_dest')
    assignee_fy = assignee_fy.rename_axis(index={'assignee_firm_num': 'firm_num', 'execyear': 'year'})
    total.append(assignee_fy)

    # compustat firms
    if compustat:
        compu = read_csv(f'{output}/compustat_compustat.csv')
        compu_firm = read_csv(f'{output}/compustat_firm.csv').set_index('compid')
        compu = compu.join(compu_firm, on='compid', how='inner')

        compu_grp = compu.groupby(['firm_num', 'year'])
        compu_fy = compu_grp[[
            'assets', 'capx', 'cash', 'cogs', 'deprec', 'income', 'employ',
            'intan', 'debt', 'revenue', 'sales', 'rnd', 'fcost', 'mktval'
        ]].sum()
        ind_info = compu_grp[['naics', 'sic']].first()
        compu_fy = compu_fy.join(ind_info)
        total.append(compu_fy)

    # comprehensive
    total = pd.concat(total, axis=1).reset_index()
    int_cols = ['n_apply', 'n_grant', 'n_cited', 'n_citing', 'n_self_cited', 'n_source', 'n_dest']
    total[int_cols] = total[int_cols].astype('Int64')

    total.to_csv(f'{output}/firmyear_info.csv', index=False, float_format='%.3f')

def firm_statistics(output):
    print('Finding firm statistics')

    # firm history statistics
    firmyear = read_csv(f'{output}/firmyear_info.csv', usecols=['firm_num', 'year', 'n_grant'])
    firm_groups = firmyear.groupby('firm_num')
    firm_life = pd.DataFrame({
        'year_min': firm_groups['year'].min(),
        'year_max': firm_groups['year'].max(),
        'tot_pats': firm_groups['n_grant'].sum()
    })
    firm_life['tot_pats'] = firm_life['tot_pats'].fillna(0).astype(np.int)
    firm_life['life_span'] = firm_life['year_max'] - firm_life['year_min'] + 1

    # load in ipc info
    grant = read_csv(f'{output}/grant_info.csv', usecols=['firm_num', 'ipc'])
    grant = grant.dropna(subset=['firm_num'])
    grant['firm_num'] = grant['firm_num'].astype('Int64')
    grant['ipc4'] = grant['ipc'].str.slice(0, 4)

    # get modal ipc4 info
    count_ipc = grant.groupby(['firm_num', 'ipc4']).size().rename('count_ipc4')
    firm_ipc = count_ipc.reset_index(level='firm_num').groupby('firm_num')['count_ipc4']
    mode_ipc = firm_ipc.idxmax().rename('mode_ipc4')
    mode_ipc_count = firm_ipc.max().rename('mode_ipc4_count').astype('Int64')
    firm_life = firm_life.join(mode_ipc)
    firm_life = firm_life.join(mode_ipc_count)
    firm_life['mode_ipc4_frac'] = firm_life['mode_ipc4_count']/firm_life['tot_pats']
    firm_life = firm_life.drop('mode_ipc4_count', axis=1)

    firm_life.to_csv(f'{output}/firm_life.csv', float_format='%.3f')

def patent_stocks(output):
    print('Constructing patent stocks')

    # load firm data
    firmyear_info = read_csv(f'{output}/firmyear_info.csv')
    firm_info = read_csv(f'{output}/firm_life.csv', usecols=['firm_num', 'year_min', 'year_max', 'life_span'])

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
    datf_idx.to_csv(f'{output}/firmyear_index.csv', index=False, float_format='%.3f')

def merge_firms(output, compustat=False):
    merge_grants(output)
    generate_firmyear(output, compustat=compustat)
    firm_statistics(output)
    patent_stocks(output)
