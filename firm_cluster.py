# name matching using locality-sensitive hashing (simhash)
# these are mostly idempotent

from itertools import chain, repeat, product
from collections import defaultdict
from math import ceil

import sqlite3
import numpy as np
import pandas as pd
import networkx as nx
try:
    from distance.cdistance import levenshtein
except:
    from distance import levenshtein

from standardize import standardize_weak, standardize_strong
import simhash as sh

#
# data processing routines
#

def generate_names(output):
    print('generating names')

    apply = pd.read_csv(f'{output}/apply_apply.csv', usecols=['appnum', 'appname']).dropna()
    grant = pd.read_csv(f'{output}/grant_grant.csv', usecols=['patnum', 'owner']).dropna()
    assignor = pd.read_csv(f'{output}/assign_use.csv', usecols=['assignid', 'assignor']).dropna()
    assignee = pd.read_csv(f'{output}/assign_use.csv', usecols=['assignid', 'assignee']).dropna()
    compustat = pd.read_csv(f'{output}/compustat.csv', usecols=['compid', 'name']).dropna()

    apply = apply[apply['appname'].str.len()>0]
    grant = grant[grant['owner'].str.len()>0]
    assignor = assignor[assignor['assignor'].str.len()>0]
    assignee = assignee[assignee['assignee'].str.len()>0]
    compustat = compustat[compustat['name'].str.len()>0]

    apply['name'] = apply['appname'].apply(standardize_weak)
    grant['name'] = grant['owner'].apply(standardize_weak)
    assignor['name'] = assignor['assignor'].apply(standardize_weak)
    assignee['name'] = assignee['assignee'].apply(standardize_weak)
    compustat['name'] = compustat['name'].apply(standardize_weak)

    names = pd.concat([apply['name'], grant['name'], assignor['name'], assignee['name'], compustat['name']]).drop_duplicates().reset_index(drop=True)
    names = names.rename('name').rename_axis('id').reset_index()
    names.to_csv(f'{output}/name.csv', index=False)

    apply = pd.merge(apply, names, how='left', on='name')
    grant = pd.merge(grant, names, how='left', on='name')
    assignor = pd.merge(assignor, names, how='left', on='name')
    assignee = pd.merge(assignee, names, how='left', on='name')
    compustat = pd.merge(compustat, names, how='left', on='name')

    apply[['appnum', 'id']].to_csv(f'{output}/apply_match.csv', index=False)
    grant[['patnum', 'id']].to_csv(f'{output}/grant_match.csv', index=False)
    assignor[['assignid', 'id']].to_csv(f'{output}/assignor_match.csv', index=False)
    assignee[['assignid', 'id']].to_csv(f'{output}/assignee_match.csv', index=False)
    compustat[['compid', 'id']].to_csv(f'{output}/compustat_match.csv', index=False)

    print(f'found {len(names)} names')

# k = 8, thresh = 4 works well
def filter_pairs(output, nshingle=2, k=8, thresh=4):
    print('filtering pairs')

    c = sh.Cluster(k=k, thresh=thresh)
    name_dict = {}

    names = pd.read_csv(f'{output}/name.csv', usecols=['id', 'name'], na_filter=False)
    for i, id, name in names.itertuples():
        words = name.split()
        shings = list(sh.shingle(name, nshingle))

        features = shings + words
        weights = list(np.linspace(1.0, 0.0, len(shings))) + list(np.linspace(1.0, 0.0, len(words)))

        c.add(features, weights=weights, label=id)
        name_dict[id] = name

        if i > 0 and i % 100_000 == 0:
            print(f'{i}: {len(c.unions)}')

    pairs = pd.DataFrame([(i1, i2, name_dict[i1], name_dict[i2]) for i1, i2 in c.unions], columns=['id1', 'id2', 'name1', 'name2'])
    pairs.to_csv(f'{output}/pair.csv', index=False)

    print('Found %i pairs' % len(pairs))

# compute distances on owners in same cluster
def find_groups(output, thresh=0.85):
    print('finding matches')

    def dmetr(name1, name2):
        max_len = max(len(name1), len(name2))
        max_dist = int(ceil(max_len*(1.0-thresh)))
        ldist = levenshtein(name1, name2, max_dist=max_dist)
        return (1.0 - float(ldist)/max_len) if (ldist != -1 and max_len != 0) else 0.0

    close = []
    name_std = {}

    pairs = pd.read_csv(f'{output}/pair.csv', usecols=['id1', 'id2', 'name1', 'name2'], na_filter=False)
    for i, id1, id2, name1, name2 in pairs.itertuples():
        if id1 not in name_std:
            name_std[id1] = standardize_strong(name1)
        if id2 not in name_std:
            name_std[id2] = standardize_strong(name2)

        n1std = name_std[id1]
        n2std = name_std[id2]

        if dmetr(n1std, n2std) > thresh:
            close.append((id1, id2))

        if i > 0 and i % 100_000 == 0:
            print(f'{i}: {len(close)}')

    G = nx.Graph()
    G.add_edges_from(close)
    comps = sorted(nx.connected_components(G), key=len, reverse=True)

    match = pd.DataFrame(chain(*[zip(repeat(fid), ids) for fid, ids in enumerate(comps)]), columns=['firm_num', 'id'])
    match.to_csv(f'{output}/match.csv', index=False)

    print(f'found {len(comps)} groups')

# must be less than 1000000 components
def merge_firms(output, base=1_000_000):
    print('merging firms')

    names = pd.read_csv(f'{output}/name.csv', na_filter=False)
    match = pd.read_csv(f'{output}/match.csv', na_filter=False)
    firms = pd.merge(names, match, how='left', on='id')
    firms['firm_num'] = firms['firm_num'].fillna(firms['id']+base).astype(np.int)
    firms[['firm_num', 'id']].to_csv(f'{output}/firm.csv', index=False)

    apply = pd.read_csv(f'{output}/apply_match.csv')
    grant = pd.read_csv(f'{output}/grant_match.csv')
    assignor = pd.read_csv(f'{output}/assignor_match.csv')
    assignee = pd.read_csv(f'{output}/assignee_match.csv')
    compustat = pd.read_csv(f'{output}/compustat_match.csv')

    apply = pd.merge(apply, firms, on='id')
    grant = pd.merge(grant, firms, on='id')
    assignor = pd.merge(assignor, firms, on='id')
    assignee = pd.merge(assignee, firms, on='id')
    compustat = pd.merge(compustat, firms, on='id')

    apply[['appnum', 'firm_num']].to_csv(f'{output}/apply_firm.csv', index=False)
    grant[['patnum', 'firm_num']].to_csv(f'{output}/grant_firm.csv', index=False)
    assignor[['assignid', 'firm_num']].to_csv(f'{output}/assignor_firm.csv', index=False)
    assignee[['assignid', 'firm_num']].to_csv(f'{output}/assignee_firm.csv', index=False)
    compustat[['compid', 'firm_num']].to_csv(f'{output}/compustat_firm.csv', index=False)

if __name__ == "__main__":
    import argparse

    # parse input arguments
    parser = argparse.ArgumentParser(description='Create firm name clusters.')
    parser.add_argument('--output', type=str, default='tables', help='directory to operate on')
    args = parser.parse_args()

    # go through steps
    generate_names(output)
    filter_pairs(output)
    find_groups(output)
    merge_firms(output)
