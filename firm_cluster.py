# name matching using locality-sensitive hashing (simhash)
# these are mostly idempotent

from itertools import chain, repeat
from collections import defaultdict
from math import ceil

import numpy as np
import pandas as pd
import networkx as nx
from editdistance import eval as levenshtein

from tools.standardize import standardize_weak, standardize_strong
from tools.tables import read_csv
from tools.simhash import shingle, Cluster

# firm name sources - (tag, table, id_col, name_col)
sources = [
    ('apply', 'apply_apply', 'appnum', 'appname'),
    ('grant', 'grant_grant', 'patnum', 'owner'),
    ('assignor', 'assign_use', 'assignid', 'assignor'),
    ('assignee', 'assign_use', 'assignid', 'assignee'),
    # ('compustat', 'compustat', 'compid', 'name'),
]

# find all unique names
def generate_names(output):
    print('generating names')

    sdict = {}
    for tag, table, id_col, name_col in sources:
        src = read_csv(f'{output}/{table}.csv', usecols=[id_col, name_col]).dropna()
        src['name'] = src[name_col].apply(standardize_weak)
        sdict[tag] = src

    names = pd.concat([src['name'] for src in sdict.values()], axis=0).drop_duplicates()
    names = names[names.str.len()>0].reset_index(drop=True)
    names = names.rename('name').rename_axis('id').reset_index()
    names.to_csv(f'{output}/name.csv', index=False)

    for tag, table, id_col, name_col in sources:
        src = pd.merge(sdict[tag], names, how='left', on='name')
        src[[id_col, 'id']].to_csv(f'{output}/{tag}_match.csv', index=False)

    print(f'found {len(names)} names')

# k = 8, thresh = 4 works well
def filter_pairs(output, nshingle=2, k=8, thresh=4):
    print('filtering pairs')

    c = Cluster(k=k, thresh=thresh)
    name_dict = {}

    names = read_csv(f'{output}/name.csv', usecols=['id', 'name'])
    for i, id, name in names.itertuples():
        words = name.split()
        shings = list(shingle(name, nshingle))

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
        ldist = levenshtein(name1, name2)
        return (1.0 - float(ldist)/max_len) if (ldist != -1 and max_len != 0) else 0.0

    close = []
    name_std = {}

    pairs = read_csv(f'{output}/pair.csv', usecols=['id1', 'id2', 'name1', 'name2'])
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
def merge_firms(output, base=1000000):
    print('merging firms')

    names = read_csv(f'{output}/name.csv')
    match = read_csv(f'{output}/match.csv')
    firms = pd.merge(names, match, how='left', on='id')
    firms['firm_num'] = firms['firm_num'].fillna(firms['id']+base).astype(np.int)
    firms[['firm_num', 'id']].to_csv(f'{output}/firm.csv', index=False)

    for tag, table, id_col, name_col in sources:
        src = read_csv(f'{output}/{tag}_match.csv')
        src = pd.merge(src, firms, on='id')
        src[[id_col, 'firm_num']].to_csv(f'{output}/{tag}_firm.csv', index=False)

if __name__ == "__main__":
    import argparse

    # parse input arguments
    parser = argparse.ArgumentParser(description='Create firm name clusters.')
    parser.add_argument('--output', type=str, default='tables', help='directory to operate on')
    args = parser.parse_args()

    # go through steps
    generate_names(args.output)
    filter_pairs(args.output)
    find_groups(args.output)
    merge_firms(args.output)
