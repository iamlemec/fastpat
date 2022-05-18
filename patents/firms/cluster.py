# name matching using locality-sensitive hashing (simhash)
# these are mostly idempotent

import os
import numpy as np
import pandas as pd
import networkx as nx
from math import ceil
from itertools import chain, repeat
from collections import defaultdict
from editdistance import eval as levenshtein

from ..tools.standardize import standardize_weak, standardize_strong
from ..tools.tables import read_csv
from ..tools.simhash import shingle, Cluster

# firm name sources - tag: (table, id_col, name_col)
colmap = {
    'apply': ('apply_apply', 'appnum', 'appname'),
    'grant': ('grant_grant', 'patnum', 'owner'),
    'assignor': ('assign_use', 'assignid', 'assignor'),
    'assignee': ('assign_use', 'assignid', 'assignee'),
    'compustat': ('compustat_compustat', 'compid', 'name'),
}
sources0 = ['apply', 'grant', 'assignee', 'assignor']

def get_columns(sources):
    sources = sources if sources is not None else sources0
    return {k: v for k, v in colmap.items() if k in sources}

# find all unique names
def generate_names(output, sources=None):
    print('generating names')

    columns = get_columns(sources)

    sdict = {}
    for tag, (table, id_col, name_col) in columns.items():
        ipath = os.path.join(output, f'{table}.csv')
        src = read_csv(ipath, usecols=[id_col, name_col])
        sdict[tag] = src.dropna().rename({name_col: 'name'}, axis=1)

    names = pd.concat([src['name'] for src in sdict.values()], axis=0)
    names = names.apply(standardize_weak).drop_duplicates()
    names = names[names.str.len()>0].reset_index(drop=True)
    names = names.rename('name').rename_axis('id').reset_index()
    names.to_csv(f'{output}/name.csv', index=False)

    for tag, (table, id_col, name_col) in columns.items():
        opath = os.path.join(output, f'{tag}_match.csv')
        src = pd.merge(sdict[tag], names, how='left', on='name')
        src['id'] = src['id'].astype('Int64')
        src[[id_col, 'id']].to_csv(opath, index=False)

    print(f'found {len(names)} names')

# k = 8, thresh = 4 works well
def filter_pairs(output, nshingle=2, k=8, thresh=4):
    print('filtering pairs')

    c = Cluster(k=k, thresh=thresh)
    name_dict = {}

    npath = os.path.join(output, 'name.csv')
    names = read_csv(npath, usecols=['id', 'name'])

    for i, id, name in names.itertuples():
        words = name.split()
        shings = list(shingle(name, nshingle))

        features = shings + words
        weights = list(np.linspace(1.0, 0.0, len(shings))) + list(np.linspace(1.0, 0.0, len(words)))

        c.add(features, weights=weights, label=id)
        name_dict[id] = name

        if i > 0 and i % 100_000 == 0:
            print(f'{i}: {len(c.unions)}')

    ppath = os.path.join(output, 'pair.csv')
    pairs = pd.DataFrame([
        (i1, i2, name_dict[i1], name_dict[i2]) for i1, i2 in c.unions
    ], columns=['id1', 'id2', 'name1', 'name2'])
    pairs.to_csv(ppath, index=False)

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

    ppath = os.path.join(output, 'pair.csv')
    pairs = read_csv(ppath, usecols=['id1', 'id2', 'name1', 'name2'])

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

    mpath = os.path.join(output, 'match.csv')
    match = pd.DataFrame(chain(*[
        zip(repeat(fid), ids) for fid, ids in enumerate(comps)
    ]), columns=['firm_num', 'id'])
    match.to_csv(mpath, index=False)

    print(f'found {len(comps)} groups')

# must be less than 1000000 components
def merge_firms(output, sources=None, base=1000000):
    print('merging firms')

    columns = get_columns(sources)

    npath = os.path.join(output, 'name.csv')
    mpath = os.path.join(output, 'match.csv')
    fpath = os.path.join(output, 'firm.csv')

    names = read_csv(npath)
    match = read_csv(mpath)

    firms = pd.merge(names, match, how='left', on='id')
    firms['firm_num'] = firms['firm_num'].fillna(firms['id']+base).astype(np.int)
    firms[['firm_num', 'id']].to_csv(fpath, index=False)

    for tag, (table, id_col, name_col) in columns.items():
        ipath = os.path.join(output, f'{tag}_match.csv')
        opath = os.path.join(output, f'{tag}_firm.csv')
        src = read_csv(ipath)
        src = pd.merge(src, firms, on='id')
        src[[id_col, 'firm_num']].to_csv(opath, index=False)

# go through all steps
def cluster_firms(output, sources=None):
    generate_names(output, sources=sources)
    filter_pairs(output)
    find_groups(output)
    merge_firms(output, sources=sources)
