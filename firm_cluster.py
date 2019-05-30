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

def generate_names(con):
    print('generating names')

    apply = pd.read_sql('select appnum,appname from apply', con).dropna()
    grant = pd.read_sql('select patnum,owner from grant', con).dropna()
    assignor = pd.read_sql('select assignid,assignor from assign_use', con).dropna()
    assignee = pd.read_sql('select assignid,assignee from assign_use', con).dropna()
    compustat = pd.read_sql('select compid,name from compustat', con).dropna()

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
    names.to_sql('name', con, index=False, if_exists='replace')

    apply = pd.merge(apply, names, how='left', on='name')
    grant = pd.merge(grant, names, how='left', on='name')
    assignor = pd.merge(assignor, names, how='left', on='name')
    assignee = pd.merge(assignee, names, how='left', on='name')
    compustat = pd.merge(compustat, names, how='left', on='name')

    apply[['appnum', 'id']].to_sql('apply_match', con, index=False, if_exists='replace')
    grant[['patnum', 'id']].to_sql('grant_match', con, index=False, if_exists='replace')
    assignor[['assignid', 'id']].to_sql('assignor_match', con, index=False, if_exists='replace')
    assignee[['assignid', 'id']].to_sql('assignee_match', con, index=False, if_exists='replace')
    compustat[['compid', 'id']].to_sql('compustat_match', con, index=False, if_exists='replace')

    con.commit()
    print(f'found {len(names)} names')

# k = 8, thresh = 4 works well
def filter_pairs(con, nshingle=2, k=8, thresh=4):
    print('filtering pairs')

    c = sh.Cluster(k=k, thresh=thresh)
    name_dict = {}

    names = pd.read_sql('select id,name from name', con)
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
    pairs.to_sql('pair', con, index=False, if_exists='replace')

    con.commit()
    print('Found %i pairs' % len(pairs))

# compute distances on owners in same cluster
def find_groups(con, thresh=0.85):
    print('finding matches')

    def dmetr(name1, name2):
        max_len = max(len(name1), len(name2))
        max_dist = int(ceil(max_len*(1.0-thresh)))
        ldist = levenshtein(name1, name2, max_dist=max_dist)
        return (1.0 - float(ldist)/max_len) if (ldist != -1 and max_len != 0) else 0.0

    close = []
    name_std = {}

    pairs = pd.read_sql('select id1,id2,name1,name2 from pair', con)
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
    match.to_sql('match', con, index=False, if_exists='replace')

    con.commit()
    print(f'found {len(comps)} groups')

# must be less than 1000000 components
def merge_firms(con, base=1_000_000):
    print('merging firms')

    names = pd.read_sql('select * from name', con)
    match = pd.read_sql('select * from match', con)
    firms = pd.merge(names, match, how='left', on='id')
    firms['firm_num'] = firms['firm_num'].fillna(firms['id']+base).astype(np.int)
    firms[['firm_num', 'id']].to_sql('firm', con, index=False, if_exists='replace')

    apply = pd.read_sql('select * from apply_match', con)
    grant = pd.read_sql('select * from grant_match', con)
    assignor = pd.read_sql('select * from assignor_match', con)
    assignee = pd.read_sql('select * from assignee_match', con)
    compustat = pd.read_sql('select * from compustat_match', con)

    apply = pd.merge(apply, firms, on='id')
    grant = pd.merge(grant, firms, on='id')
    assignor = pd.merge(assignor, firms, on='id')
    assignee = pd.merge(assignee, firms, on='id')
    compustat = pd.merge(compustat, firms, on='id')

    apply[['appnum', 'firm_num']].to_sql('apply_firm', con, index=False, if_exists='replace')
    grant[['patnum', 'firm_num']].to_sql('grant_firm', con, index=False, if_exists='replace')
    assignor[['assignid', 'firm_num']].to_sql('assignor_firm', con, index=False, if_exists='replace')
    assignee[['assignid', 'firm_num']].to_sql('assignee_firm', con, index=False, if_exists='replace')
    compustat[['compid', 'firm_num']].to_sql('compustat_firm', con, index=False, if_exists='replace')

    con.commit()

def get_groups(con):
    return pd.read_sql('select * from match join name on match.id=name.id order by firm_num', con)

if __name__ == "__main__":
    import argparse

    # parse input arguments
    parser = argparse.ArgumentParser(description='Create firm name clusters.')
    parser.add_argument('--db', type=str, default=None, help='database file to store to')
    args = parser.parse_args()

    # go through steps
    with sqlite3.connect(args.db) as con:
        unique_names(con)
        filter_pairs(con)
        find_groups(con)
        merge_firms(con)
