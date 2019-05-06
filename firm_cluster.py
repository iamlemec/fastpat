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
# globals
#

db_fname = None
def set_db(fn):
    global db_fname
    db_fname = fn

# white magic
def autodb(f):
    def f1(*args,**kwargs):
        con = sqlite3.connect(db_fname)
        cur = con.cursor()
        try:
            ret = f(con=con,cur=cur,*args[2:],**kwargs)
        finally:
            con.close()
        return ret
    return f1

#
# data processing routines
#

@autodb
def generate_names(con, cur):
    print('generating owner names')

    # standardize compustat names
    cur.execute('drop table if exists compustat_std')
    cur.execute('create table compustat_std (gvkey int, year int, namestd text)')
    ret = cur.execute('select gvkey,year,name from compustat')
    cur.executemany('insert into compustat_std values (?,?,?)',[(gvkey,year,standardize_weak(owner)) for (gvkey,year,owner) in ret])

    # standardize patent names
    cur.execute('drop table if exists patent_std')
    cur.execute('create table patent_std (patnum int, namestd int)')
    ret = cur.execute('select patnum,owner from patent')
    cur.executemany('insert into patent_std values (?,?)',[(patnum,standardize_weak(owner)) for (patnum,owner) in ret])

    # standardize assignment names
    cur.execute('drop table if exists assign_std')
    cur.execute('create table assign_std (assignid int, assigneestd int, assignorstd)')
    ret = cur.execute('select assignid,assignor,assignee from assign_use')
    cur.executemany('insert into assign_std values (?,?,?)',[(assignid,standardize_weak(assignor),standardize_weak(assignee)) for (assignid,assignor,assignee) in ret])

    # store unique names
    cur.execute('drop table if exists owner')
    cur.execute('create table owner (ownerid integer primary key asc, name text)')
    cur.execute("""insert into owner(name) select namestd from compustat_std
                   union select namestd from patent_std
                   union select assigneestd from assign_std
                   union select assignorstd from assign_std""")

    # map back into compustat
    cur.execute('drop table if exists compustat_owner')
    cur.execute('create table compustat_owner (gvkey int, year int, ownerid int)')
    cur.execute('insert into compustat_owner select gvkey,year,ownerid from compustat_std join owner on compustat_std.namestd=owner.name')

    # map back into patent
    cur.execute('drop table if exists patent_owner')
    cur.execute('create table patent_owner (patnum int, ownerid int)')
    cur.execute('insert into patent_owner select patnum,ownerid from patent_std join owner on patent_std.namestd=owner.name')

    # map back into assignments
    cur.execute('drop table if exists assign_owner')
    cur.execute('create table assign_owner (assignid int, assigneeid int, assignorid int)')
    cur.execute("""insert into assign_owner select assignid,assignee_owner.ownerid,assignor_owner.ownerid from assign_std
                   join owner as assignee_owner on assign_std.assigneestd=assignee_owner.name
                   join owner as assignor_owner on assign_std.assignorstd=assignor_owner.name""")

    con.commit()

    print('generating names')

    apply = pd.read_sql('select appnum,appname from apply', con).dropna()
    grant = pd.read_sql('select patnum,owner from grant', con).dropna()
    assignor = pd.read_sql('select assignid,assignor', con).dropna()
    assignee = pd.read_sql('select assignid,assignee', con).dropna()
    compustat = pd.read_sql('select compid,namestd', con).dropna()

    apply = apply[apply['appname'].str.len()>0]
    grant = grant[grant['appname'].str.len()>0]

    apply['name'] = apply['appname'].apply(standardize_weak)
    grant['name'] = grant['appname'].apply(standardize_weak)

    names = pd.concat([apply['name'], grant['name']]).drop_duplicates().reset_index(drop=True)
    names = names.rename('name').rename_axis('id').reset_index()
    names.to_sql('name', con, index=False, if_exists='replace')

    apply = pd.merge(apply, names, how='left', on='name')
    grant = pd.merge(grant, names, how='left', on='name')

    apply[['appnum', 'id']].to_sql('apply_match', con, index=False, if_exists='replace')
    grant[['patnum', 'id']].to_sql('grant_match', con, index=False, if_exists='replace')

    con.commit()
    print(f'found {len(names)} names')


# k = 8, thresh = 4 works well
@autodb
def owner_cluster(con,cur,nitem=None,reverse=True,nshingle=2,store=True,**kwargs):
    print('generating hashes and pairs')

    c = sh.Cluster(**kwargs)

    cmd = 'select ownerid,name from owner'
    if reverse:
        cmd += ' order by rowid desc'
    if nitem:
        cmd += ' limit %i' % nitem

    name_dict = {}
    for (i,(ownerid,name)) in enumerate(cur.execute(cmd)):
        words = name.split()
        shings = list(sh.shingle(name,nshingle))

        features = shings + words
        weights = list(np.linspace(1.0,0.0,len(shings))) + list(np.linspace(1.0,0.0,len(words)))

        c.add(features,weights=weights,label=ownerid)
        name_dict[ownerid] = name

        if i%10000 == 0:
            print(i)

    ipairs = c.unions
    npairs = [(name_dict[i1],name_dict[i2]) for (i1,i2) in ipairs]
    print('Found %i pairs' % len(ipairs))

    if store:
        cur.execute('drop table if exists pair')
        cur.execute('create table pair (ownerid1 int, ownerid2 int, name1 text, name2 text)')
        cur.executemany('insert into pair values (?,?,?,?)',[(o1,o2,n1,n2) for ((o1,o2),(n1,n2)) in zip(ipairs,npairs)])
        con.commit()
    else:
        return (ipairs,npairs)

# compute distances on owners in same cluster
@autodb
def find_components(con,cur,thresh=0.85,store=True):
    print('finding firm components')

    cmd = 'select * from pair'

    def dmetr(name1,name2):
        maxlen = max(len(name1),len(name2))
        ldist = levenshtein(name1,name2,max_dist=int(ceil(maxlen*(1.0-thresh))))
        return (1.0 - float(ldist)/maxlen) if (ldist != -1 and maxlen != 0) else 0.0

    dists = []
    close = []
    name_dict = {}
    name_std = {}

    for (o1,o2,n1,n2) in cur.execute(cmd):
        if o1 not in name_dict:
            n1s = standardize_strong(n1)
            name_dict[o1] = n1
            name_std[o1] = n1s
        else:
            n1s = name_std[o1]
        if o2 not in name_dict:
            n2s = standardize_strong(n2)
            name_dict[o2] = n2
            name_std[o2] = n2s
        else:
            n2s = name_std[o2]

        d = dmetr(n1s,n2s)

        dists.append((o1,o2,d))
        if d > thresh:
            close.append((o1,o2))

    G = nx.Graph()
    G.add_edges_from(close)
    comps = sorted(nx.connected_components(G),key=len,reverse=True)

    if store:
        cur.execute('drop table if exists component')
        cur.execute('create table component (compid int, ownerid int)')
        cur.executemany('insert into component values (?,?)',chain(*[zip(repeat(cid),comp) for (cid,comp) in enumerate(comps)]))
        con.commit()
    else:
        comp_names = [[name_std[id] for id in ids] for ids in comps]
        return comp_names

# must be less than 1000000 components
@autodb
def merge_components(con,cur):
    print('merging firm components')

    print('matching owners to firms')
    cur.execute('drop table if exists owner_firm')
    cur.execute('create table owner_firm (ownerid int, firm_num int)')
    cur.execute('insert into owner_firm select ownerid,compid+1000000 from owner left join component using(ownerid)')
    cur.execute('update owner_firm set firm_num=ownerid where firm_num is null')

    print('merging into compustat')
    cur.execute('drop table if exists compustat_merge')
    cur.execute("""create table compustat_merge as select compustat.*,compustat_owner.ownerid,owner_firm.firm_num
                   from compustat left join compustat_owner using(gvkey,year)
                   left join owner_firm using(ownerid)""")

    print('merging into patents')
    cur.execute('drop table if exists patent_merge')
    cur.execute("""create table patent_merge as select patent.*,patent_owner.ownerid,owner_firm.firm_num
                   from patent left join patent_owner using(patnum)
                   left join owner_firm using(ownerid)""")

    print('merging into assignments')
    cur.execute('drop table if exists assign_merge')
    cur.execute("""create table assign_merge as select assign_use.*,assign_owner.assigneeid,assign_owner.assignorid,assignee_firm.firm_num as dest_fn,assignor_firm.firm_num as source_fn
                   from assign_use left join assign_owner on assign_use.assignid=assign_owner.assignid
                   left join owner_firm as assignee_firm on assign_owner.assigneeid=assignee_firm.ownerid
                   left join owner_firm as assignor_firm on assign_owner.assignorid=assignor_firm.ownerid""")

    print('generating simplified patents')
    cur.execute('drop table if exists patent_basic')
    cur.execute('create table patent_basic (patnum text, firm_num int, fileyear int, grantyear int, state text, country text, ipc text, ipcver text)')
    cur.execute("insert into patent_basic select patnum,firm_num,substr(filedate,1,4),substr(grantdate,1,4),state,country,ipc,ipcver from patent_merge")
    cur.execute('create unique index patent_basic_idx on patent_basic(patnum)')

    print('generating simplified assignments')
    cur.execute('drop table if exists assign_info')
    cur.execute('create table assign_info (assignid integer primary key, patnum int, source_fn int, dest_fn int, execyear int, recyear int, state text, country text)')
    cur.execute("insert into assign_info select assignid,patnum,source_fn,dest_fn,substr(execdate,1,4),substr(recdate,1,4),assignee_state,assignee_country from assign_merge where typeof(patnum) is 'integer'")

    print('generating assignments at transaction level')
    cur.execute('drop table if exists assign_bulk')
    cur.execute('create table assign_bulk (source_fn int, dest_fn int, execyear int, ntrans int)')
    cur.execute('insert into assign_bulk select source_fn,dest_fn,execyear,count(*) from assign_info group by source_fn,dest_fn,execyear')

    con.commit()

@autodb
def get_names(con,cur=None,olist=[]):
    ostr = ','.join([str(o) for o in olist])
    return pd.read_sql(f'select * from owner where ownerid in ({ostr})',con)

@autodb
def get_component(con,cur=None,compid=0):
    owners = pd.read_sql(f'select ownerid from component where compid={compid}',con)['ownerid']
    return get_names(olist=owners)

# distance metric
def affin(name1,name2):
    maxlen = max(len(name1),len(name2))
    ldist = levenshtein(name1,name2)
    return float(ldist)/maxlen if maxlen != 0 else 1.0

@autodb
def get_distances(con,cur=None,compid=0):
    import scipy.sparse as sp
    from sklearn import cluster

    owners = pd.read_sql(f'select * from component where compid={compid}',con)
    ostr = ','.join([str(o) for o in owners['ownerid']])
    omap = {o: i for i, o in enumerate(owners['ownerid'])}
    owners['oid'] = owners['ownerid'].map(omap)
    nown = len(owners)

    pairs = pd.read_sql(f'select * from pair where ownerid1 in ({ostr}) or ownerid2 in ({ostr})',con)
    pairs['dist'] = pairs.apply(lambda df: affin(df['name1'],df['name2']),axis=1)
    pairs['oid1'] = pairs['ownerid1'].map(omap)
    pairs['oid2'] = pairs['ownerid2'].map(omap)

    dist = pd.DataFrame([(o1,o2) for o1,o2 in product(owners['ownerid'],owners['ownerid'])],columns=['ownerid1','ownerid2'])
    dist = dist.join(pairs.set_index(['ownerid1','ownerid2'])['dist'],on=['ownerid1','ownerid2']).fillna(0.0)
    amat = dist['dist'].values.reshape([nown,nown])
    # amat = sp.coo_matrix((pairs['dist'],(pairs['oid1'],pairs['oid2'])))

    # fit = cluster.SpectralClustering(affinity='precomputed').fit(amat)
    fit = cluster.AffinityPropagation(affinity='precomputed').fit(amat)
    # fit = cluster.DBSCAN(metric='precomputed').fit(amat)
    nclust = np.max(fit.labels_)+1
    cids = [np.nonzero(fit.labels_==i)[0] for i in range(nclust)]
    cown = [owners[owners['oid'].isin(c)]['ownerid'] for c in cids]
    cname = [get_names(olist=c) for c in cown]

    return owners, pairs, cname

if __name__ == "__main__":
    import argparse

    # parse input arguments
    parser = argparse.ArgumentParser(description='Create firm name clusters.')
    parser.add_argument('--db', type=str, default=None, help='database file to store to')
    args = parser.parse_args()
    set_db(args.db)

    # go through steps
    generate_names()
    owner_cluster()
    find_components()
    merge_components()
