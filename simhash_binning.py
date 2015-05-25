# name matching using locally sensitive hashing (simhash)
# Simhash ported from Liang Sun (2013)

from itertools import chain, izip, imap, ifilter, repeat
from collections import defaultdict
import operator as op

import re
import sqlite3
from math import ceil
from scipy.stats import itemfreq

import numpy as np
from hashlib import md5
from distance.cdistance import levenshtein
import networkx as nx

#
# globals
#

db_fname = 'store/patents.db'

#
# weak name standardization
#

# regular expression substitutions
paren = r"'S|\(.*\)|\."
punct = r"[^\w\s]"
space = r"[ ]{2,}"

paren_re = re.compile(paren)
punct_re = re.compile(punct)
space_re = re.compile(space)

# standardize firm name
def name_standardize(name):
    name_strip = name
    name_strip = paren_re.sub(' ',name_strip)
    name_strip = punct_re.sub(' ',name_strip)
    name_strip = space_re.sub(' ',name_strip)
    name_strip = name_strip.strip()
    return name_strip

#
# locally sensitive hashing code
#

def hashfunc(x):
    return int(md5(x).hexdigest(),16)

# k-shingles: pairs of adjacent k-length substrings (in order)
def shingle(s, k=2):
    """Generate k-length shingles of string s."""
    k = min(len(s), k)
    for i in range(len(s) - k + 1):
        yield s[i:i+k]

class Simhash:
    # dim is the simhash width, k is the tolerance
    def __init__(self, dim=64, k=3, thresh=1):
        self.dim = dim
        self.k = k
        self.thresh = thresh

        self.unions = []

        self.hashmaps = [defaultdict(list) for _ in range(k+1)] # defaultdict(list)
        self.masks = [1 << i for i in range(dim)]

        self.offsets = [self.dim // self.k * i for i in range(self.k)]
        self.bin_masks = [(i == len(self.offsets) - 1 and 2 ** (self.dim - offset) - 1 or 2 ** (self.offsets[i+1]-offset) - 1) for (i,offset) in enumerate(self.offsets)]

    # add item to the cluster
    def add(self, item, weights=None, label=None):
        # Ensure label for this item
        if label is None:
            label = item

        # get simhash signature
        simhash = self.sign(item,weights)
        keyvec = self.get_keys(simhash)

        # Unite labels with the same keys in the same band
        matches = defaultdict(int)
        for idx, key in enumerate(keyvec):
            others = self.hashmaps[idx][key]
            for l in others:
                matches[l] += 1
            others.append(label)
        for out, val in matches.iteritems():
           if val > self.thresh:
               self.unions.append((label,out))

    # compute actual simhash
    def sign(self, features, weights):
        if weights is None:
            weights = [1.0]*len(features)
        hashs = map(hashfunc,features)
        v = [0.0]*self.dim
        for (h,w) in izip(hashs,weights):
            for i in xrange(self.dim):
                v[i] += w if h & self.masks[i] else -w
        ans = 0
        for i in xrange(self.dim):
            if v[i] >= 0:
                ans |= self.masks[i]
        return ans

    # bin simhash into chunks
    def get_keys(self, simhash):
        return [simhash >> offset & mask for (offset,mask) in zip(self.offsets,self.bin_masks)]

#
# data processing routines
#

# white magic
def autodb(fname):
    def wrap(f):
        fvars = f.func_code.co_varnames
        has_con = 'con' in fvars
        has_cur = 'cur' in fvars
        if has_con or has_cur:
            def f1(*args,**kwargs):
                con = sqlite3.connect(fname)
                if has_cur:
                    cur = con.cursor()
                    if has_con:
                        ret = f(con=con,cur=cur,*args,**kwargs)
                    else:
                        ret = f(cur=cur,*args,**kwargs)
                else:
                    ret = f(con=con,*args,**kwargs)
                con.close()
                return ret
            return f1
        else:
            return f
    return wrap

@autodb(db_fname)
def generate_names(con,cur):
    # standardize compustat names
    cur.execute('drop table if exists compustat_std')
    cur.execute('create table compustat_std (gvkey int, year int, namestd text)')
    ret = cur.execute('select gvkey,year,name from compustat')
    cur.executemany('insert into compustat_std values (?,?,?)',map(lambda (gvkey,year,owner): (gvkey,year,name_standardize(owner)),ret))

    # standardize patent names
    cur.execute('drop table if exists patent_std')
    cur.execute('create table patent_std (patnum int, namestd int)')
    ret = cur.execute('select patnum,owner from patent_use')
    cur.executemany('insert into patent_std values (?,?)',map(lambda (patnum,owner): (patnum,name_standardize(owner)),ret))

    # standardize assignment names
    cur.execute('drop table if exists assignment_std')
    cur.execute('create table assignment_std (assignid int, assigneestd int, assignorstd)')
    ret = cur.execute('select assignid,assignor,assignee from assignment_use')
    cur.executemany('insert into assignment_std values (?,?,?)',map(lambda (assignid,assignor,assignee): (assignid,name_standardize(assignor),name_standardize(assignee)),ret))

    # store unique names
    cur.execute('drop table if exists owner')
    cur.execute('create table owner (ownerid integer primary key asc, name text)')
    cur.execute("""insert into owner(name) select namestd from compustat_std
                   union select namestd from patent_std
                   union select assigneestd from assignment_std
                   union select assignorstd from assignment_std""")

    # map back into compustat
    cur.execute('drop table if exists compustat_owner')
    cur.execute('create table compustat_owner (gvkey int, year int, ownerid int)')
    cur.execute('insert into compustat_owner select gvkey,year,ownerid from compustat_std join owner on compustat_std.namestd=owner.name')

    # map back into patent
    cur.execute('drop table if exists patent_owner')
    cur.execute('create table patent_owner (patnum int, ownerid int)')
    cur.execute('insert into patent_owner select patnum,ownerid from patent_std join owner on patent_std.namestd=owner.name')

    # map back into assignments
    cur.execute('drop table if exists assignment_owner')
    cur.execute('create table assignment_owner (assignid int, assigneeid int, assignorid int)')
    cur.execute("""insert into assignment_owner select assignid,assignee_owner.ownerid,assignor_owner.ownerid from assignment_std
                   join owner as assignee_owner on assignment_std.assigneestd=assignee_owner.name
                   join owner as assignor_owner on assignment_std.assignorstd=assignor_owner.name""")

    con.commit()

# k = 8, thresh = 4 works well
@autodb(db_fname)
def owner_cluster(con,cur,nitem=None,reverse=True,nshingle=2,store=False,**kwargs):
    c = Simhash(**kwargs)

    cmd = 'select ownerid,name from owner'
    if reverse:
        cmd += ' order by rowid desc'
    if nitem:
        cmd += ' limit %i' % nitem

    name_dict = {}
    for (i,(ownerid,name)) in enumerate(cur.execute(cmd)):
        words = name.split()
        shings = list(shingle(name,nshingle))

        features = shings + words
        weights = list(np.linspace(1.0,0.0,len(shings))) + list(np.linspace(1.0,0.0,len(words)))

        c.add(features,weights=weights,label=ownerid)
        name_dict[ownerid] = name

        if i%10000 == 0:
            print i

    ipairs = c.unions
    npairs = map(lambda p: map(name_dict.get,p),ipairs)
    print 'Found %i pairs' % len(ipairs)

    if store:
        cur.execute('drop table if exists pair')
        cur.execute('create table pair (ownerid1 int, ownerid2 int, name1 text, name2 text)')
        cur.executemany('insert into pair values (?,?,?,?)',imap(lambda ((o1,o2),(n1,n2)): (o1,o2,n1,n2),izip(ipairs,npairs)))
        con.commit()
    else:
        return ipairs

# compute distances on owners in same cluster
@autodb(db_fname)
def find_components(con,cur,thresh=0.85,store=False):
    cmd = 'select * from pair'

    def dmetr(name1,name2):
        maxlen = max(len(name1),len(name2))
        ldist = levenshtein(name1,name2,max_dist=int(ceil(maxlen*(1.0-thresh))))
        return 1.0 - float(ldist)/maxlen if ldist != -1 else 0.0
    dists = map(lambda (o1,o2,n1,n2): (o1,o2,dmetr(n1,n2)),cur.execute(cmd))
    close = map(op.itemgetter(0,1),filter(lambda (o1,o2,d): d > thresh,dists))

    G = nx.Graph()
    G.add_edges_from(close)
    comps = sorted(nx.connected_components(G),key=len,reverse=True)

    if store:
        cur.execute('drop table if exists component')
        cur.execute('create table component (compid int, ownerid int)')
        cur.executemany('insert into component values (?,?)',chain(*[izip(repeat(cid),comp) for (cid,comp) in enumerate(comps)]))
        con.commit()
    else:
        return comps

# must be less than 1000000 components
@autodb(db_fname)
def merge_components(con,cur):
    # match owners to firms
    cur.execute('drop table if exists owner_firm')
    cur.execute('create table owner_firm (ownerid int, firm_num int)')
    cur.execute('insert into owner_firm select ownerid,compid+1000000 from owner left join component using(ownerid)')
    cur.execute('update owner_firm set firm_num=ownerid where firm_num is null')

    cur.execute('drop table if exists compustat_merge')
    cur.execute("""create table compustat_merge as select compustat.*,compustat_owner.ownerid,owner_firm.firm_num
                   from compustat left join compustat_owner using(gvkey,year)
                   left join owner_firm using(ownerid)""")

    cur.execute('drop table if exists patent_merge')
    cur.execute("""create table patent_merge as select patent_use.*,patent_owner.ownerid,owner_firm.firm_num
                   from patent_use left join patent_owner using(patnum)
                   left join owner_firm using(ownerid)""")

    cur.execute('drop table if exists assignment_merge')
    cur.execute("""create table assignment_merge as select assignment_use.*,assignment_owner.assigneeid,assignment_owner.assignorid,assignee_firm.firm_num as dest_fn,assignor_firm.firm_num as source_fn
                   from assignment_use left join assignment_owner on assignment_use.assignid=assignment_owner.assignid
                   left join owner_firm as assignee_firm on assignment_owner.assigneeid=assignee_firm.ownerid
                   left join owner_firm as assignor_firm on assignment_owner.assignorid=assignor_firm.ownerid""")

    # aggregate to yearly
    cur.execute('drop table if exists patent_basic')
    cur.execute('create table patent_basic (patnum int primary key, firm_num int, fileyear int, grantyear int, classone int, classtwo int)')
    cur.execute('insert into patent_basic select patnum,firm_num,strftime(\'%Y\',filedate),strftime(\'%Y\',grantdate),classone,classtwo from patent_merge')

    cur.execute('drop table if exists assignment_info')
    cur.execute('create table assignment_info (assignid int primary key, patnum int, source_fn int, dest_fn int, execyear int, recyear int, grantyear int, fileyear int, classone int, classtwo int)')
    cur.execute('insert into assignment_info select assignid,patnum,source_fn,dest_fn,substr(execdate,1,4),substr(recdate,1,4),strftime(\'%Y\',grantdate),strftime(\'%Y\',filedate),classone,classtwo from assignment_merge')

    cur.execute('drop table if exists assignment_bulk')
    cur.execute('create table assignment_bulk (source_fn int, dest_fn int, execyear int, ntrans int)')
    cur.execute('insert into assignment_bulk select source_fn,dest_fn,execyear,count(*) from assignment_info group by source_fn,dest_fn,execyear')

    con.commit()

@autodb(db_fname)
def get_names(con,cur,olist=[]):
    return cur.execute('select * from owner where ownerid in (%s)' % ','.join(map(str,olist))).fetchall()

# add in a few corp reductions, esp kabushiki kaisha
