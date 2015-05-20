# name matching using locally sensitive hashing (simhash)
# Simhash ported from Liang Sun (2013)

from itertools import chain, izip, imap
from collections import defaultdict
import operator as op

import re
import sqlite3

import numpy as np
from hashlib import md5
import distance
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
        for (out,val) in matches.iteritems():
            if val >= self.thresh:
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
        for (i,(offset,mask)) in enumerate(zip(self.offsets,self.bin_masks)):
            yield simhash >> offset & mask

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
    cur.execute('drop table if exists patent_std')
    cur.execute('create table patent_std (patnum int, namestd int)')

    ret = cur.execute('select patnum,owner from patent where owner!=\'\'')
    cur.executemany('insert into patent_std values (?,?)',map(lambda (patnum,owner): (patnum,name_standardize(owner)),ret))

    cur.execute('drop table if exists owner')
    cur.execute('create table owner (ownerid integer primary key asc, name text)')
    cur.execute('insert into owner(name) select distinct namestd from patent_std')

    cur.execute('drop table if exists patown')
    cur.execute('create table patown (patnum int, ownerid int)')
    cur.execute('insert into patown select patnum,ownerid from patent_std join owner on patent_std.namestd=owner.name')

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
    npairs = imap(lambda p: map(name_dict.get,p),ipairs)
    print 'Found %i pairs' % len(ipairs)

    if store:
        cur.execute('drop table if exists pair')
        cur.execute('create table pair (ownerid1 int, ownerid2 int, name1 text, name2 text)')
        cur.executemany('insert into pair values (?,?,?,?)',imap(lambda ((o1,o2),(n1,n2)): (o1,o2,n1,n2),izip(ipairs,npairs)))
        con.commit()
    else:
        return (ipairs,npairs)

# compute distances on owners in same cluster
@autodb(db_fname)
def compute_distances(con,cur,nitem=None,store=False):
    cur.execute('drop table if exists distance')
    cur.execute('create table distance (ownerid1 int, ownerid2 int, dist float)')

    cmd = 'select * from pair'
    if nitem:
        cmd += ' limit %i' % nitem
    pairs = cur.execute(cmd).fetchall()

    dmetr = lambda name1,name2: 1.0-float(distance.levenshtein(name1,name2))/max(len(name1),len(name2))
    dists = map(lambda (o1,o2,n1,n2): (o1,o2,dmetr(n1,n2)),cur.execute(cmd))

    if store:
        cur.executemany('insert into distance values (?,?,?)',dists)
        con.commit()
    else:
        return list(dists)

# find components using distance metrics
@autodb(db_fname)
def compute_components(con,cur,thresh=0.85):
    dists = cur.execute('select * from distance')
    close = imap(op.itemgetter(0,1),filter(lambda (o1,o2,d): d > thresh,dists))

    G = nx.Graph()
    G.add_edges_from(close)
    comps = sorted(list(nx.connected_components(G)),key=len,reverse=True)

    return comps

@autodb(db_fname)
def get_names(con,cur,olist=[]):
    return cur.execute('select * from owner where ownerid in (%s)' % ','.join(map(str,olist))).fetchall()

# add in a few corp reductions, esp kabushiki kaisha
