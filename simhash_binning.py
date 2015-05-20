# Simhash ported from Liang Sun (2013)

# name matching using locally sensitive hashing

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
# name standardization, separate from usual standardization
#

# acronyms
acronym1 = r"\b(\w) (\w) (\w)\b"
acronym1_re = re.compile(acronym1)
acronym2 = r"\b(\w) (\w)\b"
acronym2_re = re.compile(acronym2)
acronym3 = r"\b(\w)-(\w)-(\w)\b"
acronym3_re = re.compile(acronym3)
acronym4 = r"\b(\w)-(\w)\b"
acronym4_re = re.compile(acronym4)
acronym5 = r"\b(\w\w)&(\w)\b"
acronym5_re = re.compile(acronym5)
acronym6 = r"\b(\w)&(\w)\b"
acronym6_re = re.compile(acronym6)
acronym7 = r"\b(\w) & (\w)\b"
acronym7_re = re.compile(acronym7)

# punctuation
punct0 = r"'S|\(.*\)|\."
punct1 = r"[^\w\s]"
punct0_re = re.compile(punct0)
punct1_re = re.compile(punct1)

# generic terms
generics = ['THE','A','OF','AND','AN']
corps = ['INC','LLC','LTD','CORP','COMP','AG','NV','BV','GMBH','CO','BV','SA','AB','SE','KK']
dropout = generics + corps
gener_re = re.compile('|'.join([r"\b{}\b".format(el) for el in dropout]))

# substitutions - essentially lower their weighting
subsies = {
  'CORPORATION': 'CORP',
  'INCORPORATED': 'INC',
  'COMPANY': 'COMP',
  'LIMITED': 'LTD',
  'KABUSHIKI KAISHA': 'KK',
  'AKTIENGESELLSCHAFT': 'AG',
  'AKTIEBOLAG': 'AB',
  'TECHNOLOGIES': 'TECH',
  'TECHNOLOGY': 'TECH',
  'MANUFACTURING': 'MANUF',
  'MANUFACTURE': 'MANUF',
  'SEMICONDUCTORS': 'SEMI',
  'SEMICONDUCTOR': 'SEMI',
  'RESEARCH': 'RES',
  'COMMUNICATIONS': 'COMM',
  'COMMUNICATION': 'COMM',
  'SYSTEMS': 'SYS',
  'PHARMACEUTICALS': 'PHARMA',
  'PHARMACEUTICAL': 'PHARMA',
  'ELECTRONICS': 'ELEC',
  'INTERNATIONAL': 'INTL',
  'INDUSTRIES': 'INDS',
  'INDUSTRY': 'INDS',
  'CHEMICALS': 'CHEM',
  'CHEMICAL': 'CHEM',
  'LABORATORIES': 'LABS',
  'LABORATORY': 'LABS',
  'PRODUCTS': 'PROD',
  'ENGINEERING': 'ENG',
  'RESEARCH': 'RES',
  'DEVELOPMENT': 'DEV',
  'REPRESENTED': 'REPR',
  'SECRETARY': 'SECR',
  'PRODUCTS': 'PROD',
  'INDUSTRIAL': 'IND',
  'ASSOCIATES': 'ASSOC',
  'INSTRUMENTS': 'INSTR',
  'NATIONAL': 'NATL',
  'STANDARD': 'STD',
  'ORGANIZATION': 'ORG',
  'EQUIPMENT': 'EQUIP',
  'GESELLSCHAFT': 'GS',
  'INSTITUTE': 'INST',
  'MASCHINENFABRIK': 'MF',
  'AKTIEBOLAGET': 'AB',
  'SEISAKUSHO': 'SSS',
  'COMPAGNIE': 'COMP',
  'NATIONALE': 'NATL',
  'FOUNDATION': 'FOUND',
  'CONTINENTAL': 'CONTL',
  'INTERCONTINENTAL': 'INTER',
  'INDUSTRIE': 'IND',
  'INDUSTRIELLE': 'IND',
  'ELECTRICAL': 'ELEC',
  'ELECTRIC': 'ELEC',
  'UNIVERSITY': 'UNIV',
  'MICROSYSTEMS': 'MICRO',
  'MICROELECTRONICS': 'MICRO',
  'TELECOMMUNICATIONS': 'TELE',
  'HOLDINGS': 'HLDG',
  'MASSACHUSSETTES': 'MASS',
  'MINNESOTA': 'MINN',
  'MANAGEMENT': 'MGMT',
  'DEPARTMENT': 'DEP',
  'ADMINISTRATOR': 'ADMIN',
  'KOMMANDITGESELLSCHAFT': 'KG',
  'INNOVATIONS': 'INNOV',
  'INNOVATION': 'INNOV',
  'ENTERTAINMENT': 'ENTER',
  'ENTERPRISES': 'ENTER',
  'ENTERPRISE': 'ENTER'
}
subsies_re = re.compile(r"\b(" + "|".join(subsies.keys()) + r")\b")

# standardize a firm name
def name_standardize(name):
    name_strip = name

    name_strip = acronym1_re.sub(r"\1\2\3",name_strip)
    name_strip = acronym2_re.sub(r"\1\2",name_strip)
    name_strip = acronym3_re.sub(r"\1\2\3",name_strip)
    name_strip = acronym4_re.sub(r"\1\2",name_strip)
    name_strip = acronym5_re.sub(r"\1\2",name_strip)
    name_strip = acronym6_re.sub(r"\1\2",name_strip)
    name_strip = acronym7_re.sub(r"\1\2",name_strip)

    name_strip = punct0_re.sub('',name_strip)
    name_strip = punct1_re.sub(' ',name_strip)

    name_strip = subsies_re.sub(lambda x: subsies[x.group()],name_strip)
    name_strip = gener_re.sub('',name_strip)

    return name_strip.split()

city_parts = ['DO','SI','SHI']
city_re = re.compile('|'.join([r"\b{}\b".format(el) for el in city_parts]))

# standardize a city name
def city_standardize(city):
    city_strip = city.split(',')[0]

    city_strip = punct1_re.sub(' ',city_strip)
    city_strip = city_re.sub('',city_strip)

    return city_strip.split()

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
        for (h,w) in zip(hashs,weights):
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

def generate_names():
    con = sqlite3.connect('store/patents.db')
    cur = con.cursor()

    cur.execute('drop table if exists patent_std')
    cur.execute('create table patent_std (patnum int, namestd int)')

    for (patnum,owner,country) in cur.execute('select patnum,owner,country from patent where owner!=\'\'').fetchall():
        toks = name_standardize(owner)
        namestd = ' '.join(toks) + ' (' + country + ')'
        cur.execute('insert into patent_std values (?,?)',(patnum,namestd))

    cur.execute('drop table if exists owner')
    cur.execute('create table owner (ownerid integer primary key asc, name text)')
    cur.execute('insert into owner(name) select distinct namestd from patent_std')

    cur.execute('drop table if exists patown')
    cur.execute('create table patown (patnum int, ownerid int)')
    cur.execute('insert into patown select patnum,ownerid from patent_std join owner on patent_std.namestd=owner.name')

    con.commit()

# k = 8, thresh = 4 works well
def owner_cluster(nitem=None,reverse=True,nshingle=2,store=False,**kwargs):
    con = sqlite3.connect('store/patents.db')
    cur = con.cursor()

    c = Simhash(**kwargs)

    cmd = 'select ownerid,name from owner'
    if reverse:
        cmd += ' order by rowid desc'
    if nitem:
        cmd += ' limit %' % nitem

    name_dict = {}
    for (i,(ownerid,name)) in enumerate(cur.execute(cmd)):
        words = name.split()
        shings = list(shingle(name,nshingle))

        features = shings + words
        weights = list(np.linspace(1.0,0.0,len(shings))) + list(np.linspace(1.0,0.0,len(words)-1)) + [1.0]

        c.add(features,weights=weights,label=ownerid)
        name_dict[ownerid] = name

        if i%100000 == 0: print i

    ipairs = c.unions
    npairs = map(lambda p: map(name_dict.get,p),ipairs)
    print 'Found %i pairs' % len(ipairs)

    if store:
        cur.execute('drop table if exists pair')
        cur.execute('create table pair (ownerid1 int, ownerid2 int, name1 text, name2 text)')
        cur.executemany('insert into pair values (?,?,?,?)',imap(lambda ((o1,o2),(n1,n2)): (o1,o2,n1,n2),izip(ipairs,npairs)))
        con.commit()
        con.close()
    else:
        return (ipairs,npairs)

# compute distances on owners in same cluster
def compute_distances(nitem=None,store=False):
    con = sqlite3.connect('store/patents.db')
    cur = con.cursor()

    cur.execute('drop table if exists distance')
    cur.execute('create table distance (ownerid1 int, ownerid2 int, dist float)')

    cmd = 'select * from pair'
    if nitem:
        cmd += ' limit %i' % nitem
    pairs = cur.execute(cmd).fetchall()

    dmetr = lambda name1,name2: 1.0-float(distance.levenshtein(name1,name2))/max(len(name1),len(name2))
    dists = map(lambda (o1,o2,n1,n2): (o1,o2,dmetr(n1,n2)),cur.execute(cmd))
    dists += map(lambda (o1,o2,d): (o2,o1,d),dists) # symmetric matrix

    if store:
        cur.executemany('insert into distance values (?,?,?)',dists)
        con.commit()
        con.close()
    else:
        con.close()
        return dists

# find components using distance metrics
def compute_components(thresh=0.7):
    con = sqlite3.connect('store/patents.db')
    cur = con.cursor()

    dists = cur.execute('select * from distance')
    close = map(op.itemgetter(0,1),filter(lambda (o1,o2,d): d > thresh,dists))
    G = nx.Graph()
    G.add_edges_from(close)
    comps = sorted(list(nx.connected_components(G)),key=len,reverse=True)

    con.close()

    return comps

def get_names(olist):
    con = sqlite3.connect('store/patents.db')
    cur = con.cursor()

    names = cur.execute('select * from owner where ownerid in (%s)' % ','.join(map(str,olist))).fetchall()

    con.close()

    return names
