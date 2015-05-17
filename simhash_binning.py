# Created by Liang Sun in 2013
# Modified by Doug Hanley 2015

# name matching using locally sensitive hashing

from itertools import chain
from collections import defaultdict
from hashlib import md5
import operator as op
import re
import sqlite3
import numpy as np

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
  'ENTERTAINMENT': 'ENTER'
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

class UnionFind:
    """
    Union-find data structure.

    Each unionFind instance X maintains a family of disjoint sets of
    hashable objects, supporting the following two methods:

    - X[item] returns a name for the set containing the given item.
    Each set is named by an arbitrarily-chosen one of its members; as
    long as the set remains unchanged it will keep the same name. If
    the item is not yet part of a set in X, a new singleton set is
    created for it.

    - X.union(item1, item2, ...) merges the sets containing each item
    into a single larger set. If any item is not yet part of a set
    in X, it is added to X as one of the members of the merged set.
    
    Source: http://www.ics.uci.edu/~eppstein/PADS/UnionFind.py

    Union-find data structure. Based on Josiah Carlson's code,
    http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/215912
    with significant additional changes by D. Eppstein.
    """

    def __init__(self):
        """Create a new empty union-find structure."""
        self.weights = {}
        self.parents = {}

    def __getitem__(self, object):
        """Find and return the name of the set containing the object."""
        # check for previously unknown object
        if object not in self.parents:
            self.parents[object] = object
            self.weights[object] = 1
            return object

        # find path of objects leading to the root
        path = [object]
        root = self.parents[object]
        
        while root != path[-1]:
            path.append(root)
            root = self.parents[root]

        # compress the path and return
        for ancestor in path:
            self.parents[ancestor] = root
            
        return root

    def __iter__(self):
        """Iterate through all items ever found or unioned by this structure."""
        return iter(self.parents)

    def union(self, *objects):
        """Find the sets containing the objects and merge them all."""
        roots = [self[x] for x in objects]
        heaviest = max([(self.weights[r],r) for r in roots])[1]
        for r in roots:
            if r != heaviest:
                self.weights[heaviest] += self.weights[r]
                self.parents[r] = heaviest

    def sets(self):
        """Return a list of each disjoint set"""
        ret = defaultdict(list)
        for k, _ in self.parents.iteritems():
            ret[self[k]].append(k)
        return ret

class SimhashIndex:
    # dim is the simhash width, k is the tolerance
    def __init__(self, dim=64, k=2):
        """
        """
        self.k = k
        self.dim = dim
        self.unions = UnionFind()
        self.hashmaps = [defaultdict(list) for _ in range(k+1)]

        self.masks = [1 << i for i in range(dim)]

        self.offsets = [self.dim // (self.k + 1) * i for i in range(self.k + 1)]
        self.bin_masks = [(i == len(self.offsets) - 1 and 2 ** (self.dim - offset) - 1 or 2 ** (self.offsets[i+1]-offset) - 1) for (i,offset) in enumerate(self.offsets)]

    # add item to the cluster
    def add(self, item, weights=None, label=None):
        # Ensure label for this item
        if label is None:
            label = item

        # Add to unionfind structure
        self.unions[label]

        # get simhash signature
        simhash = self.sign(item,weights)

        # Unite labels with the same keys in the same band
        for idx, key in enumerate(self.get_keys(simhash)):
            self.hashmaps[idx][key].append(label)
            self.unions.union(label,self.hashmaps[idx][key][0])

    # get the clustering result
    def groups(self):
        return self.unions.sets()

    # get a set of matching labels for item
    def match(self, item):
        # Get signature
        simhash = self.sign(item)
        
        matches = set()
        
        for idx, key in enumerate(self.get_keys(simhash)):
            if key in self.hashmaps[idx]:
                matches.update(self.hashmaps[idx][key])
        
        return matches

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
            c = simhash >> offset & mask
            yield '%x:%x' % (c, i)


def firm_buckets(npat=None,reverse=False,nshingle=2,store=False,**kwargs):
    con = sqlite3.connect('store/patents.db')
    cur = con.cursor()

    c = SimhashIndex(**kwargs)

    cmd = 'select patnum,owner,country from patent where owner!=\'\''
    if reverse:
        cmd += ' order by rowid desc'
    if npat:
        cmd += ' limit {}'.format(npat)

    name_dict = {}
    i = 0
    for (patnum,owner,country) in cur.execute(cmd):
        toks = name_standardize(owner)
        name = ' '.join(toks)
        name_shings = list(shingle(name,nshingle))
        features = name_shings + toks + [country]
        weights = [1.0]*len(name_shings) + [3.0]*len(toks) + [3.0]
        c.add(features,weights=weights,label=patnum)

        name += ' (' + country + ')'
        name_dict[patnum] = name

        i += 1
        if i%100000 == 0: print i

    groups = c.groups()
    if store:
        cur.execute('drop table if exists cluster')
        cur.execute('create table cluster (patnum int, clusterid int)')

        for (cid,(_,group)) in enumerate(c.groups().items()):
            cur.executemany('insert into cluster values (?,?)',[(patnum,cid) for patnum in group])

        con.commit()
    else:
        (gnames,sgroups) = zip(*sorted(groups.items(),key=lambda (k,v): len(v),reverse=True))
        sgroups = map(lambda g: map(name_dict.get,g),sgroups)
        gnames = map(name_dict.get,gnames)
        glens = map(len,sgroups)
        guniq = map(lambda g: len(np.unique(g)),sgroups)
        return (sgroups,gnames,guniq,glens)
