from collections import defaultdict

# k-shingles: pairs of adjacent k-length substrings (in order)
def shingle(s, k):
    """Generate k-length shingles of string s."""
    k = min(len(s), k)
    for i in range(len(s) - k + 1):
        yield s[i:i+k]

# map str'able x into min_i(hash(salt+j+x[i]+salt)) for j=1,...,dim
# this result is called a signature (sig) of length dim
class MinHashSignature:
    """Hash signatures for sets/tuples using minhash."""

    def __init__(self, dim):
        """
        Define the dimension of the hash pool
        (number of hash functions).
        """
        self.dim = dim
        self.hashes = self.hash_functions()

    def hash_functions(self):
        """Return dim different hash functions."""
        def hash_factory(n):
            return lambda x: hash("salt" + str(n) + str(x) + "salt")
        
        return [ hash_factory(_) for _ in range(self.dim) ]

    def sign(self, item):
        """Return the minhash signatures for the `item`."""
        sig = [ float("inf") ] * self.dim
        
        for hash_ix, hash_fn in enumerate(self.hashes):
            # minhashing; requires item is iterable:
            sig[hash_ix] = min(hash_fn(i) for i in item)
        
        return sig

# this partitions a signature into bands of length bandwidth
# and hashes each of these bands, returning this as an iterator
# of length n_bands
class LSH:
    """
    Locality sensitive hashing.
    
    Uses a banding approach to hash
    similar signatures to the same buckets.
    """
    
    def __init__(self, size, threshold):
        """
        LSH approximating a given similarity `threshold`
        with a given hash signature `size`.
        """
        self.size = size
        self.threshold = threshold
        self.bandwidth = self.get_bandwidth(size, threshold)

    @staticmethod
    def get_bandwidth(n, t):
        """
        Approximate the bandwidth (number of rows in each band)
        needed to get threshold.

        Threshold t = (1/b) ** (1/r)
        where
        b = # of bands
        r = # of rows per band
        n = b * r = size of signature
        """
        best = n # 1
        minerr = float("inf")
        
        for r in range(1, n + 1):
            try:
                b = 1. / (t ** r)
            except: # Divide by zero, your signature is huge
                return best
            
            err = abs(n - b * r)
            
            if err < minerr:
                best = r
                minerr = err
                
        return best

    def hash(self, sig):
        """Generate hash values for this signature."""
        for band in zip(*(iter(sig),) * self.bandwidth):
            yield hash("salt" + str(band) + "tlas")

    @property
    def exact_threshold(self):
        """The exact threshold defined by the chosen bandwith."""
        r = self.bandwidth
        b = self.size / r
        return (1. / b) ** (1. / r)

    def get_n_bands(self):
        """The number of bands."""
        return int(self.size / self.bandwidth)

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
        return ret.values()

# hashmaps - list of n_bands dictionaries, each of which maps from 
# LSH values to lists of items
#
# match - a match occurs when the LSH hash value matches in any of 
# bands
class Cluster:
    """
    Cluster items with a Jaccard similarity above
    some `threshold` with a high probability.

    Based on Rajaraman, "Mining of Massive Datasets":
    
    1. Generate items hash signatures
    2. Use LSH to map similar signatures to same buckets
    3. Use UnionFind to merge buckets containing same values
    """
    
    def __init__(self, threshold=0.5, size=10):
        """
        The `size` parameter controls the number of hash
        functions ("signature size") to create.
        """
        self.size = size
        self.unions = UnionFind()
        self.signer = MinHashSignature(size)
        self.hasher = LSH(size, threshold)
        self.hashmaps = [
            defaultdict(list) for _ in range(self.hasher.get_n_bands())
        ]

    def add(self, item, label=None):
        """
        Add an `item` to the cluster.
        
        Optionally, use a `label` to reference this `item`.
        Otherwise, the `item` itself is used as the label.
        """
        # Ensure label for this item
        if label is None:
            label = item

        # Add to unionfind structure
        self.unions[label]

        # Get item signature
        sig = self.signer.sign(item)

        # Unite labels with the same LSH keys in the same band
        for band_idx, hashval in enumerate(self.hasher.hash(sig)):
            self.hashmaps[band_idx][hashval].append(label)
            self.unions.union(label, self.hashmaps[band_idx][hashval][0])

    def groups(self):
        """
        Get the clustering result.
        
        Returns sets of labels.
        """
        return self.unions.sets()

    def match(self, item):
        """
        Get a set of matching labels for `item`.
        
        Returns a (possibly empty) set of labels.
        """
        # Get signature
        sig = self.signer.sign(item)
        
        matches = set()
        
        for band_idx, hashval in enumerate(self.hasher.hash(sig)):
            if hashval in self.hashmaps[band_idx]:
                matches.update(self.hashmaps[band_idx][hashval])
        
        return matches

# trick is to implement hashmaps in sql
# Looping over patent assignees f:
#  1. Find sig and LSH: table(f_id,hash_1,...,hash_n) where n is n_bands
#  2. A firm is a potential match if it shares any of hash_1 through hash_n with the existing set
#  3. If not a match, create a new bucket. If a match, add to that bucket
# Build iteratively, this could be done in memory or not

# output - table mapping from patents to buckets
# the higher the threshhold and the lower the hash size, the larger the number of buckets

# Now when doing firm match, run matching code within each bucket separately
# then merge at the end. Speed gain is quadratic in number of buckets.

import sqlite3
from standardize import name_standardize

def generate_namestd():
    con = sqlite3.connect('store/patents.db')
    cur = con.cursor()

    cur.execute('drop table if exists patent_std')
    cur.execute('create table patent_std as (patnum int, owner text)')

    for (patnum,name) in cur.execute("select patnum,owner from patent where owner!=''"):
        name = ' '.join(name_standardize(name))
        if name:
            cur.execute('insert into patent_std vales (?,?)',(patnum,name))

    con.commit()
    con.close()

def firm_buckets(cur,npat=None,kshingle=2,**kwargs):
    c = Cluster(**kwargs)

    cmd = "select patnum,owner from patent where owner!=''"
    if npat: cmd += " limit {}".format(npat)

    name_dict = {}
    for (patnum,name) in cur.execute(cmd):
        name = ' '.join(name_standardize(name))
        if not name: continue
        name_dict[patnum] = name
        c.add(frozenset(shingle(name,kshingle)),label=patnum)

    return [[name_dict[pn] for pn in grp] for grp in c.groups()]
