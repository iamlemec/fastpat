#
# locally sensitive hashing code
#

from collections import defaultdict
import numpy as np
import xxhash

import pyximport
pyximport.install()
import simcore as csimcore

# k-shingles: pairs of adjacent k-length substrings (in order)
def shingle(s, k=2):
    k = min(len(s), k)
    for i in range(len(s) - k + 1):
        yield s[i:i+k]

# split into words
def tokenize(s):
    return s.split()

def murmur(x):
    return np.uint64(xxhash.xxh64_intdigest(x))

# compute actual simhash
class Simhash:
    def __init__(self):
        self.dim = 64
        self.unums = list(map(np.uint64,range(self.dim)))
        self.masks = [self.unums[1] << n for n in self.unums]

    def simhash(self, features, weights=None):
        if weights is None:
            weights = [1.0]*len(features)
        hashish = map(murmur,features)
        v = [0.0]*self.dim
        for (h,w) in zip(hashish,weights):
            for i in range(self.dim):
                v[i] += w if h & self.masks[i] else -w
        ans = self.unums[0]
        for i in range(self.dim):
            if v[i] >= 0:
                ans |= self.masks[i]
        return ans

# compute actual simhash with C - only 64 width
class CSimhash():
    def __init__(self):
        self.simcore = csimcore.simcore

    def simhash(self, features, weights=None):
        if weights is None:
            weights = [1.0]*len(features)
        hashish = [murmur(f) for f in features]
        ret = np.uint64(self.simcore(hashish,weights))
        return ret

class Cluster:
    # dim is the simhash width, k is the tolerance
    def __init__(self, dim=64, k=4, thresh=1):
        self.dim = dim
        self.k = k
        self.thresh = thresh

        self.unions = []
        self.hashmaps = [defaultdict(list) for _ in range(k)] # defaultdict(list)
        self.offsets = [np.uint64(dim//k*i) for i in range(k)]
        self.bin_masks = [np.uint64(2**(dim-offset)-1) if (i == len(self.offsets)-1) else np.uint64(2**(self.offsets[i+1]-offset)-1) for (i,offset) in enumerate(self.offsets)]

        self.csim = CSimhash()
        self.hasher = self.csim.simhash

    # add item to the cluster
    def add(self, features, label, weights=None):
        # get subkeys
        sign = self.hasher(features,weights)
        keyvec = self.get_keys(sign)

        # Unite labels with the same keys in the same band
        matches = defaultdict(int)
        for idx, key in enumerate(keyvec):
            others = self.hashmaps[idx][key]
            for l in others:
                matches[l] += 1
            others.append(label)
        for out, val in matches.items():
           if val > self.thresh:
               self.unions.append((label,out))

    # bin simhash into chunks
    def get_keys(self, simhash):
        return [simhash >> offset & mask for (offset,mask) in zip(self.offsets,self.bin_masks)]
