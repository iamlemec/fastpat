DEF dim = 64

cdef unsigned long masks[dim]
for i in range(dim):
  masks[i] = 1 << i

cdef float v[dim]

def simcore(hashish, weights):
    cdef unsigned long long ans
    cdef int n = len(hashish)
    cdef unsigned long long h
    cdef float w
    cdef float q

    for j in range(dim):
        v[j] = 0.0

    for i in range(n):
        h = hashish[i]
        w = weights[i]
        for j in range(dim):
            if h & masks[j]:
              q = w
            else:
              q = -w
            v[j] += q

    ans = 0
    for j in range(dim):
        if v[j] >= 0:
            ans |= masks[j]

    return ans
