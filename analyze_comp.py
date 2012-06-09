# import stuff
import mecstat as mec

# file names
fname_comp_npy = 'store/compustat_trans.npy'

# load compustat data - this is presorted by (gvkey,year)
fy_array = np.load(fname_comp_npy)
n_fy = len(fy_array)

# unpack
gvkey = fy_array['gvkey']
year = fy_array['year']
inc = fy_array['income']
revn = fy_array['revenue']
rnd = fy_array['rnd']
naics = fy_array['naics']
source = fy_array['source_pnum']
dest = fy_array['dest_pnum']
grant = fy_array['grant_pnum']

# adjacency
has_adj = (gvkey[:-1]==gvkey[1:])&((year[:-1]+1)==year[1:])

# next year
has_next = np.concatenate((has_adj,[False]))
next_idx = np.arange(n_fy)[has_next]+1
sel_next = np.zeros(n_fy).astype(np.bool)
sel_next[next_idx] = True

# prev year
has_prev = np.concatenate(([False],has_adj))
prev_idx = np.arange(n_fy)[has_prev]-1
sel_prev = np.zeros(n_fy).astype(np.bool)
sel_prev[prev_idx] = True

# next period revenue
revn_next = mec.nans(n_fy)
revn_next[has_next] = revn[sel_next]
revn_prev = mec.nans(n_fy)
revn_prev[has_prev] = revn[sel_prev]

# calculate growth rate
growth = (revn_next-revn)*2.0/(revn+revn_next)
lgrowth = mec.log_nan(revn_next/revn)
has_growth = ~np.isnan(growth)
has_lgrowth = ~np.isnan(lgrowth)

# profits
prof = inc/revn
prof[np.isinf(prof)] = np.nan
prof_next = mec.nans(n_fy)
prof_next[has_next] = prof[sel_next]
has_prof = ~np.isnan(prof)
has_prof_next = ~np.isnan(prof_next)

# income
inc_next = mec.nans(n_fy)
inc_next[has_next] = prof[sel_next]
has_inc = ~np.isnan(inc)
has_inc_next = ~np.isnan(inc_next)

# patents
source_prev = mec.nans(n_fy)
source_prev[has_prev] = source[sel_prev]
dest_prev = mec.nans(n_fy)
dest_prev[has_prev] = dest[sel_prev]
grant_prev = mec.nans(n_fy)
grant_prev[has_prev] = grant[sel_prev]

source_next = mec.nans(n_fy)
source_next[has_next] = source[sel_next]
dest_next = mec.nans(n_fy)
dest_next[has_next] = dest[sel_next]
grant_next = mec.nans(n_fy)
grant_next[has_next] = grant[sel_next]

# prof diff
prof_diff = prof_next-prof
has_prof_diff = ~np.isnan(prof_diff)

# construct patent stocks
rho = 0.9 # anti-depreciation rate of knowledge stock
kbase = 1.0 # knowledge stock base
lagmax = 70
ever_pats = grant > 0
pat_stock = np.zeros(n_fy)
sel_t = np.arange(n_fy)
disc = 1.0
n_left_vec = np.zeros(lagmax)

for t in range(lagmax):
  sel_t = sel_t[has_next[sel_t]]+1
  disc *= rho

  pat_stock[sel_t] += disc*grant[sel_t-t]
  ever_pats[sel_t-t-1] |= grant[sel_t] > 0

  n_left = len(sel_t)
  n_left_vec[t] = n_left

  if n_left == 0:
    break

