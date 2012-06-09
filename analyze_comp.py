# import stuff
import numpy as np
import mecstat as mec
import scipy.weave as weave
from scipy.weave import converters

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

# unique gvkeys
gvkeys_all = np.unique(gvkey)
n_firms = len(gvkeys_all)

# construct firm panel
age_max = 70 # max age is 60
firm_panel = -np.ones((n_firms,age_max),dtype=np.int)
firm_life = np.zeros(n_firms,dtype=np.int)
weave.inline("""
int gv_cur = -1;
int f = -1;
int gv;
int base_year;
int year_idx;
for (int i = 0; i < n_fy; i++) {
  gv = gvkey(i);
  if (gv != gv_cur) {
    if (f >= 0) firm_life(f) = year_idx+1;
    gv_cur = gv;
    base_year = year(i);
    f++;
  }
  year_idx = year(i)-base_year;
  firm_panel(f,year_idx) = i;
}
""",['gvkey','year','firm_panel','firm_life','n_fy'],type_converters=converters.blitz)
sel_panel = firm_panel != -1

# patent stocks
rho = 0.94 # anti-depreciation rate of knowledge stock
pat_stock_panel = np.zeros((n_firms,age_max))
sel_age = firm_panel[:,0] != -1
pat_stock_panel[sel_age,0] = grant[firm_panel[sel_age,0]]
for age in range(1,age_max):
  sel_alive = age < firm_life
  sel_age = sel_panel[:,age]
  pat_stock_panel[sel_alive,age] = rho*pat_stock_panel[sel_alive,age-1]
  pat_stock_panel[sel_age,age] += grant[firm_panel[sel_age,age]]

# map back into flat format
pat_stock = pat_stock_panel[sel_panel]

# yearly data
base_year = 1950
max_year = 2010
len_year = max_year-base_year+1
year_vec = np.arange(base_year,max_year+1)
grant_tot = np.zeros(len_year)
source_tot = np.zeros(len_year)
dest_tot = np.zeros(len_year)
for t in range(len_year):
  yr = base_year+t
  grant_tot[t] = np.sum(grant[year==yr])
  source_tot[t] = np.sum(source[year==yr])
  dest_tot[t] = np.sum(dest[year==yr])

