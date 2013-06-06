#!/usr/bin/python

import sqlite3
import numpy as np
import mecstat as mec
import datetime
import matplotlib.pylab as plt

if 'lags_loaded' not in vars():
  lags_loaded = False
else:
  lags_loaded = True

# set up date ranges
base_year = 1950
max_year = 2011
len_year = max_year-base_year+1
year_vec = np.arange(base_year,max_year+1)
year_counts = np.zeros(len_year)

base_age = 0
max_age = 25
len_age = max_age-base_age+1
age_vec = np.arange(base_age,max_age+1)
age_counts = np.zeros(len_age)

# get raw year counts
grant_npy = 'store/grants.npy'
if not loaded:
  datm_grant = np.load(grant_npy)
pat_file_date = datm_grant[:,1]
pat_file_year = np.floor(pat_file_date/365.25) # yeah, that right
pat_file_valid = pat_file_year[(pat_file_year>=0)&(pat_file_year<len_year)]
pat_file_counts = np.bincount(pat_file_valid.astype(int),minlength=len_year)

# get age-year counts
for age in age_vec:
  age_counts[age-base_age] = np.sum(pat_file_counts[:(len_year-1-age)])

assign_npy = 'store/assignments.npy'
if not loaded:
  datm_assign = np.load(assign_npy)
#patnum = datm_assign[:,0]
assign_file_date = datm_assign[:,1]
assign_exec_date = datm_assign[:,5]
#ctype = datm_assign[:,5]

assign_lag = assign_exec_date - assign_file_date
assign_lag_year = np.floor(assign_lag/365.25)
assign_lag_valid = assign_lag_year[(assign_lag_year>=0)&(assign_lag_year<len_age)]
assign_lag_counts = np.bincount(assign_lag_valid.astype(int),minlength=len_age).astype(np.float)
assign_age_frac = assign_lag_counts/age_counts

# bin assignments by execyear
assign_exec_year = np.floor(assign_exec_date/365.25)
assign_exec_valid = assign_exec_year[(assign_exec_year>=0)&(assign_exec_year<len_year)]
assign_exec_counts = np.bincount(assign_exec_valid.astype(int),minlength=len_year).astype(np.float)

# bin assignments by patent file year
assign_file_year = np.floor(assign_file_date/365.25)
assign_file_valid = assign_file_year[(assign_file_year>=0)&(assign_file_year<len_year)]
assign_file_counts = np.bincount(assign_file_valid.astype(int),minlength=len_year).astype(np.float)

# bin assignments within 10 years by patent file year
assign_within10_file_valid = assign_file_year[(assign_file_year>=0)&(assign_file_year<len_year)&(assign_lag<10.0)]
assign_within10_file_counts = np.bincount(assign_within10_file_valid.astype(int),minlength=len_year).astype(np.float)

# fraction of patents filed for in year X that are reassigned
frac_reassigned = assign_file_counts/pat_file_counts
frac_reassigned_5yr = np.add.reduceat(assign_file_counts,np.arange(len_year)[::5])/np.add.reduceat(pat_file_counts,np.arange(len_year)[::5])
frac_reassigned_within10 = assign_within10_file_counts/pat_file_counts

plt.plot(year_vec[:-6],assign_exec_counts[6:]/pat_file_counts[:-6])
plt.axis(ymin=0.0)
plt.show()

# adjust assigned counts for truncation bias
truncation_bias = np.cumsum(assign_age_frac[::-1])
truncation_bias = truncation_bias/truncation_bias[-1]
assign_file_adjust = assign_file_counts.copy()
assign_file_adjust[-max_age-1:] /= (1.0-truncation_bias)
frac_reassigned_adjust = assign_file_adjust/pat_file_counts
frac_reassigned_adjust_5yr = np.add.reduceat(assign_file_adjust,np.arange(len_year)[::5])/np.add.reduceat(pat_file_counts,np.arange(len_year)[::5])
#plt.plot(year_vec[::5][:-1]-2.5,frac_reassigned_5yr[:-1])
plt.plot(year_vec[::5][:-1]-2.5,frac_reassigned_adjust_5yr[:-1])
plt.show()

#grant_lag = exec_date - grnt_date
#grant_with = grant_lag[has_dates]
#grant_sort = np.sort(grant_with)
#grant_valid = (grant_with>-5000) & (grant_with<10000)
#grant_good = grant_with[grant_valid]
#plt.hist(grant_good,100,normed=True)
#plt.show()

