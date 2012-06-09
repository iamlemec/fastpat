#!/usr/bin/python

import sqlite3
import numpy as np
import mecstat as mec
import datetime
import matplotlib.pylab as plt

if 'loaded' not in vars():
  loaded = False
else:
  loaded = True

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
pat_year_valid = pat_file_year[(pat_file_year>=0)&(pat_file_year<len_year)]
year_counts = np.bincount(pat_year_valid.astype(int),minlength=len_year)
print year_counts

# get age-year counts
for age in age_vec:
  age_counts[age-base_age] = np.sum(year_counts[:(len_year-1-age)])

assign_npy = 'store/assignments.npy'
if not loaded:
  datm_assign = np.load(assign_npy)
#patnum = datm_assign[:,0]
file_date = datm_assign[:,1]
exec_date = datm_assign[:,5]
#ctype = datm_assign[:,5]

assign_lag = exec_date - file_date
assign_year = np.floor(assign_lag/365.25)
assign_valid = assign_year[(assign_year>=0)&(assign_year<len_age)]
assign_counts = np.bincount(assign_valid.astype(int),minlength=len_age).astype(np.float)
assign_frac = assign_counts/age_counts

#plt.plot(age_vec,assign_frac)
#plt.show()

exec_year = np.floor(exec_date/365.25)
exec_valid = exec_year[(exec_year>=0)&(exec_year<len_year)]
exec_counts = np.bincount(exec_valid.astype(int),minlength=len_year).astype(np.float)
print exec_counts

trans_frac = exec_counts/year_counts
trans_smooth = 0.5*(trans_frac[:-1:2]+trans_frac[1::2])
year_smooth = year_vec[::2]

plt.plot(year_smooth[:-5],trans_smooth[:-5])
plt.show()

#assign_good = assign_lag[(assign_lag>-3000)&(assign_lag<11000)]
#plt.hist(assign_good,100,normed=True)
#plt.show()

#grant_lag = exec_date - grnt_date
#grant_with = grant_lag[has_dates]
#grant_sort = np.sort(grant_with)
#grant_valid = (grant_with>-5000) & (grant_with<10000)
#grant_good = grant_with[grant_valid]
#plt.hist(grant_good,100,normed=True)
#plt.show()

