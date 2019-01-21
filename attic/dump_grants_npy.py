#!/usr/bin/python

import sys
import datetime
import sqlite3
import numpy as np
import calendar

# file names
db_fname = 'store/patents.db'
np_fname = 'store/grants.npy'

# date conversion - base 1950/01/01
base_date = datetime.date(1950,1,1)
def make_date(dstr):
  if dstr != None:
    year = int(dstr[:4])
    month = max(1,int(dstr[5:7]))
    day = min(calendar.monthrange(year,month)[1],max(1,int(dstr[8:10])))
    date = datetime.date(year,month,day)
    days = (date-base_date).days
  else:
    days = np.nan
  return days

# open db
conn = sqlite3.connect(db_fname)
cur = conn.cursor()

# allocate output arrays
vsize = int(cur.execute('select count(*) from patent').fetchone()[0])
outp_vec = np.zeros((vsize,3))
print vsize

# loop through rows
lnum = 0
ret = cur.execute('select * from patent')
for row in ret:
  (pat_num_str,file_date_str,grnt_date_str,_) = row

  # patent number
  pat_num = int(pat_num_str)

  # parse file, grant, and exec dates
  file_date = make_date(file_date_str)
  grnt_date = make_date(grnt_date_str)

  #print (lnum,pat_num,file_date,grnt_date,exec_date,ctype)
  outp_vec[lnum,:] = (pat_num,file_date,grnt_date)

  lnum += 1

# save lag vector
np.save(np_fname,outp_vec)

# close db
conn.close()

