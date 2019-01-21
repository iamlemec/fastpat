#!/usr/bin/python

import re
import sys
import datetime
import sqlite3
import numpy as np
import calendar

# file names
db_fname = 'store/patents.db'
np_fname = 'store/assignments.npy'

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

# detect conveyance type
conv_map = dict({'ASSIGN' : 1, 'MERGER' : 2, 'LICENSE' : 3})
conv_re = re.compile('|'.join(conv_map.keys()))
def conv_type(cstr):
  ret = conv_re.search(cstr)
  if ret == None:
    return 0
  else:
    return conv_map.get(ret.group())

# open db
conn = sqlite3.connect(db_fname)
cur = conn.cursor()

# allocate output arrays
vsize = int(cur.execute('select count(*) from assignment_use').fetchone()[0])
outp_vec = np.zeros((vsize,8))
conv_sum = np.zeros(4)
print vsize

# loop through rows
lnum = 0
ret = cur.execute('select patnum,filedate,grantdate,classone,classtwo,execdate,recdate,conveyance from assignment_use')
for row in ret:
  (pat_num,file_date_str,grnt_date_str,class_one,class_two,exec_date_str,recd_date_str,conveyance) = row

  if type(class_one) != int or type(class_two) != int:
    print row
    continue

  # parse file, grant, and exec dates
  file_date = make_date(file_date_str)
  grnt_date = make_date(grnt_date_str)
  exec_date = make_date(exec_date_str)
  recd_date = make_date(recd_date_str)

  # detect conveyance type
  ctype = conv_type(conveyance)
  conv_sum[ctype] += 1

  #print (lnum,pat_num,file_date,grnt_date,exec_date,ctype)
  outp_vec[lnum,:] = (pat_num,file_date,grnt_date,class_one,class_two,exec_date,recd_date,ctype)

  lnum += 1

# save lag vector
np.save(np_fname,outp_vec)

# close db
conn.close()

# print summary of types
print conv_sum/lnum

