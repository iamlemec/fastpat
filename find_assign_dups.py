#!/usr/bin/python

import sys
import re
import sqlite3

# name standardizers
punctuation = '[^\w\s]'
punct_re = re.compile(punctuation)
generics = ['THE','A','OF','AND','CORPORATION','INCORPORATED','COMPANY','LIMITED','KABUSHIKI','KAISHA','INC','LLC','LTD','CORP','AG','NV','BV','GMBH','CO','BV','HOLDINGS','GROUP','INDUSTRIES','TECHNOLOGY','SEMICONDUCTOR','TECHNOLOGIES','COMMUNICATIONS','INTERNATIONAL','MANUFACTURING','LABORATORIES','ELECTRIC','PERIPHERALS','SYSTEMS','TECH']
gener_re = re.compile('|'.join(['\\b{}\\b'.format(el) for el in generics]))

# detect conveyance type
convey = ['MERGER','LICENSE']
conv_re = re.compile('|'.join(convey))

# open db
db_fname = 'store/patents.db'
conn = sqlite3.connect(db_fname)
cur = conn.cursor()

rlim = sys.maxint

match_num = 0

ret = cur.execute('select patnum,assignor,assignee,conveyance from assignment')
rnum = 0
for row in ret:
  (patnum,assignor,assignee,conveyance) = row

  assignor_strip = gener_re.sub('',punct_re.sub(' ',assignor))
  assignee_strip = gener_re.sub('',punct_re.sub(' ',assignee))

  assignor_toks = assignor_strip.split()
  assignee_toks = assignee_strip.split()

  ctype_good = conv_re.search(conveyance) != None

  match = False
  if not ctype_good:
    for tok in assignor_toks:
      if len(tok) > 2 and tok in assignee_toks:
        match = True
        break

  #if rnum%1000 == 0 and match == False and ctype_good == False:
  #  #print '{:1}, {:7}, {:20.20}: {:50.50} -> {:50.50}'.format(match,patnum,tok,assignor,assignee)
  #  #print '{:1}, {:7}: {:50.50} -> {:50.50}'.format(match,patnum,assignor,assignee)

  match_num += match

  rnum += 1
  if rnum >= rlim:
    break

conn.close()

print match_num
print rnum
print rnum-match_num
print float(match_num)/float(rnum)

