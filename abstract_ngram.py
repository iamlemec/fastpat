import datetime
import time
from itertools import izip
from collections import defaultdict
import re
import sqlite3
import numpy as np

# useful functions
def unfurl(ret):
  v = zip(*ret)
  return v[0] if len(v) > 0 else []

def daterange(start_date,end_date):
  for i in range(int((end_date-start_date).days)):
    yield start_date + datetime.timedelta(i)

def nextmonth(year,month):
  if month == 12:
    return (year+1,1)
  else:
    return (year,month+1)

def monthstr(year,month):
  return '%4i%02i01' % (year,month)

def monthrange(start_date,end_date):
  (cur_month,cur_year) = (start_date.month,start_date.year)
  (end_month,end_year) = (end_date.month,end_date.year)
  while (cur_month,cur_year) != (end_month,end_year):
    yield (cur_year,cur_month)
    (cur_year,cur_month) = nextmonth(cur_year,cur_month)

datefmt = '%Y%m%d'
def strtodate(s):
  return datetime.datetime.fromtimestamp(time.mktime(time.strptime(s,datefmt))).date()

# load database
db_fname = 'store/abstracts.db'
con = sqlite3.connect(db_fname)
cur = con.cursor()

# tokenize abstracts into dated unigrams
cur.execute('drop table if exists ngram')
cur.execute('create table ngram (date text, label text, patnum int)')
cur.execute('create index date_idx on ngram (date)')

punctuation = re.compile('\W')
depunct = lambda s: punctuation.sub('',s).lower()

#(start_str,end_str) = ('20070101','20070601')
(start_str,end_str) = cur.execute('select min(filedate),max(filedate) from abstract').fetchone()
(start_date,end_date) = map(strtodate,(start_str,end_str))

i = 0
for (patnum,filedate,abstext) in cur.execute('select * from abstract where filedate>=? and filedate<?',(start_str,end_str)).fetchall():
  labels = map(depunct,abstext.split())
  cur.executemany('insert into ngram values (?,?,?)',izip(len(labels)*[filedate],labels,len(labels)*[patnum]))
  i += 1
  if i == 10000:
    print patnum
    i = 0

# storage
cur.execute('drop table if exists cumfreq')
cur.execute('drop table if exists totfreq')
cur.execute('create table cumfreq (date text, label text, cfreq real)')
cur.execute('create index label_idx on cumfreq (label)')
cur.execute('create table totfreq (date text, tfreq real)')

# decay rate
decay_year = 0.1 # yearly
discount = 1.0-(decay_year/365.25) # daily

# hash map tokens
tokmap = defaultdict(int)

for (y,m) in monthrange(start_date,end_date):
  d = monthstr(y,m)
  dp = monthstr(*nextmonth(y,m))
  print (d,dp)

  # insert todays history
  cur.executemany('insert into cumfreq values (?,?,?)',izip(len(tokmap)*[d],*izip(*tokmap.items())))
  cur.execute('insert into totfreq values (?,?)',(d,sum(tokmap.values())))

  # update weightings
  for s in tokmap.keys(): tokmap[s] *= discount
  labels = unfurl(cur.execute('select label from ngram where date>=? and date<?',(d,dp)).fetchall())
  print len(labels)
  for s in labels: tokmap[s] += 1

# normalize
cur.execute('drop table if exists cumfreq2')
cur.execute('create table cumfreq2 (date text, label text, cfreq real, rfreq real)')
cur.execute('create index label_idx on cumfreq2 (label)')
cur.execute("""insert into cumfreq2 select cumfreq.date,label,cfreq,cfreq/tfreq as rfreq
               from cumfreq join totfreq on cumfreq.date = totfreq.date""")
cur.execute('drop table cumfreq')
cur.execute('alter table cumfreq2 rename to cumfreq')

# clean up
con.commit()
cur.close()
con.close()
