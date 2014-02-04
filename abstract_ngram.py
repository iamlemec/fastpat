import datetime
import time
from itertools import izip
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

datefmt = '%Y%m%d'
def strtodate(s):
  return datetime.datetime.fromtimestamp(time.mktime(time.strptime(s,datefmt))).date()

# load database
db_fname = 'store/abstracts.db'
conn = sqlite3.connect(db_fname)
cur = conn.cursor()

# tokenize abstracts into dated unigrams
cur.execute('drop table if exists ngram')
cur.execute('create table ngram (date text, label text, patnum int)')

punctuation = re.compile('\W')
depunct = lambda s: punctuation.sub('',s).lower()

for (patnum,filedate,abstext) in cur.execute('select * from abstract').fetchall():
  labels = map(depunct,abstext.split())
  cur.executemany('insert into ngram values (?,?,?)',izip(len(labels)*[filedate],labels,len(labels)*[patnum]))

# generate unique tokens
cur.execute('drop table if exists utoken')
cur.execute('create table utoken (label text)')
cur.execute('insert into utoken select distinct label from ngram')

# hash map tokens
(ntoks,) = cur.execute('select count(*) from utoken').fetchone()
tokens = unfurl(cur.execute('select label from utoken order by label').fetchall())
tokmap = dict(zip(tokens,range(ntoks)))
cumfreq = np.zeros(ntoks)

# storage
cur.execute('drop table if exists cumfreq')
cur.execute('create table cumfreq (date text, label text, cfreq real)')

# decay rate
decay_year = 0.1 # yearly
discount = 1.0-(decay_year/365.25) # daily

(start_date,end_date) = map(strtodate,cur.execute('select min(date),max(date) from ngram').fetchone())
for d in daterange(start_date,end_date):
  datestr = d.strftime(datefmt)
  print datestr

  # insert todays history
  cur.executemany('insert into cumfreq values (?,?,?)',izip(ntoks*[datestr],tokens,cumfreq))

  # update weightings
  labels = unfurl(cur.execute('select label from ngram where date=?',(datestr,)).fetchall())
  indexes = map(tokmap.get,labels)
  cumfreq *= discount
  cumfreq[indexes] += 1.0
  cumfreq /= np.sum(cumfreq)

# clean up
conn.commit()
cur.close()
conn.close()
