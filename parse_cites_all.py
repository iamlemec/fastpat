#!/usr/bin/python

import os
import csv
import sqlite3
import itertools

# connect to nber db
db_fname = 'store/citations.db'
con = sqlite3.connect(db_fname)
cur = con.cursor()

# citation table
cur.execute('drop table if exists citation')
cur.execute('create table citation (citer int, citee int)')
cmd_cite = 'insert into citation values (?,?)'

# pull in NBER <=2006 citations
cite_fname = 'nber_files/cite76_06.csv'
citeReader = csv.reader(open(cite_fname,'rb'),delimiter='\t',quotechar='\"')
next(citeReader,None) # skip header row
batch_size = 1000000

rnum = 0
print 'Citations'
while True:
  citations = [(row[0],row[1]) for row in itertools.islice(citeReader,batch_size)]
  if len(citations) > 0:
    cur.executemany(cmd_cite,citations)
  else:
    break

  rnum += len(citations)
  print rnum

# commit and close
con.commit()
con.close()

# parse generation 3 citations files
grant_dir = 'grant_files'
cmd_fmt = 'python parse_cites_gen{}.py grant_files/{} 1'

gen3_files = []

for f in os.listdir(grant_dir):
  if f.startswith('ipgb') and f.endswith('.xml'):
    gen3_files.append(f)

gen3_files.sort()

for f in gen3_files:
  print '{}: gen 3'.format(f)
  cmd = cmd_fmt.format(3,f)
  os.system(cmd)

# remove duplicates and aggregate by patent
con = sqlite3.connect(db_fname)
cur = con.cursor()

cur.execute('drop table if exists citation2')
cur.execute('create table citation2 (citer int, citee int)')
cur.execute('insert into citation2 select distinct citer,citee from citation')
cur.execute('drop table citation')
cur.execute('alter table citation2 rename to citation')

# clean up
con.commit()
con.close()
