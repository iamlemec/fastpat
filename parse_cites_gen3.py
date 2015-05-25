#!/usr/bin/python

import sys
from lxml.etree import iterparse
import sqlite3

# handle arguments
if len(sys.argv) <= 1:
  print 'Usage: parse_cites_gen3.py filename store_db'
  sys.exit(0)

in_fname = sys.argv[1]
if sys.argv[2] == '1':
  store_db = True
else:
  store_db = False

if store_db:
  # database file
  db_fname = 'store/patents.db'
  conn = sqlite3.connect(db_fname)
  cur = conn.cursor()
  try:
    cur.execute("create table citation (citer int, citee int)")
  except sqlite3.OperationalError as e:
    pass

# citations generator
def citee_gen(cite_vec):
  for cite in cite_vec:
    try:
      docid = cite[0][0]
      if docid[0].text == 'US' and docid[2].text in ['A','B1']:
        yield int(docid[1].text)
      else:
        pass
    except:
      pass

# store for batch commit
batch_size = 1000
citations = []

def commitBatch():
  if store_db:
    cur.executemany('insert into citation values (?,?)',citations)
  del citations[:]

# get an iterable
context = iterparse(in_fname,tag='us-patent-grant',remove_blank_text=True)
context = iter(context)
(event,root) = context.next()

# parseahol
pcount = 0
ccount = 0
for (event,elem) in context:
    # top-level section
    bib = elem[0]
    pubref = bib.find('publication-reference')
    cites = bib.find('references-cited')
    if cites is None: continue

    # patent info
    citer = pubref[0][1].text
    if citer[0] != '0': continue
    citer = int(citer[1:])

    # roll it in
    citations += [(citer,citee) for citee in citee_gen(cites)]
    if len(citations) == batch_size:
      commitBatch()

    # stats
    pcount += 1
    ccount += len(cites)

# clear out the rest
if len(citations) > 0:
  commitBatch()

if store_db:
  # commit to db and close
  conn.commit()
  cur.close()
  conn.close()

print pcount
print ccount
