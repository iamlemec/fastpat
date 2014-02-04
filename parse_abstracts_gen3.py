#!/usr/bin/python

import sys
from lxml.etree import iterparse,tostring
import sqlite3

# handle arguments
if len(sys.argv) <= 1:
  print 'Usage: parse_abstracts_gen3.py filename store_db'
  sys.exit(0)

in_fname = sys.argv[1]
if sys.argv[2] == '1':
  store_db = True
else:
  store_db = False

if store_db:
  # database file
  db_fname = 'store/abstracts.db'
  conn = sqlite3.connect(db_fname)
  cur = conn.cursor()
  try:
    cur.execute('create table abstract (patnum int, filedate text, full_text text)')
  except sqlite3.OperationalError as e:
    print e
else:
  cur = None

# store for batch commit
batch_size = 1000
abstracts = []

def commitBatch():
  if store_db:
    cur.executemany('insert into abstract values (?,?,?)',abstracts)
  del abstracts[:]

def get_text(e):
  text = e.text or ''
  text += ' '.join([tostring(se) for se in e.getchildren()])
  return text

# get an iterable
context = iter(iterparse(in_fname,tag='us-patent-grant',remove_blank_text=True))
(event,root) = context.next()

# parseahol
pcount = 0
for (event,elem) in context:
    # top-level section
    bib = elem.find('us-bibliographic-data-grant')
    pubref = bib.find('publication-reference')
    appref = bib.find('application-reference')

    # patent number
    pubinfo = pubref.find('document-id')
    patnum = pubinfo.find('doc-number').text
    if patnum[0] != '0': continue
    patnum = int(patnum[1:])

    # filing date
    appinfo = appref.find('document-id')
    date = appinfo.find('date').text

    # roll it in
    abstract = elem.find('abstract')
    abstext = ' '.join([get_text(p) for p in abstract.getchildren()])
    abstracts.append((patnum,date,abstext))
    if len(abstracts) == batch_size:
      commitBatch()

    # stats
    pcount += 1

# clear out the rest
if len(abstracts) > 0:
  commitBatch()

if store_db:
  # commit to db and close
  conn.commit()
  cur.close()
  conn.close()

print pcount
