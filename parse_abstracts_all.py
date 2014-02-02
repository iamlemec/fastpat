#!/usr/bin/python

from xml.sax import handler, make_parser, SAXException
import sys
import sqlite3
import re

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
    cur.execute('drop table if exists abstract')
    cur.execute('drop table if exists abstract_key')
    cur.execute('create table abstract (patnum int, full_text text)')
    cur.execute('create table abstract_key (patnum int, keyword text)')
  except sqlite3.OperationalError as e:
    print e
else:
  cur = None

# store for batch commit
rlim = sys.maxint
batch_size = 1000

# regexp
punct = r"[^\w\s]"
punct_re = re.compile(punct)

# record limit error
class RecordLimitError(Exception):
  def __init__(self):
    pass
  def __str__(self):
    return 'Reached record limit'

# SAX hanlder for gen3 patent grants
class GrantHandler(handler.ContentHandler):
  def __init__(self):
    pass

  def startDocument(self):
    self.in_pubref = False
    self.in_patnum = False
    self.in_paragraph = False
    self.completed = 0
    self.abstracts = []
    self.abstract_keys = []

  def endDocument(self):
    if len(self.abstracts) > 0:
      self.commitBatch()

  def startElement(self, name, attrs):
    if name == 'us-patent-grant':
      self.patnum = ''
      self.abstract = ''
    elif name == 'publication-reference':
      self.in_pubref = True
    elif name == 'doc-number':
      if self.in_pubref:
        self.in_patnum = True
    elif name == 'p':
      self.in_paragraph = True

  def endElement(self, name):
    if name == 'us-patent-grant':
      if self.patnum[0] == '0':
        self.addPatent()
    elif name == 'publication-reference':
      self.in_pubref = False
    elif name == 'doc-number':
      self.in_patnum = False
    elif name == 'p':
      self.in_paragraph = False

  def characters(self, content):
    if self.in_patnum:
      self.patnum += content
    if self.in_paragraph:
      self.abstract += content

  def commitBatch(self):
    if store_db:
      cur.executemany('insert into abstract values (?,?)',self.abstracts)
      cur.executemany('insert into abstract_key values (?,?)',self.abstract_keys)
    del self.abstracts[:]
    del self.abstract_keys[:]

  def addPatent(self):
    self.completed += 1

    patint = self.patnum[1:]
    abstract_esc = self.abstract.encode('ascii','ignore')

    #print '{:7} -- {:.80}'.format(patint,abstract_esc)

    abstract_strip = abstract_esc
    abstract_strip = punct_re.sub(' ',abstract_strip)
    toks = abstract_strip.split()

    self.abstracts.append((patint,abstract_esc))
    self.abstract_keys += zip([patint]*len(toks),toks)

    if len(self.abstracts) == batch_size:
      self.commitBatch()

    if self.completed >= rlim:
      raise RecordLimitError

# do parsing
parser = make_parser()
grant_handler = GrantHandler()
parser.setContentHandler(grant_handler)
try:
  parser.parse(in_fname)
except RecordLimitError as e:
  print e

# clear out the rest

if store_db:
  # index that shit
  cur.execute('delete from abstract_key where rowid not in (select min(rowid) from abstract_key group by patnum,keyword)')
  cur.execute('create unique index abstract_key_idx on abstract_key (patnum asc, keyword asc)')

  # commit to db and close
  conn.commit()
  cur.close()
  conn.close()

print grant_handler.completed

