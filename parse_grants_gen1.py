#!/usr/bin/python

import sys
import sqlite3

# handle arguments
if len(sys.argv) <= 1:
  print 'Usage: parse_grants_gen1.py filename store_db'
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
    cur.execute("create table patent (patnum int, filedate text, grantdate text, classone int, classtwo int)")
  except sqlite3.OperationalError as e:
    print e

# store for batch commit
batch_size = 1000
patents = []

def commitBatch():
  if store_db:
    cur.executemany('insert into patent values (?,?,?,?,?)',patents)
  del patents[:]

# SAX hanlder for gen1 patent grants
class GrantHandler:
  def __init__(self):
    self.in_patent = False
    self.section = ''

    self.completed = 0

  def tag(self, name, text):
    if len(text) == 0:
      self.section = name

    if name == 'PATN':
      if self.in_patent:
        if self.patnum[0] == '0':
          self.addPatent()
      self.in_patent = True
      self.patnum = ''
      self.file_date = ''
      self.grant_date = ''
      self.class_one = ''
      self.class_two = ''
    elif name == 'WKU':
      if self.section == 'PATN':
        self.patnum = text
    elif name == 'APD':
      if self.section == 'PATN':
        self.file_date = text
    elif name == 'ISD':
      if self.section == 'PATN':
        self.grant_date = text
    elif name == 'OCL':
      if self.section == 'CLAS':
        self.class_str = text

  def addPatent(self):
    self.completed += 1

    self.patint = self.patnum[1:8]
    self.class_one = self.class_str[:3]
    self.class_two = self.class_str[3:6]

    print '{:.8} {} {} {:.3} {:.3}'.format(self.patint,self.file_date,self.grant_date,self.class_one,self.class_two)

    patents.append((self.patint,self.file_date,self.grant_date,self.class_one,self.class_two))
    if len(patents) == batch_size:
      commitBatch()

# parser, emulate SAX here
class ParserGen1:
  def __init__(self):
    pass

  def setContentHandler(self,handler):
    self.handler = handler

  def parse(self,fname):
    fid = open(fname)
    for line in fid:
      line = line[:-1]

      if len(line) == 0 or line[0] == ' ':
        continue

      tag = line[:4].strip()
      text = line[5:]

      self.handler.tag(tag.rstrip(),text)

# do parsing
parser = ParserGen1()
grant_handler = GrantHandler()
parser.setContentHandler(grant_handler)
parser.parse(in_fname)

# clear out the rest
if len(patents) > 0:
  commitBatch()

if store_db:
  # commit to db and close
  conn.commit()
  cur.close()
  conn.close()

print grant_handler.completed


