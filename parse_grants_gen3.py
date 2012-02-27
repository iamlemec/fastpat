#!/usr/bin/python

from xml.sax import handler, make_parser, SAXException
import sys
import sqlite3

# handle arguments
if len(sys.argv) <= 1:
  print 'Usage: parse_grants_gen3.py filename store_db'
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

# SAX hanlder for gen3 patent grants
class GrantHandler(handler.ContentHandler):
  def __init__(self):
    pass

  def startDocument(self):
    self.in_pubref = False
    self.in_appref = False
    self.in_patnum = False
    self.in_grantdate = False
    self.in_filedate = False
    self.in_fos = False
    self.in_classnat = False
    self.in_mainclass = False

    self.completed = 0
    self.multi_assign = 0

  def endDocument(self):
    pass

  def startElement(self, name, attrs):
    if name == 'us-patent-grant':
      self.patnum = ''
      self.grant_date = ''
      self.file_date = ''
      self.class_str = ''
    elif name == 'publication-reference':
      self.in_pubref = True
    elif name == 'application-reference':
      self.in_appref = True
    elif name == 'field-of-search':
      self.in_fos = True
    elif name == 'doc-number':
      if self.in_pubref:
        self.in_patnum = True
    elif name == 'classification-national':
      if not self.in_fos:
        self.in_classnat = True
    elif name == 'main-classification':
      if self.in_classnat:
        self.in_mainclass = True
    elif name == 'date':
      if self.in_pubref:
        self.in_grantdate = True
      elif self.in_appref:
        self.in_filedate = True

  def endElement(self, name):
    if name == 'us-patent-grant':
      if self.patnum[0] == '0':
        self.addPatent()
    elif name == 'publication-reference':
      self.in_pubref = False
    elif name == 'application-reference':
      self.in_appref = False
    elif name == 'field-of-search':
      self.in_fos = False
    elif name == 'doc-number':
      self.in_patnum = False
    elif name == 'classification-national':
      if not self.in_fos:
        self.in_classnat = False
    elif name == 'main-classification':
      if self.in_classnat:
        self.in_mainclass = False
    elif name == 'date':
      if self.in_grantdate:
        self.in_grantdate = False
      elif self.in_filedate:
        self.in_filedate = False

  def characters(self, content):
    if self.in_patnum:
      self.patnum += content
    if self.in_grantdate:
      self.grant_date += content
    if self.in_filedate:
      self.file_date += content
    if self.in_mainclass:
      self.class_str += content

  def addPatent(self):
    self.completed += 1

    self.patint = self.patnum[1:]
    self.class_one = self.class_str[:3]
    self.class_two = self.class_str[3:6]

    print '{:7} {} {} {:.3} {:.3}'.format(self.patint,self.file_date,self.grant_date,self.class_one,self.class_two)

    patents.append((self.patint,self.file_date,self.grant_date,self.class_one,self.class_two))
    if len(patents) == batch_size:
      commitBatch()

# do parsing
parser = make_parser()
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

print grant_handler.multi_assign
print grant_handler.completed


