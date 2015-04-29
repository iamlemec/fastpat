#!/usr/bin/python

from xml.sax import xmlreader, handler, make_parser, SAXException
import sys
import sqlite3

# handle arguments
if len(sys.argv) <= 1:
  print 'Usage: parse_grants_gen2.py filename store_db'
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
    cur.execute("create table patent (patnum int, filedate text, grantdate text, classone int, classtwo int, ipcver text, ipccode text, owner text)")
  except sqlite3.OperationalError as e:
    print e

# store for batch commit
batch_size = 1000
patents = []

def commitBatch():
  if store_db:
    cur.executemany('insert into patent values (?,?,?,?,?,?,?,?)',patents)
  del patents[:]

# XML codes gen2
# B110 - patent number section (PDAT)
# B140 - grant date section (PDAT)
# B220 - issue date section (PDAT)
# B511 - international patent class (PDAT)
# B521 - classification section (PDAT)
# B731 - original assignee name section (name: NAM->PDAT, country: CTRY->PDAT)

# SAX hanlder for gen2 patent grants
class GrantHandler(handler.ContentHandler):
  def __init__(self):
    pass

  def startDocument(self):
    self.in_patnum_sec = False
    self.in_patnum = False
    self.in_grantdate = False
    self.in_grantdate_sec = False
    self.in_filedate = False
    self.in_filedate_sec = False
    self.in_ipc = False
    self.in_ipc_sec = False
    self.in_class = False
    self.in_class_sec = False
    self.in_orgname = False
    self.in_orgname_sec = False
    self.in_orgname_sec2 = False

    self.completed = 0

  def endDocument(self):
    pass

  def startElement(self, name, attrs):
    if name == 'PATDOC':
      self.patnum = ''
      self.grant_date = ''
      self.file_date = ''
      self.ipc_str = ''
      self.class_str = ''
      self.orgname = ''
    elif name == 'B110':
      self.in_patnum_sec = True
    elif name == 'B140':
      self.in_grantdate_sec = True
    elif name == 'B220':
      self.in_filedate_sec = True
    elif name == 'B511':
      self.in_ipc_sec = True
    elif name == 'B521':
      self.in_class_sec = True
    elif name == 'B731':
      self.in_orgname_sec = True
    elif name == 'NAM':
      if self.in_orgname_sec:
        self.in_orgname_sec2 = True
    elif name == 'PDAT':
      if self.in_patnum_sec:
        self.in_patnum = True
      elif self.in_grantdate_sec:
        self.in_grantdate = True
      elif self.in_filedate_sec:
        self.in_filedate = True
      elif self.in_ipc_sec:
        self.in_ipc = True
      elif self.in_class_sec:
        self.in_class = True
      elif self.in_orgname_sec2:
        if self.orgname == '':
          self.in_orgname = True

  def endElement(self, name):
    if name == 'PATDOC':
      if self.patnum[0] == '0':
        self.addPatent()
    elif name == 'B110':
      self.in_patnum_sec = False
    elif name == 'B140':
      self.in_grantdate_sec = False
    elif name == 'B220':
      self.in_filedate_sec = False
    elif name == 'B511':
      self.in_ipc_sec = False
    elif name == 'B521':
      self.in_class_sec = False
    elif name == 'B731':
      self.in_orgname_sec = False
    elif name == 'NAM':
      self.in_orgname_sec2 = False
    elif name == 'PDAT':
      if self.in_patnum:
        self.in_patnum = False
      elif self.in_grantdate:
        self.in_grantdate = False
      elif self.in_filedate:
        self.in_filedate = False
      elif self.in_ipc:
        self.in_ipc = False
      elif self.in_class:
        self.in_class = False
      elif self.in_orgname:
        self.in_orgname = False

  def characters(self, content):
    if self.in_patnum:
      self.patnum += content
    elif self.in_grantdate:
      self.grant_date += content
    elif self.in_filedate:
      self.file_date += content
    elif self.in_ipc:
      self.ipc_str = content
    elif self.in_class:
      self.class_str += content
    elif self.in_orgname:
      self.orgname += content

  def addPatent(self):
    self.completed += 1

    self.patint = self.patnum[1:]
    self.ipc_ver = 'GEN2'
    self.ipc_code = self.ipc_str[:4] + self.ipc_str[5:7].strip() + '/' + self.ipc_str[7:].strip()
    self.class_one = self.class_str[:3]
    self.class_two = self.class_str[3:6]
    self.orgname_esc = self.orgname.replace('&amp;','&').encode('ascii','ignore').upper()

    if not store_db: print '{:.8} {} {} {:.3} {:.3} {:12.12} {:.30}'.format(self.patint,self.file_date,self.grant_date,self.class_one,self.class_two,self.ipc_ver,self.ipc_code,self.orgname_esc)

    patents.append((self.patnum,self.file_date,self.grant_date,self.class_one,self.class_two,self.ipc_ver,self.ipc_code,self.orgname_esc))
    if len(patents) == batch_size:
      commitBatch()

# do parsing
grant_handler = GrantHandler()

input_source = xmlreader.InputSource()
input_source.setEncoding('iso-8859-1')
input_source.setByteStream(open(in_fname))

parser = make_parser()
parser.setContentHandler(grant_handler)
parser.parse(input_source)

# clear out the rest
if len(patents) > 0:
  commitBatch()

if store_db:
  # commit to db and close
  conn.commit()
  cur.close()
  conn.close()

print grant_handler.completed


