#!/usr/bin/python

import sys
import sqlite3
from xml.sax import make_parser, xmlreader
from parse_grants_common import PathHandler

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
    cur.execute("create table patent (patnum int, filedate text, grantdate text, classone int, classtwo int, ipcver text, ipccode text, country text, owner text)")
  except sqlite3.OperationalError as e:
    print e

# store for batch commit
batch_size = 1000
patents = []

def commitBatch():
  if store_db:
    cur.executemany('insert into patent values (?,?,?,?,?,?,?,?,?)',patents)
  del patents[:]

# XML codes gen2
# B110 - patent number section (PDAT)
# B140 - grant date section (PDAT)
# B220 - issue date section (PDAT)
# B511 - international patent class (PDAT)
# B521 - classification section (PDAT)
# B731 - original assignee name section (name: NAM->PDAT, country: CTRY->PDAT, city: CITY->PDAT)

# SAX hanlder for gen3 patent grants
class GrantHandler(PathHandler):
  def __init__(self):
    track_keys = ['PATDOC','B110','B140','B220','B511','B521','B731','PDAT','NAM','CTRY','CITY']
    start_keys = ['PATDOC']
    end_keys = ['PATDOC']
    PathHandler.__init__(self,track_keys=track_keys,start_keys=start_keys,end_keys=end_keys)

    self.completed = 0

  def startElement(self,name,attrs):
    PathHandler.startElement(self,name,attrs)

    if name == 'PATDOC':
      self.patnum = ''
      self.grant_date = ''
      self.file_date = ''
      self.ipc_code = ''
      self.class_str = ''
      self.country = ''
      self.city = ''
      self.orgname = ''

  def endElement(self,name):
    PathHandler.endElement(self,name)

    if name == 'PATDOC':
      if self.patnum[0] == '0':
        self.addPatent()

  def characters(self,content):
    if len(self.path) < 2:
      return

    if self.path[-2] == 'B110' and self.path[-1] == 'PDAT':
        self.patnum += content
    elif self.path[-2] == 'B140' and self.path[-1] == 'PDAT':
        self.grant_date += content
    elif self.path[-2] == 'B220' and self.path[-1] == 'PDAT':
      self.file_date += content
    elif self.path[-2] == 'B511' and self.path[-1] == 'PDAT':
        self.ipc_code += content      
    elif self.path[-2] == 'B521' and self.path[-1] == 'PDAT':
      self.class_str += content

    if len(self.path) < 3:
      return

    if self.path[-3] == 'B731':
      if self.path[-2] == 'NAM' and self.path[-1] == 'PDAT':
        self.orgname += content
      elif self.path[-2] == 'CTRY' and self.path[-1] == 'PDAT':
        self.country += content
      elif self.path[-2] == 'CITY' and self.path[-1] == 'PDAT':
        self.city += content

  def addPatent(self):
    self.completed += 1

    self.patint = self.patnum[1:]
    self.ipc_ver = 'GEN2'
    self.ipc_code = self.ipc_code[:4] + self.ipc_code[5:7].strip() + '/' + self.ipc_code[7:].strip()
    self.class_one = self.class_str[:3].strip()
    self.class_two = self.class_str[3:6].strip()
    self.country = self.country if self.country else 'US'
    self.orgname_esc = self.orgname.replace('&amp;','&').encode('ascii','ignore').upper()

    if not store_db: print '{:.8} {} {} {:3} {:3} {:4} {:10} {:10} {:3} {:.30}'.format(self.patint,self.file_date,self.grant_date,self.class_one,self.class_two,self.ipc_ver,self.ipc_code,self.city,self.country,self.orgname_esc)

    patents.append((self.patnum,self.file_date,self.grant_date,self.class_one,self.class_two,self.ipc_ver,self.ipc_code,self.country,self.orgname_esc))
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


